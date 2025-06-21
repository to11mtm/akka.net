using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Akka.Pattern;

namespace Akka.IO
{
    internal abstract class TlsConnection : ReceiveActor, IRequiresMessageQueue<IUnboundedMessageQueueSemantics>
    {
        private readonly record struct ConnState(
            bool IsReceiving,
            bool IsSending,
            bool PeerClosed,
            bool OutputShutdown,
            bool ReadingSuspended,
            bool WritingSuspended,
            bool KeepOpenOnPeerClosed,
            Queue<(Tcp.Write Cmd, IActorRef Snd)> Queue,
            int QueuedBytes)
        {
            public bool HasPending => IsSending || Queue.Count != 0;
            public bool CanSend => !OutputShutdown && !WritingSuspended;
            public bool CanReceive => !PeerClosed && !ReadingSuspended;

            public static ConnState Initial(Queue<(Tcp.Write Cmd, IActorRef Snd)> q) =>
                new(false, false, false, false, true, true, false, q, 0);
        }

        private sealed class AckSocketAsyncEventArgs : SocketAsyncEventArgs, INoSerializationVerificationNeeded, IDeadLetterSuppression
        {
            public readonly List<(IActorRef Commander, object Ack)> PendingAcks = new(8);
            public void ClearAcks() => PendingAcks.Clear();
        }

        private sealed class ReadSocketAsyncEventArgs : SocketAsyncEventArgs, INoSerializationVerificationNeeded, IDeadLetterSuppression;

        protected readonly TcpExt Tcp;
        protected readonly Socket Socket;
        protected readonly SslStream SslStream;
        protected ILoggingAdapter Log { get; } = Context.GetLogger();

        private readonly Queue<(Tcp.Write Cmd, IActorRef Sender)> _pendingWrites;
        private readonly AckSocketAsyncEventArgsFake _sendArgs;
        private readonly ReadSocketAsyncEventArgsFake _receiveArgs;
        private readonly byte[] _receiveBuffer;
        private ConnState _state;
        private readonly bool _traceLogging;
        private readonly bool _pullMode;
        private IActorRef? _commander;
        private IActorRef? _handler;
        private CloseInformation? _closeInformation;
        private int? _partialWriteOffset;

        protected TlsConnection(TcpExt tcp, Socket socket, SslStream sslStream, bool pullMode)
        {
            Tcp = tcp;
            Socket = socket ?? throw new ArgumentNullException(nameof(socket));
            SslStream = sslStream ?? throw new ArgumentNullException(nameof(sslStream));
            _pullMode = pullMode;
            _pendingWrites = new Queue<(Tcp.Write Cmd, IActorRef Sender)>(16);
            _traceLogging = tcp.Settings.TraceLogging;
            _receiveBuffer = new byte[tcp.Settings.MaxFrameSizeBytes];
            _receiveArgs = new ReadSocketAsyncEventArgsFake();
            _sendArgs = new AckSocketAsyncEventArgsFake();
            InitSocketEventArgs();

            _state = ConnState.Initial(_pendingWrites);
            if (_pullMode)
                _state = _state with { ReadingSuspended = true };
        }

        private void InitSocketEventArgs()
        {
            _receiveArgs.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
            _receiveArgs.UserToken = Self;
            _receiveArgs.Completed += OnCompleted;

            _sendArgs.UserToken = Self;
            _sendArgs.Completed += OnCompleted;
        }

        private static void OnCompleted(object? sender, SendReceiveArgsFake e)
        {
            if (e.UserToken is not IActorRef self) return;
            self.Tell(e);
        }

        protected override void PostStop()
        {
            if (Socket.Connected) AbortSocket();
            else CloseSocket();

            _receiveArgs.Dispose();
            _sendArgs.Dispose();

            while (_pendingWrites.Count > 0)
            {
                var (cmd, snd) = _pendingWrites.Dequeue();
                snd.Tell(cmd.FailureMessage);
            }

            if (_closeInformation != null)
            {
                foreach (var sub in _closeInformation.NotificationsTo)
                    sub.Tell(_closeInformation.ClosedEvent);
            }
        }

        protected override void PostRestart(Exception reason)
        {
            throw new IllegalStateException("Restarting not supported for connection actors.");
        }

        protected void CompleteConnect(IActorRef commander, IEnumerable<Inet.SocketOption> options)
        {
            try { Socket.NoDelay = true; }
            catch (SocketException e) { Log.Debug("Could not enable TcpNoDelay: {0}", e.Message); }

            foreach (var option in options)
                option.AfterConnect(Socket);

            _commander = commander;
            Context.Watch(_commander);
            commander.Tell(new Tcp.Connected(Socket.RemoteEndPoint, Socket.LocalEndPoint));

            Context.SetReceiveTimeout(Tcp.Settings.RegisterTimeout);
            Become(AwaitRegBehaviour);
        }

        private void AwaitRegBehaviour()
        {
            Receive<Tcp.Register>(reg =>
            {
                _handler = reg.Handler;
                if (_traceLogging) Log.Debug("[{0}] registered as connection handler", reg.Handler);
                Context.Watch(_handler);
                Context.Unwatch(_commander);
                _state = _state with { KeepOpenOnPeerClosed = reg.KeepOpenOnPeerClosed, ReadingSuspended = _pullMode, WritingSuspended = false };
                _closeInformation = CloseInformation.Single(_handler, IO.Tcp.Aborted.Instance);
                Context.SetReceiveTimeout(null);
                Become(OpenBehaviour);
                IssueReceive();
                TrySend();
            });
            Receive<Tcp.WriteCommand>(w =>
            {
                var queueSizeBefore = _pendingWrites.Count;
                Enqueue(w);
                if (_pendingWrites.Count > queueSizeBefore)
                {
                    Log.Warning("Received Write command before Register command. It will be buffered until Register will be received (buffered write size is {0} bytes)", w.Bytes);
                }
            });
            Receive<Tcp.CloseCommand>(c => HandleClose(Sender, c.Event));
            Receive<Tcp.SuspendReading>(_ => { _state = _state with { ReadingSuspended = true }; });
            Receive<Tcp.ResumeReading>(_ => { _state = _state with { ReadingSuspended = false }; });
            Receive<ReceiveTimeout>(_ =>
            {
                Log.Debug("Configured registration timeout of [{0}] expired, stopping", Tcp.Settings.RegisterTimeout);
                Context.Stop(Self);
            });
        }

        private void OpenBehaviour()
        {
            Receive<ReadSocketAsyncEventArgs>(s => HandleReceiveCompleted(s, null));
            Receive<AckSocketAsyncEventArgs>(HandleSendCompleted);
            Receive<Tcp.WriteCommand>(Enqueue);
            Receive<Tcp.CloseCommand>(c => HandleClose(Sender, c.Event));
            SuspendResumeHandlers();
        }

        private void SuspendResumeHandlers()
        {
            Receive<Tcp.ResumeReading>(_ =>
            {
                _state = _state with { ReadingSuspended = false };
                IssueReceive();
            });
            Receive<Tcp.SuspendReading>(_ => { _state = _state with { ReadingSuspended = true }; });
            Receive<Tcp.ResumeWriting>(_ =>
            {
                _state = _state with { WritingSuspended = false };
                TrySend();
            });
        }

        private void HandleReceiveCompleted(SocketAsyncEventArgs ea, IActorRef? closeCommander)
        {
            _state = _state with { IsReceiving = false };
            if (ea is { SocketError: SocketError.Success, BytesTransferred: > 0 })
            {
                _handler!.Tell(new Tcp.Received(ByteString.CopyFrom(_receiveBuffer, 0, ea.BytesTransferred)));
                if (_pullMode)
                    _state = _state with { ReadingSuspended = true };
                else
                    IssueReceive();
                return;
            }

            if (ea.SocketError != SocketError.Success)
            {
                Log.Debug("[TlsConnection] read failed with error [{0}]", ea.SocketError);
                HandleError(new SocketException((int)ea.SocketError));
                return;
            }

            if (ea.BytesTransferred == 0)
            {
                if (_state.OutputShutdown)
                {
                    Log.Debug("[TlsConnection] EOF received; our side is already closed. Closing connection.");
                    DoCloseConnection(closeCommander ?? _handler!, IO.Tcp.ConfirmedClosed.Instance);
                }
                else
                {
                    Log.Debug("[TlsConnection] EOF received");
                    _state = _state with { PeerClosed = true };
                    HandleClose(closeCommander ?? _handler!, IO.Tcp.PeerClosed.Instance);
                }
            }
        }

        private void HandleSendCompleted(AckSocketAsyncEventArgs ea)
        {
            _state = _state with { IsSending = false };

            if (ea.SocketError != SocketError.Success)
            {
                Log.Debug("[TlsConnection] write failed with error [{0}]", ea.SocketError);
                HandleError(new SocketException((int)ea.SocketError));
                return;
            }

            foreach (var (c, ack) in ea.PendingAcks)
                c.Tell(ack);

            ea.ClearAcks();
            ea.BufferList = null;
            TrySend();
        }

        private void IssueReceive()
        {
            if (_state.IsReceiving || _state.ReadingSuspended) return;
            _state = _state with { IsReceiving = true };
            SslStream.ReadAsync(_receiveBuffer, 0, _receiveBuffer.Length)
                .PipeTo(Self, Self, i =>
                {
                    _receiveArgs.SocketError = SocketError.Success;
                    _receiveArgs.BytesTransferred = i;
                    return _receiveArgs;
                }, e =>
                {
                    _receiveArgs.SocketError = SocketError.SocketError;
                    _receiveArgs.BytesTransferred = 0;
                    return _receiveArgs;
                });
        }

        private void Enqueue(Tcp.WriteCommand cmd)
        {
            // For brevity, only handle Tcp.Write here. CompoundWrite can be added as needed.
            if (cmd is Tcp.Write w && w.Data.Count > 0)
            {
                _pendingWrites.Enqueue((w, Sender));
                _state = _state with { QueuedBytes = _state.QueuedBytes + w.Data.Count };
                TrySend();
            }
        }

        private void TrySend()
        {
            if (_traceLogging)
                Log.Debug(
                    $"[TlsConnection] TrySend called. IsSending={_state.IsSending}, PendingWrites={_pendingWrites.Count}, CanSend={_state.CanSend}, PartialWriteOffset={_partialWriteOffset}");

            if (!_state.CanSend) return;
            if (_state.IsSending || _pendingWrites.Count == 0) return;

            var (w, snd) = _pendingWrites.Peek();
            var data = w.Data;
            var offset = _partialWriteOffset ?? 0;
            var remaining = data.Count - offset;

            if (remaining == 0)
            {
                _pendingWrites.Dequeue();
                _state = _state with { QueuedBytes = _state.QueuedBytes - w.Data.Count };
                _partialWriteOffset = null;
                if (w.WantsAck) snd.Tell(w.Ack);
                TrySend();
                return;
            }

            _sendArgs.ClearAcks();
            if (w.WantsAck) _sendArgs.PendingAcks.Add((snd, w.Ack));
            _state = _state with { IsSending = true };

            var slice = offset > 0 ? data.Slice(offset, remaining) : data;
            byte[] buffer = slice.ToArray();

            SslStream.WriteAsync(buffer, 0, buffer.Length)
                .PipeTo(Self, false, Self,
                    () =>
                    {
                        if (_traceLogging)
                            Log.Debug($"[TlsConnection] TrySend: wrote {buffer.Length} bytes");

                        if (buffer.Length < remaining)
                        {
                            // Partial write, update offset
                            _partialWriteOffset = offset + buffer.Length;
                        }
                        else
                        {
                            // Full write, reset offset and dequeue
                            _partialWriteOffset = null;
                            _pendingWrites.Dequeue();
                            _state = _state with { QueuedBytes = _state.QueuedBytes - w.Data.Count };
                        }

                        _sendArgs.SocketError = SocketError.Success;
                        _sendArgs.BytesTransferred = buffer.Length;
                        return _sendArgs;
                    },
                    e =>
                    {
                        if (_traceLogging)
                            Log.Debug(e, "[TlsConnection] TrySend: failed with exception: {0}", e.Message);

                        _sendArgs.SocketError = SocketError.SocketError;
                        _sendArgs.BytesTransferred = 0;
                        return _sendArgs;
                    });
        }

        private class HandleErrorMsg : IDeadLetterSuppression
        {
            public Exception Exception { get; }
    
            public HandleErrorMsg(Exception exception)
            {
                Exception = exception;
            }
        }

        private void HandleClose(IActorRef closeSender, Tcp.ConnectionClosed closeEvent)
        {
            // Simplified for brevity; expand as needed for ConfirmedClosed, PeerClosed, etc.
            DoCloseConnection(closeSender, closeEvent);
        }

        private void HandleError(SocketException e)
        {
            Log.Debug(e, "Closing TLS connection due to I/O error: {0}", e.SocketErrorCode);
            var errorClosed = new Tcp.ErrorClosed(e.Message);
            if (_closeInformation != null)
                _closeInformation = _closeInformation with { ClosedEvent = errorClosed };
            else
                _closeInformation = CloseInformation.Single(_handler ?? Self, errorClosed);
            Context.Stop(Self);
        }

        private void DoCloseConnection(IActorRef closeSender, Tcp.ConnectionClosed closedEvent)
        {
            StopWith(new CloseInformation(ImmutableHashSet<IActorRef>.Empty.Add(closeSender), closedEvent));
        }

        private void CloseSocket()
        {
            try { SslStream.Dispose(); } catch { }
            try { Socket.Dispose(); } catch { }
            _state = _state with { OutputShutdown = true, ReadingSuspended = true };
        }

        private void AbortSocket()
        {
            try { Socket.LingerState = new LingerOption(true, 0); } catch { }
            CloseSocket();
        }

        protected sealed record CloseInformation(ImmutableHashSet<IActorRef> NotificationsTo, Tcp.Event ClosedEvent)
        {
            public static CloseInformation Single(IActorRef closeSender, Tcp.Event closedEvent)
            {
                return new CloseInformation(ImmutableHashSet<IActorRef>.Empty.Add(closeSender), closedEvent);
            }
        }
        /* ================================================================= */
        /*  Close‑notification tracking                                  */
        /* ---------------------------------------------------------------- */
        protected void StopWith(CloseInformation closeInformation)
        {
            if(_handler != null)
            {
                closeInformation = closeInformation with { NotificationsTo = closeInformation.NotificationsTo.Add(_handler!) };
            }
            
            _closeInformation = closeInformation;
            Context.Stop(Self);
        }
    }
}