//-----------------------------------------------------------------------
// <copyright file="PipeAssociationHandle.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Threading;
using Akka.Actor;
using Akka.Remote.Transport.Pipelines.Codec;
using Google.Protobuf;

namespace Akka.Remote.Transport.Pipelines
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// <see cref="AssociationHandle"/> implementation for <see cref="TcpPipeTransport"/>.
    ///
    /// <para>
    /// <see cref="Write"/> enqueues the payload onto the per-connection bounded
    /// <see cref="System.Threading.Channels.Channel{T}"/> write queue and returns
    /// <c>false</c> when the channel is at capacity (matching DotNetty water-mark semantics).
    /// </para>
    /// <para>
    /// When <see cref="IsZeroCopyEnabled"/> is <c>true</c>, the higher-level
    /// <c>AkkaProtocolHandle</c> bypasses <c>ConstructPayload</c> and
    /// calls <see cref="WriteRaw"/> instead, writing the outer <c>AkkaProtocolMessage</c>
    /// tag + varint + body directly into a pooled buffer — eliminating the W5 allocation. ✨
    /// </para>
    /// <para>
    /// <see cref="Disassociate()"/> signals the owning <see cref="PipeConnection"/> to
    /// cancel its read/write loops and close the socket.
    /// </para>
    ///
    /// <!-- CopilotNotes: The circular reference between PipeAssociationHandle and PipeConnection is
    ///      intentional — they are tightly coupled by design. Connection is set via the internal
    ///      setter immediately after construction, before Start() is called, so there is no null risk
    ///      in practice. The Interlocked guard on _disassociated prevents double-close. -->
    /// </summary>
    internal sealed class PipeAssociationHandle : AssociationHandle
    {
        // CAS guard: 0 = open, 1 = disassociated
        private int _disassociated;

        /// <summary>
        /// Back-reference to the owning connection. Set by <see cref="PipeConnection"/> constructor.
        /// </summary>
        internal PipeConnection? Connection { get; set; }

        /// <summary>
        /// When <c>true</c>, <see cref="WriteRaw"/> is available and the zero-copy codec
        /// path is active. <see cref="AkkaProtocolHandle"/> checks this flag before
        /// calling <c>ConstructPayload</c>.
        ///
        /// <!-- CopilotNotes: Set at construction time from PipeTransportSettings.ZeroCopyCodec.
        ///      Immutable after construction; safe to read without locking. -->
        /// </summary>
        public bool IsZeroCopyEnabled { get; }

        /// <summary>
        /// Initializes a new <see cref="PipeAssociationHandle"/> with the legacy codec path.
        /// </summary>
        public PipeAssociationHandle(Address localAddress, Address remoteAddress)
            : base(localAddress, remoteAddress)
        {
            IsZeroCopyEnabled = false;
        }

        /// <summary>
        /// Initializes a new <see cref="PipeAssociationHandle"/>.
        /// </summary>
        /// <param name="localAddress">The local Akka address.</param>
        /// <param name="remoteAddress">The remote Akka address.</param>
        /// <param name="zeroCopyCodec">
        ///   <c>true</c> to activate the zero-copy write path (from
        ///   <c>akka.remote.pipe.tcp.zero-copy-codec = on</c>). 🌸
        /// </param>
        public PipeAssociationHandle(Address localAddress, Address remoteAddress, bool zeroCopyCodec)
            : base(localAddress, remoteAddress)
        {
            IsZeroCopyEnabled = zeroCopyCodec;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Thread-safe. Returns <c>false</c> if the channel is full
        /// or the association has already been disassociated.
        /// A return value of <c>false</c> means the write was dropped
        /// (guaranteed-no-duplication semantics per the <see cref="AssociationHandle"/> contract).
        /// </remarks>
        public override bool Write(ByteString payload)
        {
            if (Connection is null || Volatile.Read(ref _disassociated) == 1)
                return false;

            return Connection.TryEnqueueWrite(payload);
        }

        /// <summary>
        /// Zero-copy write path: wraps <paramref name="innerPayload"/>
        /// (the <c>AckAndEnvelopeContainer</c> bytes, i.e. W4 bytes) in the outer
        /// <c>AkkaProtocolMessage</c> envelope directly into a pooled frame, then
        /// enqueues it for writing. ✨
        ///
        /// <para>
        /// This bypasses <c>AkkaPduProtobuffCodec.ConstructPayload</c>'s
        /// <c>ToByteString()</c> allocation (W5), replacing it with a single
        /// tag + varint write into a <see cref="PooledFrame"/> rented from
        /// <see cref="MemoryPool{T}.Shared"/>.
        /// </para>
        ///
        /// <para>
        /// Only called when <see cref="IsZeroCopyEnabled"/> is <c>true</c>. 🌸
        /// </para>
        /// </summary>
        public bool WriteRaw(ReadOnlyMemory<byte> innerPayload)
        {
            if (Connection is null || Volatile.Read(ref _disassociated) == 1)
                return false;

            // Compute the exact byte count for the outer AkkaProtocolMessage wrapper:
            //   tag(0x0A) + varint(innerPayload.Length) + innerPayload.Length
            var frameSize = OuterEnvelopeWriter.ComputePayloadFrameSize(innerPayload.Length);
            var frame     = PooledFrame.Rent(frameSize);
            OuterEnvelopeWriter.WritePayloadFrame(frame, innerPayload.Span);
            return Connection.TryEnqueueWrite(frame);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Safe to call multiple times; only the first call takes effect.
        /// </remarks>
#pragma warning disable CS0672 // override of obsolete member — suppressed globally via Directory.Build.props NoWarn
        public override void Disassociate()
#pragma warning restore CS0672
        {
            if (Interlocked.CompareExchange(ref _disassociated, 1, 0) == 0)
                Connection?.BeginDisassociate();
        }
    }
}

