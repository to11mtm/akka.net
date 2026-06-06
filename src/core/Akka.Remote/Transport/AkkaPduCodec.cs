//-----------------------------------------------------------------------
// <copyright file="AkkaPduCodec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Linq;
using Akka.Actor;
using Google.Protobuf;
using System.Runtime.Serialization;
using System.Text;
using Akka.Remote.Serialization;
using Akka.Remote.Serialization.Proto.Msg;
using SerializedMessage = Akka.Remote.Serialization.Proto.Msg.Payload;

namespace Akka.Remote.Transport
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    internal class PduCodecException : AkkaException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PduCodecException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="cause">The exception that is the cause of the current exception.</param>
        public PduCodecException(string message, Exception cause = null) : base(message, cause) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="PduCodecException"/> class.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected PduCodecException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /*
     * Interface used to represent Akka PDUs (Protocol Data Unit)
     */
    /// <summary>
    /// TBD
    /// </summary>
    internal interface IAkkaPdu { }

    /// <summary>
    /// TBD
    /// </summary>
    internal sealed class Associate : IAkkaPdu
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="info">TBD</param>
        public Associate(HandshakeInfo info)
        {
            Info = info;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public HandshakeInfo Info { get; private set; }
    }

    /// <summary>
    /// TBD
    /// </summary>
    internal sealed class Disassociate : IAkkaPdu
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="reason">TBD</param>
        public Disassociate(DisassociateInfo reason)
        {
            Reason = reason;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public DisassociateInfo Reason { get; private set; }
    }

    /// <summary>
    /// INTERNAL API.
    /// 
    /// Represents a heartbeat on the wire.
    /// </summary>
    internal sealed class Heartbeat : IAkkaPdu { }

    /// <summary>
    /// TBD
    /// </summary>
    internal sealed class Payload : IAkkaPdu
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="bytes">TBD</param>
        public Payload(ByteString bytes)
        {
            Bytes = bytes.Memory;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public ReadOnlyMemory<byte> Bytes { get; private set; }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// Represents a decoded inbound remote message carrying 
    ///  A protobuf <see cref="SerializedMessage"/> (<c>Payload</c>) — used by the DotNetty /
    ///  Pipe+Protobuf codec paths.
    ///
    /// </summary>
    internal sealed class Message : IAkkaPdu, IHasSequenceNumber
    {
        /// <summary>
        /// Creates a <see cref="Message"/> backed by a protobuf <c>SerializedMessage</c>.
        /// Used by the DotNetty and Pipe+Protobuf codecs.
        /// </summary>
        public Message(IInternalActorRef recipient, Address recipientAddress, SerializedMessage serializedMessage,
            IActorRef senderOptional = null, SeqNo? seq = null)
        {
            Seq = seq;
            SenderOptional = senderOptional;
            SerializedMessage = serializedMessage;
            RecipientAddress = recipientAddress;
            Recipient = recipient;
        }

        /// <summary>The resolved local recipient actor ref.</summary>
        public IInternalActorRef Recipient { get; private set; }

        /// <summary>The address component of the recipient actor path.</summary>
        public Address RecipientAddress { get; private set; }

        /// <summary>
        /// Protobuf payload
        /// </summary>
        public SerializedMessage SerializedMessage { get; private set; }
        
        /// <summary>Optional sender ref; falls back to Dead Letters when null.</summary>
        public IActorRef SenderOptional { get; private set; }

        /// <summary>Whether this message uses reliable delivery sequencing.</summary>
        public bool ReliableDeliveryEnabled => Seq != null;

        /// <summary>
        /// The optional sequence number for reliable delivery. Null when reliable delivery is not used.
        /// </summary>
        public SeqNo? Seq { get; private set; }

        /// <inheritdoc/>
        SeqNo IHasSequenceNumber.Seq => Seq!.Value;
    }

    /// <summary>
    /// INTERNAL API
    /// </summary>
    internal sealed class AckAndMessage
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="ackOption">TBD</param>
        /// <param name="messageOption">TBD</param>
        public AckAndMessage(Ack ackOption, Message messageOption)
        {
            MessageOption = messageOption;
            AckOption = ackOption;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public Ack AckOption { get; private set; }

        /// <summary>
        /// TBD
        /// </summary>
        public Message MessageOption { get; private set; }
    }

    /// <summary>
    /// INTERNAL API
    /// 
    /// A codec that is able to convert Akka PDUs from and to <see cref="ByteString"/>
    /// </summary>
    internal abstract class AkkaPduCodec
    {
        protected readonly ActorSystem System;
        protected readonly ActorPathThreadLocalCache ActorPathCache;

        protected AkkaPduCodec(ActorSystem system)
        {
            System = system;
            ActorPathCache = ActorPathThreadLocalCache.For(system);
        }

        /// <summary>
        /// Return an <see cref="IAkkaPdu"/> instance that represents a PDU contained in the raw
        /// <see cref="ByteString"/>.
        /// </summary>
        /// <param name="raw">Encoded raw byte representation of an Akka PDU</param>
        /// <returns>Class representation of a PDU.</returns>
        public abstract IAkkaPdu DecodePdu(ByteString raw);

        /// <summary>
        /// Takes an <see cref="IAkkaPdu"/> representation of an Akka PDU and returns its encoded form
        /// as a <see cref="ByteString"/>.
        /// </summary>
        /// <param name="pdu">TBD</param>
        /// <returns>TBD</returns>
        public virtual ByteString EncodePdu(IAkkaPdu pdu)
        {
            switch (pdu)
            {
                case Payload p:
                    return ConstructPayload(p.Bytes);
                case Heartbeat _:
                    return ConstructHeartbeat();
                case Associate a:
                    return ConstructAssociate(a.Info);
                case Disassociate d:
                    return ConstructDisassociate(d.Reason);
                default:
                    return null; // unsupported message type
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="payload">TBD</param>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructPayload(ByteString payload);

        public abstract ByteString ConstructPayload(ReadOnlyMemory<byte> payload);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="info">TBD</param>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructAssociate(HandshakeInfo info);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="reason">TBD</param>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructDisassociate(DisassociateInfo reason);

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructHeartbeat();

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="raw">TBD</param>
        /// <param name="provider">TBD</param>
        /// <param name="localAddress">TBD</param>
        /// <returns>TBD</returns>
        public abstract AckAndMessage DecodeMessage(ByteString raw, IRemoteActorRefProvider provider, Address localAddress);

        /// <summary>
        /// Zero-copy overload: decodes an <c>AckAndEnvelopeContainer</c> PDU from pool-rented memory
        /// without constructing an intermediate <see cref="ByteString"/>. 🌸
        ///
        /// <para>
        /// The default implementation wraps <paramref name="raw"/> in a
        /// <see cref="UnsafeByteOperations.UnsafeWrap"/> alias and delegates to
        /// <see cref="DecodeMessage(ByteString,IRemoteActorRefProvider,Address)"/>.
        /// Override for a true zero-copy path (e.g. <c>ParseFrom(ReadOnlySequence&lt;byte&gt;)</c>
        /// via Google.Protobuf 3.21+).
        /// </para>
        /// </summary>
        /// <param name="raw">The raw inner-envelope bytes (backed by pool memory).</param>
        /// <param name="provider">The remote actor-ref provider.</param>
        /// <param name="localAddress">The local Akka address.</param>
        /// <returns>The decoded ack + message pair.</returns>
        public virtual AckAndMessage DecodeMessage(ReadOnlyMemory<byte> raw, IRemoteActorRefProvider provider, Address localAddress)
            => DecodeMessage(UnsafeByteOperations.UnsafeWrap(raw), provider, localAddress);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="localAddress">TBD</param>
        /// <param name="recipient">TBD</param>
        /// <param name="serializedMessage">TBD</param>
        /// <param name="senderOption">TBD</param>
        /// <param name="seqOption">TBD</param>
        /// <param name="ackOption">TBD</param>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructMessage(Address localAddress, IActorRef recipient,
            SerializedMessage serializedMessage, IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null);

        public virtual ByteString ConstructMessage(Address localAddress, IActorRef recipient,
            SysandTransportInfo systemAndTransportInfo, object message,
            IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null) =>
            ConstructMessage(localAddress, recipient,
                MessageSerializer.Serialize(systemAndTransportInfo.System, systemAndTransportInfo.TransportInformation,
                    message),
                senderOption, seqOption, ackOption);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="ack">TBD</param>
        /// <returns>TBD</returns>
        public abstract ByteString ConstructPureAck(Ack ack);
    }

    /// <summary>
    /// TBD
    /// </summary>
    internal sealed class AkkaPduProtobuffCodec : AkkaPduCodec
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="raw">TBD</param>
        /// <exception cref="PduCodecException">
        /// This exception is thrown when the Akka PDU in the specified byte string,
        /// <paramref name="raw" />, meets one of the following conditions:
        /// <ul>
        /// <li>The PDU is neither a message or a control message.</li>
        /// <li>The PDU is a control message with an invalid format. </li>
        /// </ul>
        /// </exception>
        /// <returns>TBD</returns>
        public override IAkkaPdu DecodePdu(ByteString raw)
        {
            try
            {
                var pdu = AkkaProtocolMessage.Parser.ParseFrom(raw);
                if (pdu.Instruction != null) return DecodeControlPdu(pdu.Instruction);
                else if (!pdu.Payload.IsEmpty) return new Payload(pdu.Payload); // TODO HasPayload
                else throw new PduCodecException("Error decoding Akka PDU: Neither message nor control message were contained");
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new PduCodecException("Decoding PDU failed", ex);
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="payload">TBD</param>
        /// <returns>TBD</returns>
        public override ByteString ConstructPayload(ByteString payload)
        {
            return new AkkaProtocolMessage() { Payload = payload }.ToByteString();
        }

        public override ByteString ConstructPayload(ReadOnlyMemory<byte> payload)
        {
            return new AkkaProtocolMessage() { Payload = UnsafeByteOperations.UnsafeWrap(payload) }.ToByteString();
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="info">TBD</param>
        /// <exception cref="ArgumentException">
        /// This exception is thrown when the specified <paramref name="info"/> contains an invalid address.
        /// </exception>
        /// <returns>TBD</returns>
        public override ByteString ConstructAssociate(HandshakeInfo info)
        {
            var handshakeInfo = new AkkaHandshakeInfo()
            {
                Origin = SerializeAddress(info.Origin),
                Uid = (ulong)info.Uid
            };

            return ConstructControlMessagePdu(CommandType.Associate, handshakeInfo);
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="reason">TBD</param>
        /// <returns>TBD</returns>
        public override ByteString ConstructDisassociate(DisassociateInfo reason)
        {
            switch (reason)
            {
                case DisassociateInfo.Quarantined:
                    return DISASSOCIATE_QUARANTINED;
                case DisassociateInfo.Shutdown:
                    return DISASSOCIATE_SHUTTING_DOWN;
                case DisassociateInfo.Unknown:
                default:
                    return DISASSOCIATE;
            }
        }

        /*
         * Since there's never any ActorSystem-specific information coded directly
         * into the heartbeat messages themselves (i.e. no handshake info,) there's no harm in caching in the
         * same heartbeat byte buffer and re-using it.
         */
        private static readonly ByteString HeartbeatPdu = ConstructControlMessagePdu(CommandType.Heartbeat);

        /// <summary>
        /// Creates a new Heartbeat message instance.
        /// </summary>
        /// <returns>The Heartbeat message.</returns>
        public override ByteString ConstructHeartbeat()
        {
            return HeartbeatPdu;
        }

        /// <summary>
        /// Indicated RemoteEnvelope.Seq is not defined (order is irrelevant)
        /// </summary>
        private const ulong SeqUndefined = ulong.MaxValue;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="raw">TBD</param>
        /// <param name="provider">TBD</param>
        /// <param name="localAddress">TBD</param>
        /// <returns>TBD</returns>
        public override AckAndMessage DecodeMessage(ByteString raw, IRemoteActorRefProvider provider, Address localAddress)
        {
            // return DecodeMessage(raw.Memory, provider, localAddress);
            return DecodeMessageLessCopies(raw.Memory, provider, localAddress);
            /*
            var ackAndEnvelope = AckAndEnvelopeContainer.Parser.ParseFrom(raw);

            Ack ackOption = null;

            if (ackAndEnvelope.Ack != null)
            {
                ackOption = new Ack(new SeqNo((long)ackAndEnvelope.Ack.CumulativeAck), ackAndEnvelope.Ack.Nacks.Select(x => new SeqNo((long)x)));
            }

            Message messageOption = null;

            if (ackAndEnvelope.Envelope != null)
            {
                var envelopeContainer = ackAndEnvelope.Envelope;
                if (envelopeContainer != null)
                {
                    var recipient = provider.ResolveActorRefWithLocalAddress(envelopeContainer.Recipient.Path, localAddress);
                    
                    //todo get parsed address from provider
                    var recipientAddress = ActorPathCache.Cache.GetOrCompute(envelopeContainer.Recipient.Path).Address;
                    
                    var serializedMessage = envelopeContainer.Message;
                    IActorRef senderOption = null;
                    if (envelopeContainer.Sender != null)
                        senderOption = provider.ResolveActorRefWithLocalAddress(envelopeContainer.Sender.Path, localAddress);
                    
                    SeqNo? seqOption = null;
                    if (envelopeContainer.Seq != SeqUndefined)
                    {
                        unchecked
                        {
                            seqOption = new SeqNo((long)envelopeContainer.Seq); //proto takes a ulong
                        }
                    }

                    messageOption = new Message(recipient, recipientAddress, serializedMessage, senderOption, seqOption);
                }
            }


            return new AckAndMessage(ackOption, messageOption);
            */
        }

        /// <summary>
        /// Zero-copy override: decodes from pool-rented memory using
        /// <see cref="MessageParser{T}.ParseFrom(ReadOnlySequence{byte})"/> (Google.Protobuf ≥ 3.21)
        /// so no intermediate <c>byte[]</c> or <see cref="ByteString"/> is allocated. ✨
        ///
        /// <!-- CopilotNotes: new ReadOnlySequence<byte>(raw) wraps without copying — protobuf reads
        ///      directly from the pooled memory. The caller is responsible for keeping the pool buffer
        ///      live until this method returns and all field values have been stored/copied. 🌸 -->
        /// </summary>
        public override AckAndMessage DecodeMessage(ReadOnlyMemory<byte> raw, IRemoteActorRefProvider provider, Address localAddress)
        {
            var ackAndEnvelope = AckAndEnvelopeContainer.Parser.ParseFrom(new ReadOnlySequence<byte>(raw));

            Ack ackOption = null;

            if (ackAndEnvelope.Ack != null)
            {
                ackOption = new Ack(new SeqNo((long)ackAndEnvelope.Ack.CumulativeAck), ackAndEnvelope.Ack.Nacks.Select(x => new SeqNo((long)x)));
            }

            Message messageOption = null;

            if (ackAndEnvelope.Envelope != null)
            {
                var envelopeContainer = ackAndEnvelope.Envelope;
                if (envelopeContainer != null)
                {
                    var recipient = provider.ResolveActorRefWithLocalAddress(envelopeContainer.Recipient.Path, localAddress);
                    var recipientAddress = ActorPathCache.Cache.GetOrCompute(envelopeContainer.Recipient.Path).Address;
                    var serializedMessage = envelopeContainer.Message;
                    IActorRef senderOption = null;
                    if (envelopeContainer.Sender != null)
                        senderOption = provider.ResolveActorRefWithLocalAddress(envelopeContainer.Sender.Path, localAddress);
                    SeqNo? seqOption = null;
                    if (envelopeContainer.Seq != SeqUndefined)
                    {
                        unchecked
                        {
                            seqOption = new SeqNo((long)envelopeContainer.Seq);
                        }
                    }
                    messageOption = new Message(recipient, recipientAddress, serializedMessage, senderOption, seqOption);
                }
            }

            return new AckAndMessage(ackOption, messageOption);
        }
        
        // Legacy field-order-dependent helpers (GetAckSection / GetMsgSection /
        // SliceSegmentMemory) have been removed in favour of the tag-iterating parsers
        // below. `ReadRawInt32WithNewBufferPos` (above) is retained as part of the
        // existing public-ish surface in case external code references it. ✨
        
        // ── Wire-format helpers (proto3, hand-rolled, allocation-free) ─────────────
        //
        // CopilotNotes: All three of `SliceSegmentMemory`, the old `PayloadParser` and the
        // old `EnvelopeContainerParser` made two unsafe assumptions:
        //   1. fields appear on the wire in field-number order (true for the *current*
        //      Google.Protobuf C# emitter, NOT guaranteed by the protobuf spec); and
        //   2. every "expected" field is present.
        // Assumption (2) is wrong even for traffic produced by this very codec, because
        // proto3 elides scalar fields with their default value (e.g. `fixed64 seq = 0`,
        // `int32 serializerId = 0`). When such a message arrives, the legacy parser walks
        // off the end of the buffer (e.g. `second.Span[0]` with empty `second`) and
        // throws `IndexOutOfRangeException`. The replacement below is a proper
        // tag-iterating parser: it loops field-by-field, reads the wire-type, accepts
        // any field order, tolerates missing fields, and skips unknown fields. 🌸

        /// <summary>
        /// Reads a protobuf varint as a <see cref="ulong"/> (used for tags &amp; lengths up to 64-bit).
        /// Returns the decoded value and the number of bytes consumed.
        /// </summary>
        private static (ulong value, int bytesRead) ReadRawVarint64(ReadOnlySpan<byte> buffer)
        {
            ulong result = 0;
            int shift = 0;
            int pos = 0;
            while (shift < 64)
            {
                byte b = buffer[pos++];
                result |= ((ulong)(b & 0x7F)) << shift;
                if ((b & 0x80) == 0)
                    return (result, pos);
                shift += 7;
            }
            ThrowMalformedVarintException();
            return default;
        }

        private static void ThrowMalformedVarintException()
        {
            throw new InvalidOperationException("Malformed varint (>10 bytes)");
        }

        /// <summary>
        /// Skips a single field value of the given wire-type starting at <paramref name="pos"/>,
        /// returning the new position. Used to tolerate unknown / future fields.
        /// </summary>
        private static int SkipField(ReadOnlySpan<byte> span, int pos, int wireType)
        {
            switch (wireType)
            {
                case 0: // varint
                {
                    var (_, n) = ReadRawVarint64(span.Slice(pos));
                    return pos + n;
                }
                case 1: // fixed64
                    return pos + 8;
                case 2: // length-delimited
                {
                    var (len, n) = ReadRawVarint64(span.Slice(pos));
                    return pos + n + (int)len;
                }
                case 5: // fixed32
                    return pos + 4;
                default:
                    ThrowInvalidOperationHandleUnsupportedWireType(wireType);
                    return pos;
            }
        }

        private static void ThrowInvalidOperationHandleUnsupportedWireType(int wireType)
        {
            throw new InvalidOperationException($"Unsupported wire-type {wireType}");
        }

        /// <summary>
        /// Represents a sliced view over an Akka <c>Payload</c> protobuf message.
        /// Parses lazily, in a single forward pass, with NO byte[] allocation. ✨
        /// </summary>
        public readonly struct PayloadParser
        {
            // Payload schema:
            //   bytes message         = 1;  // tag 0x0A, wireType 2
            //   int32 serializerId    = 2;  // tag 0x10, wireType 0 (omitted when == 0)
            //   bytes messageManifest = 3;  // tag 0x1A, wireType 2 (omitted when empty)
            private readonly ReadOnlyMemory<byte> _manifestBytes;
            private readonly ReadOnlyMemory<byte> _payloadMessageBytes;
            private readonly int _serId;

            /// <summary>Gets the raw user-message bytes (the value of <c>Payload.message</c>).</summary>
            public ReadOnlyMemory<byte> GetMessageByteSpan() => _payloadMessageBytes;

            /// <summary>Gets the serializer identifier (the value of <c>Payload.serializerId</c>).</summary>
            public int SerId => _serId;

            /// <summary>Gets the raw manifest bytes (the value of <c>Payload.messageManifest</c>).</summary>
            public ReadOnlyMemory<byte> Manifest() => _manifestBytes;

            /// <summary>Returns the manifest content as a UTF-8 byte span.</summary>
            public ReadOnlySpan<byte> GetManifestSpan() => _manifestBytes.Span;

            public PayloadParser(ReadOnlyMemory<byte> raw)
            {
                _payloadMessageBytes = default;
                _manifestBytes = default;
                _serId = 0;

                var span = raw.Span;
                int pos = 0;
                while (pos < span.Length)
                {
                    var (tag, tBytes) = ReadRawVarint64(span.Slice(pos));
                    pos += tBytes;
                    int fieldNum = (int)(tag >> 3);
                    int wireType = (int)(tag & 0x7);

                    switch (fieldNum)
                    {
                        case 1 when wireType == 2: // message bytes
                        {
                            var (len, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _payloadMessageBytes = raw.Slice(pos, (int)len);
                            pos += (int)len;
                            break;
                        }
                        case 2 when wireType == 0: // serializerId varint
                        {
                            var (val, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _serId = (int)val;
                            break;
                        }
                        case 3 when wireType == 2: // messageManifest bytes
                        {
                            var (len, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _manifestBytes = raw.Slice(pos, (int)len);
                            pos += (int)len;
                            break;
                        }
                        default:
                            pos = SkipField(span, pos, wireType);
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Tag-iterating parser for the <c>RemoteEnvelope</c> protobuf message.
        /// </summary>
        /// <remarks>
        /// Kept as a <see langword="struct"/> because it is consumed inline within
        /// <see cref="DecodeMessageLessCopies"/>. All fields are stored as windows
        /// over the original <see cref="ReadOnlyMemory{T}"/> — no allocations. 🌸
        /// </remarks>
        public readonly ref struct EnvelopeContainerParser
        {
            // RemoteEnvelope schema:
            //   ActorRefData recipient = 1;  // tag 0x0A
            //   Payload      message   = 2;  // tag 0x12
            //   ActorRefData sender    = 4;  // tag 0x22 (optional)
            //   fixed64      seq       = 5;  // tag 0x29 (omitted when == 0 default)
            // ActorRefData schema:
            //   string path = 1;             // tag 0x0A
            private readonly ReadOnlyMemory<byte> _rec;
            private readonly ReadOnlyMemory<byte> _msg;
            private readonly ReadOnlyMemory<byte> _sender;
            private readonly long _seq;

            /// <summary>Pulls out the <see cref="PayloadParser"/> from this envelope.</summary>
            public PayloadParser GetPayloadParser() => new PayloadParser(_msg);

            /// <summary>Gets the sender path as a UTF-8 byte span.</summary>
            public ReadOnlySpan<byte> SenderUtf8Bytes() => _sender.Span;

            /// <summary>Gets the sender path as a UTF-8 byte memory window.</summary>
            public ReadOnlyMemory<byte> SenderSegment() => _sender;

            /// <summary>Gets the recipient path as a UTF-8 byte span.</summary>
            public ReadOnlySpan<byte> ReceiverUtf8Bytes() => _rec.Span;

            /// <summary>Gets the recipient path as a UTF-8 byte memory window.</summary>
            public ReadOnlyMemory<byte> ReceiverSegment() => _rec;

            /// <summary>Gets the inner <c>Payload</c> bytes.</summary>
            public ReadOnlyMemory<byte> MessageArraySegment() => _msg;

            /// <summary>
            /// Gets the sequence number. Returns 0 when omitted on the wire (proto3 default).
            /// Note: <c>AkkaPduProtobuffCodec</c> uses <c>ulong.MaxValue</c> as the
            /// "no sequence" sentinel — callers must reinterpret accordingly.
            /// </summary>
            public long GetSequenceNumber() => _seq;

            public EnvelopeContainerParser(ReadOnlyMemory<byte> raw)
            {
                _rec = default;
                _msg = default;
                _sender = default;
                _seq = 0;
                //_hasSender = false;

                var span = raw.Span;
                int pos = 0;
                while (pos < span.Length)
                {
                    var (tag, tBytes) = ReadRawVarint64(span.Slice(pos));
                    pos += tBytes;
                    int fieldNum = (int)(tag >> 3);
                    int wireType = (int)(tag & 0x7);

                    switch (fieldNum)
                    {
                        case 1 when wireType == 2: // recipient ActorRefData
                        {
                            var (len, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _rec = ReadActorRefPath(raw.Slice(pos, (int)len));
                            pos += (int)len;
                            break;
                        }
                        case 2 when wireType == 2: // message Payload
                        {
                            var (len, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _msg = raw.Slice(pos, (int)len);
                            pos += (int)len;
                            break;
                        }
                        case 4 when wireType == 2: // sender ActorRefData (optional)
                        {
                            var (len, n) = ReadRawVarint64(span.Slice(pos));
                            pos += n;
                            _sender = ReadActorRefPath(raw.Slice(pos, (int)len));
                            //_hasSender = _sender.Length > 0;
                            pos += (int)len;
                            break;
                        }
                        case 5 when wireType == 1: // seq fixed64
                        {
                            _seq = (long)BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(pos, 8));
                            pos += 8;
                            break;
                        }
                        default:
                            pos = SkipField(span, pos, wireType);
                            break;
                    }
                }
            }

            /// <summary>
            /// Extracts the inner <c>path</c> string bytes from an <c>ActorRefData</c> sub-message.
            /// </summary>
            private static ReadOnlyMemory<byte> ReadActorRefPath(ReadOnlyMemory<byte> actorRefData)
            {
                var span = actorRefData.Span;
                int pos = 0;
                ReadOnlyMemory<byte> path = default;
                while (pos < span.Length)
                {
                    var (tag, tBytes) = ReadRawVarint64(span.Slice(pos));
                    pos += tBytes;
                    int fieldNum = (int)(tag >> 3);
                    int wireType = (int)(tag & 0x7);
                    if (fieldNum == 1 && wireType == 2)
                    {
                        var (len, n) = ReadRawVarint64(span.Slice(pos));
                        pos += n;
                        path = actorRefData.Slice(pos, (int)len);
                        //pos += (int)len;
                        break;
                    }
                    else
                    {
                        pos = SkipField(span, pos, wireType);
                    }
                }
                return path;
            }
        }
        /// <summary>
        /// Zero-copy override: hand-decodes the <c>AckAndEnvelopeContainer</c> directly from
        /// pool-rented memory — no intermediate <c>byte[]</c>, <see cref="ByteString"/>, or
        /// generated protobuf POCO is allocated. ✨
        ///
        /// <para>
        /// This is a fully field-order-independent tag-iterating parser at every level
        /// (outer container, envelope, payload, actor-ref). It tolerates proto3-default
        /// field elision (e.g. omitted <c>seq=0</c>, omitted <c>serializerId=0</c>) and
        /// skips unknown / future fields. 🌸
        /// </para>
        ///
        /// <!-- CopilotNotes: The previous implementation hard-coded the order
        ///      [ack? envelope?] at the top level and [recipient, message, sender?, seq]
        ///      inside the envelope, indexing `second.Span[0]` blindly. Real traffic
        ///      includes envelopes whose `seq == 0` (proto3 elides it) and that arrive
        ///      with no sender — in which case `second` is empty and the old code
        ///      threw IndexOutOfRangeException on the sender-tag check. -->
        /// </summary>
        public AckAndMessage DecodeMessageLessCopies(ReadOnlyMemory<byte> raw, IRemoteActorRefProvider provider, Address localAddress)
        {
            // AckAndEnvelopeContainer schema:
            //   AcknowledgementInfo ack      = 1;  // tag 0x0A
            //   RemoteEnvelope      envelope = 2;  // tag 0x12
            ReadOnlyMemory<byte> ackSection = default;
            ReadOnlyMemory<byte> msgSection = default;
            bool hasAck = false;
            bool hasMsg = false;

            var span = raw.Span;
            int pos = 0;
            while (pos < span.Length)
            {
                var (tag, tBytes) = ReadRawVarint64(span.Slice(pos));
                pos += tBytes;
                int fieldNum = (int)(tag >> 3);
                int wireType = (int)(tag & 0x7);

                switch (fieldNum)
                {
                    case 1 when wireType == 2: // ack
                    {
                        var (len, n) = ReadRawVarint64(span.Slice(pos));
                        pos += n;
                        ackSection = raw.Slice(pos, (int)len);
                        hasAck = ackSection.Length > 0;
                        pos += (int)len;
                        break;
                    }
                    case 2 when wireType == 2: // envelope
                    {
                        var (len, n) = ReadRawVarint64(span.Slice(pos));
                        pos += n;
                        msgSection = raw.Slice(pos, (int)len);
                        hasMsg = msgSection.Length > 0;
                        pos += (int)len;
                        break;
                    }
                    default:
                        pos = SkipField(span, pos, wireType);
                        break;
                }
            }

            Ack ackOption = null;
            Message messageOption = null;

            if (hasAck)
            {
                // AcknowledgementInfo is small (cumulativeAck + optional packed nacks); the
                // generated parser is plenty fast and avoids re-implementing packed varints.
                var ackInfo = AcknowledgementInfo.Parser.ParseFrom(UnsafeByteOperations.UnsafeWrap(ackSection));
                ackOption = new Ack(new SeqNo((long)ackInfo.CumulativeAck), ackInfo.Nacks.Select(x => new SeqNo((long)x)));
            }

            if (hasMsg)
            {
                messageOption = GetMessageOption(provider, localAddress, msgSection);
            }

            return new AckAndMessage(ackOption, messageOption);
        }

        private Message GetMessageOption(IRemoteActorRefProvider provider, Address localAddress, ReadOnlyMemory<byte> msgSection)
        {
            var msgParser = new EnvelopeContainerParser(msgSection);

            var senderSeg = msgParser.SenderSegment();
            var sender = !senderSeg.IsEmpty
                ? provider.ResolveActorRefWithLocalAddress(
                    Encoding.UTF8.GetString(senderSeg.Span), localAddress)
                : null;
                
            var recieverSegmentString = Encoding.UTF8.GetString(msgParser.ReceiverSegment().Span);
            var recipient = provider.ResolveActorRefWithLocalAddress(recieverSegmentString, localAddress);
            var recipientAddress = ActorPathCache.Cache.GetOrCompute(recieverSegmentString).Address;

            // Reinterpret the seq sentinel exactly like the generated-codec path does:
            //   ulong.MaxValue → reliable delivery NOT in use → seqOption == null
            //   anything else  → real seq number
            SeqNo? seqOption = null;
            var rawSeq = (ulong)msgParser.GetSequenceNumber();
            if (rawSeq != SeqUndefined)
            {
                unchecked { seqOption = new SeqNo((long)rawSeq); }
            }
                
            var payloadParser = msgParser.GetPayloadParser();
            var serialized = new SerializedMessage
            {
                Message = UnsafeByteOperations.UnsafeWrap(payloadParser.GetMessageByteSpan()),
                SerializerId = payloadParser.SerId,
                MessageManifest = UnsafeByteOperations.UnsafeWrap(payloadParser.Manifest()),
            };

            return new Message(recipient, recipientAddress, serialized, sender, seqOption);
        }

        private AcknowledgementInfo AckBuilder(Ack ack)
        {
            var acki = new AcknowledgementInfo();
            acki.CumulativeAck = (ulong)ack.CumulativeAck.RawValue;
            acki.Nacks.Add(from nack in ack.Nacks select (ulong)nack.RawValue);

            return acki;
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="localAddress">TBD</param>
        /// <param name="recipient">TBD</param>
        /// <param name="serializedMessage">TBD</param>
        /// <param name="senderOption">TBD</param>
        /// <param name="seqOption">TBD</param>
        /// <param name="ackOption">TBD</param>
        /// <returns>TBD</returns>
        public override ByteString ConstructMessage(Address localAddress, IActorRef recipient, SerializedMessage serializedMessage,
            IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null)
        {
            var ackAndEnvelope = new AckAndEnvelopeContainer();
            var envelope = new RemoteEnvelope() { Recipient = SerializeActorRef(recipient.Path.Address, recipient) };
            if (senderOption != null && senderOption.Path != null) { envelope.Sender = SerializeActorRef(localAddress, senderOption); }
            if (seqOption is { } seq) { envelope.Seq = (ulong)seq.RawValue; } else envelope.Seq = SeqUndefined;
            if (ackOption != null) { ackAndEnvelope.Ack = AckBuilder(ackOption); }
            envelope.Message = serializedMessage;
            ackAndEnvelope.Envelope = envelope;
            return ackAndEnvelope.ToByteString();
        }

        public override ByteString ConstructMessage(Address localAddress, IActorRef recipient,
            SysandTransportInfo systemAndTransportInfo,
            object message, IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null)
        {
            // return ConstructMessage_Original(localAddress, recipient, systemAndTransportInfo, message, senderOption,
            //     seqOption, ackOption);
            return ConstructMessage_Buffered(localAddress, recipient, systemAndTransportInfo, message, senderOption,
                seqOption, ackOption);
        }
        
        public ByteString ConstructMessage_Original(Address localAddress, IActorRef recipient,
            SysandTransportInfo systemAndTransportInfo,
            object message, IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null)
        {
            var ackAndEnvelope = new AckAndEnvelopeContainer();
            var envelope = new RemoteEnvelope() { Recipient = SerializeActorRef(recipient.Path.Address, recipient) };
            if (senderOption != null && senderOption.Path != null)
            {
                envelope.Sender = SerializeActorRef(localAddress, senderOption);
            }

            if (seqOption is { } seq)
            {
                envelope.Seq = (ulong)seq.RawValue;
            }
            else envelope.Seq = SeqUndefined;

            if (ackOption != null)
            {
                ackAndEnvelope.Ack = AckBuilder(ackOption);
            }

            using var f = ArrayPoolMemoryOwnerBufferedWriter.Create<byte>(1024);
            var (msgBytesWritten, serializerId, manifest) =
                MessageSerializer.Serialize(systemAndTransportInfo, message, f);
            Memory<byte> manifestMemory = default;
            if (!string.IsNullOrWhiteSpace(manifest))
            {
                //hax; if we have a manifest we probably have a message to go with it.
                //So, we 'cheat' and use maxByteCount here.
                var mbc = Encoding.UTF8.GetByteCount(manifest);
                manifestMemory = f.GetMemory(mbc).Slice(0, mbc);
                Encoding.UTF8.GetBytes(manifest, manifestMemory.Span);
                f.Advance(mbc);
            }


            // Hax; pool allocation of manifest encoding by now grabbing another slice.
            envelope.Message = new SerializedMessage()
            {
                Message = UnsafeByteOperations.UnsafeWrap(f.Memory.Slice(0, msgBytesWritten)),
                SerializerId = serializerId,
                MessageManifest = manifestMemory.Length > 0
                    ? UnsafeByteOperations.UnsafeWrap(manifestMemory)
                    : ByteString.Empty
            };
            ackAndEnvelope.Envelope = envelope;

            return ackAndEnvelope.ToByteString();
        }
        
        public ByteString ConstructMessage_Buffered(Address localAddress, IActorRef recipient,
            SysandTransportInfo systemAndTransportInfo,
            object message, IActorRef senderOption = null, SeqNo? seqOption = null, Ack ackOption = null)
        {
            var ackAndEnvelope = new AckAndEnvelopeContainerUtf8();
            var envelope = new RemoteEnvelopeUtf8();
            
            if (seqOption is { } seq)
            {
                envelope.Seq = (ulong)seq.RawValue;
            }
            else envelope.Seq = SeqUndefined;

            if (ackOption != null)
            {
                ackAndEnvelope.Ack = AckBuilder(ackOption);
            }

            using var f = ArrayPoolMemoryOwnerBufferedWriter.Create<byte>(1024);
            // Message is often the largest part of the message, so we allocate it first in hopes the excess growth minimizes recopies.
            // It's -possible- that the inverse  is a better pattern, we can test it later.
            var (msgByteEndIdx, serializerId, manifest) =
                MessageSerializer.Serialize(systemAndTransportInfo, message, f);
            // Memory<byte> manifestMemory = default;
            var manifestByteEndIdx = msgByteEndIdx;
            if (!string.IsNullOrWhiteSpace(manifest))
            {
                //hax; if we have a manifest we probably have a message to go with it.
                //So, we 'cheat' and use maxByteCount here.
                manifestByteEndIdx = msgByteEndIdx + (int)Encoding.UTF8.GetBytes(manifest, f);
                //var mbc = Encoding.UTF8.GetByteCount(manifest);
                //manifestMemory = f.GetMemory(mbc).Slice(0, mbc);
                //Encoding.UTF8.GetBytes(manifest, manifestMemory.Span);
                //f.Advance(mbc);
            }
            
            var recipiendEndIdx = manifestByteEndIdx+ WriteSerializedActorRefPathToBuffer(f, recipient.Path.Address, recipient); 
            
            // Grab sender last, now we can unwrap/construct.
            if (senderOption != null && senderOption.Path != null)
            {
                var senderEndIdx = recipiendEndIdx+ WriteSerializedActorRefPathToBuffer(f, localAddress, senderOption);
                envelope.Sender = new ActorRefDataUtf8()
                {
                    Path = UnsafeByteOperations.UnsafeWrap(f.Memory.Slice(recipiendEndIdx, senderEndIdx - recipiendEndIdx))
                };
            }
            envelope.Recipient = new ActorRefDataUtf8()
            {
                Path = UnsafeByteOperations.UnsafeWrap(f.Memory.Slice(manifestByteEndIdx, recipiendEndIdx - manifestByteEndIdx))
            };
            envelope.Message = new SerializedMessage()
            {
                Message = UnsafeByteOperations.UnsafeWrap(f.Memory.Slice(0, msgByteEndIdx)),
                SerializerId = serializerId,
                MessageManifest = manifestByteEndIdx > msgByteEndIdx ?
                    UnsafeByteOperations.UnsafeWrap(f.Memory.Slice(msgByteEndIdx,manifestByteEndIdx-msgByteEndIdx))
                    : ByteString.Empty
            };
            ackAndEnvelope.Envelope = envelope;
            return ackAndEnvelope.ToByteString();
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="ack">TBD</param>
        /// <returns>TBD</returns>
        public override ByteString ConstructPureAck(Ack ack)
        {
            return new AckAndEnvelopeContainer() { Ack = AckBuilder(ack) }.ToByteString();
        }

#region Internal methods
        private IAkkaPdu DecodeControlPdu(AkkaControlMessage controlPdu)
        {
            switch (controlPdu.CommandType)
            {
                case CommandType.Associate:
                    var handshakeInfo = controlPdu.HandshakeInfo;
                    if (handshakeInfo != null) // HasHandshakeInfo
                    {
                        return new Associate(new HandshakeInfo(DecodeAddress(handshakeInfo.Origin), (int)handshakeInfo.Uid));
                    }
                    break;
                case CommandType.Disassociate:
                    return new Disassociate(DisassociateInfo.Unknown);
                case CommandType.DisassociateQuarantined:
                    return new Disassociate(DisassociateInfo.Quarantined);
                case CommandType.DisassociateShuttingDown:
                    return new Disassociate(DisassociateInfo.Shutdown);
                case CommandType.Heartbeat:
                    return new Heartbeat();
            }

            throw new PduCodecException($"Decoding of control PDU failed, invalid format, unexpected {controlPdu}");
        }



        private ByteString DISASSOCIATE
        {
            get { return ConstructControlMessagePdu(CommandType.Disassociate); }
        }

        private ByteString DISASSOCIATE_SHUTTING_DOWN
        {
            get { return ConstructControlMessagePdu(CommandType.DisassociateShuttingDown); }
        }

        private ByteString DISASSOCIATE_QUARANTINED
        {
            get { return ConstructControlMessagePdu(CommandType.DisassociateQuarantined); }
        }

        private static ByteString ConstructControlMessagePdu(CommandType code, AkkaHandshakeInfo handshakeInfo = null)
        {
            var controlMessage = new AkkaControlMessage() { CommandType = code };
            if (handshakeInfo != null)
            {
                controlMessage.HandshakeInfo = handshakeInfo;
            }

            return new AkkaProtocolMessage() { Instruction = controlMessage }.ToByteString();
        }

        private static Address DecodeAddress(AddressData origin)
        {
            return new Address(origin.Protocol, origin.System, origin.Hostname, (int)origin.Port);
        }

        private static ActorRefData SerializeActorRef(Address defaultAddress, IActorRef actorRef)
        {
            return new ActorRefData()
            {
                Path = (!string.IsNullOrEmpty(actorRef.Path.Address.Host))
                    ? actorRef.Path.ToSerializationFormat()
                    : actorRef.Path.ToSerializationFormatWithAddress(defaultAddress)
            };
        }
        
        private static ActorRefDataUtf8 SerializeActorRefDataUtf8(Address defaultAddress, IActorRef actorRef)
        {
            return new ActorRefDataUtf8()
            {
                Path = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes((!string.IsNullOrEmpty(actorRef.Path.Address.Host))
                    ? actorRef.Path.ToSerializationFormat()
                    : actorRef.Path.ToSerializationFormatWithAddress(defaultAddress)))
            };
        }

        private static int WriteSerializedActorRefPathToBuffer(IBufferWriter<byte> writer, Address defaultAddress, IActorRef actorRef)
        {
            if (!string.IsNullOrEmpty(actorRef.Path.Address.Host))
            {
                return actorRef.Path.WriteToSerializationFormatUtf8Bytes(writer);
            }
            else
            {
                return actorRef.Path.WriteToSerializationFormatWithAddressUtf8Bytes(writer, defaultAddress);
            }
        }

        private static AddressData SerializeAddress(Address address)
        {
            if (string.IsNullOrEmpty(address.Host) || !address.Port.HasValue)
                throw new ArgumentException($"Address {address} could not be serialized: host or port missing");
            return new AddressData()
            {
                Hostname = address.Host,
                Port = (uint)address.Port.Value,
                System = address.System,
                Protocol = address.Protocol
            };
        }

#endregion

        public AkkaPduProtobuffCodec(ActorSystem system) : base(system)
        {
        }
    }
}
