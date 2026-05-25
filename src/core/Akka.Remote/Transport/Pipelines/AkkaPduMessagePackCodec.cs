//-----------------------------------------------------------------------
// <copyright file="AkkaPduMessagePackCodec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable

using System;
using System.Buffers;
using System.Linq;
using System.Text;
using Akka.Actor;
using Akka.Util;
using Akka.Remote.Transport.Pipelines.MessagePack;
using Google.Protobuf;
using MP = global::MessagePack;
using SerializedMessage = Akka.Remote.Serialization.Proto.Msg.Payload;

namespace Akka.Remote.Transport.Pipelines
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// MessagePack-based implementation of <see cref="AkkaPduCodec"/> that mirrors
    /// the <c>AkkaProtocolMessage</c> / <c>AckAndEnvelopeContainer</c> protobuf schema
    /// using source-generated (or dynamically emitted) MessagePack formatters.
    ///
    /// <para>
    /// This codec is <b>cluster-wide opt-in</b>: enable it only when every node in the
    /// cluster is running the pipelines transport with
    /// <c>akka.remote.pipe.tcp.envelope = messagepack</c>. Mixed-codec clusters will
    /// produce <see cref="PduCodecException"/> on decode.
    /// </para>
    ///
    /// <para>
    /// Wire format summary:
    /// <list type="bullet">
    ///   <item>
    ///     <b>Protocol frame</b> — a single discriminator byte (<see cref="ProtocolTag"/>)
    ///     followed by tag-specific payload bytes. This replaces the previous
    ///     <c>MpProtocolFrame</c> envelope so that the <see cref="ProtocolTag.Payload"/>
    ///     case can carry the inner <see cref="MpAckAndEnvelope"/> bytes <i>verbatim</i> with
    ///     no extra MessagePack header / length prefix and no extra buffer copy.
    ///   </item>
    ///   <item>
    ///     <b>Message envelope</b> — a MessagePack-serialized <see cref="MpAckAndEnvelope"/>
    ///     containing optional <see cref="MpAck"/> and <see cref="MpRemoteEnvelope"/>.
    ///     Mirrors <c>AckAndEnvelopeContainer</c>.
    ///   </item>
    /// </list>
    /// </para>
    ///
    /// <!-- CopilotNotes: The codec caches the static control frames (heartbeat,
    ///      disassociate variants) as 1-byte ByteStrings shared across the process.
    ///      The Payload path is fully zero-copy on decode (UnsafeWrap on a slice of
    ///      the inbound buffer) and a single-allocation prefix-and-copy on encode. -->
    /// </summary>
    internal sealed class AkkaPduMessagePackCodec : AkkaPduCodec
    {
        // ── Static cache for allocation-free control frames ─────────────────
        // CopilotNotes: Each cached frame is exactly one byte. We share them as
        // ByteStrings so the write path returns the same instance every time.

        private static readonly ByteString s_heartbeatBytes               = SingleByte(ProtocolTag.Heartbeat);
        private static readonly ByteString s_disassociateBytes            = SingleByte(ProtocolTag.Disassociate);
        private static readonly ByteString s_disassociateQuarantinedBytes = SingleByte(ProtocolTag.DisassociateQuarantined);
        private static readonly ByteString s_disassociateShuttingDownBytes= SingleByte(ProtocolTag.DisassociateShuttingDown);

        // Cached PDU singletons — these types carry no per-instance state on the
        // decode side, so we share them and skip per-frame allocations.
        private static readonly Heartbeat    s_heartbeatPdu                = new();
        private static readonly Disassociate s_disassociateUnknownPdu      = new(DisassociateInfo.Unknown);
        private static readonly Disassociate s_disassociateQuarantinedPdu  = new(DisassociateInfo.Quarantined);
        private static readonly Disassociate s_disassociateShutdownPdu     = new(DisassociateInfo.Shutdown);

        // ── Constructor ────────────────────────────────────────────────────────

        /// <summary>
        /// Creates a new instance of <see cref="AkkaPduMessagePackCodec"/>.
        /// </summary>
        /// <param name="system">The hosting actor system (for path caching).</param>
        public AkkaPduMessagePackCodec(ActorSystem system) : base(system) { }

        // ── Protocol-level encode / decode ─────────────────────────────────────

        /// <inheritdoc/>
        /// <summary>
        /// Reads the leading discriminator byte and dispatches to the matching PDU.
        /// For <see cref="ProtocolTag.Payload"/> the inner envelope bytes are returned
        /// as a zero-copy <see cref="ByteString"/> slice of <paramref name="raw"/>.
        /// </summary>
        /// <exception cref="PduCodecException">
        /// Thrown when the buffer is empty, the tag is unrecognised, or an Associate
        /// frame fails to deserialize.
        /// </exception>
        public override IAkkaPdu DecodePdu(ByteString raw)
        {
            if (raw.Length == 0)
                throw new PduCodecException("Empty MessagePack PDU frame.");

            var tag = raw[0];
            try
            {
                switch (tag)
                {
                    case ProtocolTag.Payload:
                        // CopilotNotes: zero-copy slice — UnsafeWrap shares the underlying
                        // buffer, no allocation, no memcpy. Inner envelope bytes start at offset 1.
                        return new Payload(UnsafeByteOperations.UnsafeWrap(raw.Memory.Slice(1)));

                    case ProtocolTag.Heartbeat:
                        return s_heartbeatPdu;

                    case ProtocolTag.Associate:
                        return DecodeAssociate(raw.Memory.Slice(1));

                    case ProtocolTag.Disassociate:
                        return s_disassociateUnknownPdu;
                    case ProtocolTag.DisassociateQuarantined:
                        return s_disassociateQuarantinedPdu;
                    case ProtocolTag.DisassociateShuttingDown:
                        return s_disassociateShutdownPdu;

                    default:
                        throw new PduCodecException(
                            $"Unknown MessagePack protocol tag: {tag}. " +
                            "Ensure all cluster nodes use the same envelope codec.");
                }
            }
            catch (MP.MessagePackSerializationException ex)
            {
                throw new PduCodecException("Failed to deserialize MessagePack protocol frame.", ex);
            }
        }

        /// <inheritdoc/>
        /// <summary>
        /// Wraps already-serialized inner-envelope bytes by prepending the
        /// <see cref="ProtocolTag.Payload"/> discriminator byte.
        /// Single allocation, single buffer copy of <paramref name="payload"/>.
        /// </summary>
        public override ByteString ConstructPayload(ByteString payload) =>
            PrependTag(ProtocolTag.Payload, payload.Memory);

        /// <inheritdoc cref="ConstructPayload(ByteString)"/>
        public override ByteString ConstructPayload(ReadOnlyMemory<byte> payload) =>
            PrependTag(ProtocolTag.Payload, payload);
        
        public override ReadOnlySequence<byte> ConstructPayloadSequence(ByteString payload) =>
          ProtobufSequenceSegment.ForFrame(ProtocolTag.PayloadTagBytes, payload.Memory);

        /// <inheritdoc/>
        public override ByteString ConstructAssociate(HandshakeInfo info)
        {
            if (string.IsNullOrEmpty(info.Origin.Host) || !info.Origin.Port.HasValue)
                throw new ArgumentException(
                    $"HandshakeInfo origin {info.Origin} is missing host or port.", nameof(info));

            var hi = new MpHandshakeInfo
            {
                Protocol = info.Origin.Protocol,
                System   = info.Origin.System,
                Hostname = info.Origin.Host!,
                Port     = info.Origin.Port!.Value,
                Uid      = info.Uid // int → long widening
            };

            // CopilotNotes: Could be optimized further with a pooled IBufferWriter that
            // writes the tag byte directly before MessagePack writes the body — but
            // Associate is sent only once per handshake so the simple path is fine.
            var body = MP.MessagePackSerializer.Serialize(hi);
            return PrependTag(ProtocolTag.Associate, body);
        }

        /// <inheritdoc/>
        public override ByteString ConstructDisassociate(DisassociateInfo reason) => reason switch
        {
            DisassociateInfo.Quarantined => s_disassociateQuarantinedBytes,
            DisassociateInfo.Shutdown    => s_disassociateShuttingDownBytes,
            _                            => s_disassociateBytes
        };

        /// <inheritdoc/>
        public override ByteString ConstructHeartbeat() => s_heartbeatBytes;

        // ── Message-level encode / decode ──────────────────────────────────────

        /// <inheritdoc/>
        /// <summary>
        /// Deserializes a MessagePack <see cref="MpAckAndEnvelope"/> and reconstructs an
        /// <see cref="AckAndMessage"/>.
        ///
        /// <para>
        /// Unlike the protobuf codec, this path creates a <see cref="MsgPackSerializedMessage"/>
        /// instead of a protobuf <c>SerializedMessage</c>, avoiding two <c>ByteString.CopyFrom</c>
        /// allocations per inbound message. The <see cref="Message"/> type carries a
        /// <c>HasMsgPackPayload</c> flag so <c>EndpointReader</c> can route to the correct
        /// Dispatch overload.
        /// </para>
        ///
        /// <!-- CopilotNotes: MpPayload.Message / MpPayload.Manifest are already
        ///      ReadOnlyMemory<byte> values whose backing byte[] is owned by the MessagePack
        ///      deserializer's output — safe to hold long-term even inside the reliable-delivery
        ///      receive buffer. -->
        /// </summary>
        public override AckAndMessage DecodeMessage(
            ByteString raw,
            IRemoteActorRefProvider provider,
            Address localAddress)
        {
            try
            {
                var msg = MP.MessagePackSerializer.Deserialize<MpAckAndEnvelope>(raw.Memory);

                // ── ACK half ──────────────────────────────────────────────────
                Ack? ackOption = null;
                if (msg.Ack is { } mpAck)
                {
                    ackOption = new Ack(
                        new SeqNo(mpAck.CumulativeAck),
                        mpAck.Nacks?.Select(n => new SeqNo(n)) ?? Enumerable.Empty<SeqNo>());
                }

                // ── Message half ──────────────────────────────────────────────
                Message? messageOption = null;
                if (msg.Envelope is { } env)
                {
                    var recipient = provider.ResolveActorRefWithLocalAddress(
                        env.RecipientPath, localAddress);

                    // CopilotNotes: Mirrors the ActorPathCache pattern in AkkaPduProtobuffCodec
                    // so we hit the same thread-local path-parse cache.
                    var recipientAddress = ActorPathCache.Cache
                        .GetOrCompute(env.RecipientPath).Address;

                    IActorRef? senderOption = null;
                    if (!string.IsNullOrEmpty(env.SenderPath))
                        senderOption = provider.ResolveActorRefWithLocalAddress(
                            env.SenderPath, localAddress);

                    SeqNo? seqOption = null;
                    if (env.Seq != MpRemoteEnvelope.SeqUndefined)
                    {
                        unchecked { seqOption = new SeqNo((long)env.Seq); }
                    }

                    // CopilotNotes: Build MsgPackSerializedMessage — no ByteString allocation!
                    // ReadOnlyMemory<byte> slices point directly into the MpPayload fields.
                    // Property pattern on a nullable struct unwraps it, so 'm' and 'mf' are
                    // already ReadOnlyMemory<byte> (non-nullable) — no .Value needed.
                    var msgPackPayload = new MsgPackSerializedMessage(
                        bytes:        env.Message.Message  is { Length: > 0 } m  ? m  : ReadOnlyMemory<byte>.Empty,
                        serializerId: env.Message.SerializerId,
                        manifest:     env.Message.Manifest is { Length: > 0 } mf ? mf : ReadOnlyMemory<byte>.Empty);

                    messageOption = new Message(
                        recipient, recipientAddress, msgPackPayload,
                        senderOption, seqOption);
                }

                return new AckAndMessage(ackOption, messageOption);
            }
            catch (MP.MessagePackSerializationException ex)
            {
                throw new PduCodecException(
                    "Failed to deserialize MessagePack AckAndEnvelope.", ex);
            }
        }

        /// <inheritdoc/>
        /// <summary>
        /// Serializes the outbound envelope directly to a pooled MessagePack buffer,
        /// bypassing the auto-generated formatters and the intermediate
        /// <see cref="MpAckAndEnvelope"/> / <see cref="MpRemoteEnvelope"/> /
        /// <see cref="MpPayload"/> / <see cref="MpAck"/> POCO graph.
        ///
        /// <para>
        /// Wire format is byte-for-byte identical to the auto-generated formatters:
        /// each <c>[MessagePackObject][Key(N)]</c> type is emitted as
        /// <c>WriteArrayHeader(maxKey + 1)</c> followed by the values in key order
        /// (<c>WriteNil</c> for null nullable values).
        /// </para>
        ///
        /// <para>
        /// <b>Allocation profile per call</b>:
        /// <list type="bullet">
        ///   <item>1 × <c>byte[]</c> for the final result (handed to <c>UnsafeWrap</c>);
        ///         unavoidable for the <see cref="ByteString"/> return contract.</item>
        ///   <item>0 × intermediate POCOs.</item>
        ///   <item>0 × <see cref="string"/> allocations for actor paths —
        ///         <see cref="ActorPath.WritePathWithAddress"/> streams chars into a
        ///         pooled scratch and we transcode to UTF-8 in a pooled byte buffer.</item>
        ///   <item>0 × LINQ / array allocations for NACKs — emitted directly from the
        ///         <see cref="Ack.Nacks"/> enumerable.</item>
        /// </list>
        /// </para>
        ///
        /// <!-- CopilotNotes: We deliberately do NOT use a custom IMessagePackFormatter here
        ///      because formatters cannot accept extra context (like the local Address used
        ///      to fill in local paths). Direct MessagePackWriter usage is cleaner and the
        ///      wire format stays interop-compatible with the source-generated formatters
        ///      because [Key(N)] objects serialize as plain index arrays. -->
        /// </summary>
        public override ByteString ConstructMessage(
            Address localAddress,
            IActorRef recipient,
            SerializedMessage serializedMessage,
            IActorRef? senderOption       = null,
            SeqNo? seqOption              = null,
            Ack? ackOption                = null)
        {
            // Single growing byte buffer for the entire envelope. Starts at 256 bytes
            // (covers most messages without a grow); ArrayBufferWriter doubles on demand.
            // The backing byte[] becomes the ByteString's backing store via UnsafeWrap
            // below — zero-copy handoff into the protobuf-style ByteString.

            // var bufferWriter = new ArrayBufferWriter<byte>(serializedMessage.Message.Length+256);
            using var bufferWriter = ArrayPoolMemoryOwnerBufferedWriter.Create<byte>();
            {
                // new ArrayBufferWriter<byte>(serializedMessage.Message.Length+256);
                var writer = new MP.MessagePackWriter(bufferWriter);

                // ── MpAckAndEnvelope: [ack, envelope] ─────────────────────────────
                writer.WriteArrayHeader(2);

                // -- Key 0: Ack (nullable) --
                if (ackOption is not null)
                    WriteAck(ref writer, ackOption);
                else
                    writer.WriteNil();

                // -- Key 1: Envelope --
                // MpRemoteEnvelope: [recipientPath, message, senderPath, seq] (4 keys)
                writer.WriteArrayHeader(4);

                // Key 0: RecipientPath — non-nullable string.
                // Mirror of SerializeActorRef(recipient.Path.Address, recipient): we pass
                // recipient.Path.Address as the fallback, so a local path renders against its
                // own address and a remote path renders against its own host+port (because
                // WritePathWithAddress prefers the path-owned address when host+port are set).
                WriteActorPathString(ref writer, recipient.Path, recipient.Path.Address);

                // Key 1: Message — non-nullable MpPayload [bytes, serializerId, manifest].
                WriteMpPayload(ref writer, serializedMessage);

                // Key 2: SenderPath — nullable string.
                if (senderOption?.Path is not null)
                    WriteActorPathString(ref writer, senderOption.Path, localAddress);
                else
                    writer.WriteNil();

                // Key 3: Seq — non-nullable ulong (sentinel ulong.MaxValue when undefined).
                writer.Write(seqOption.HasValue
                    ? unchecked((ulong)seqOption.Value.RawValue)
                    : MpRemoteEnvelope.SeqUndefined);

                writer.Flush();

                // Zero-copy handoff: the ByteString holds a reference to the
                // ArrayBufferWriter's internal byte[] via WrittenMemory. The buffer
                // writer itself is GC-able after this call; its array stays alive
                // through the ROM reference inside the ByteString.
                // return ByteString.CopyFrom(bufferWriter.Memory.Span);
                // return new ByteString(bufferWriter.Memory);
                return UnsafeByteOperations.UnsafeWrap(bufferWriter.Memory.ToArray());
            }
        }

        /// <summary>
        /// Writes an <see cref="Ack"/> as a MessagePack array matching the
        /// auto-generated <c>MpAck</c> formatter shape: <c>[cumulativeAck, nacks[]]</c>.
        /// </summary>
        private static void WriteAck(ref MP.MessagePackWriter writer, Ack ack)
        {
            writer.WriteArrayHeader(2);
            writer.Write(ack.CumulativeAck.RawValue);

            // Nacks array — always present (never nil) to match BuildMpAck's
            // .ToArray() semantic. Stream directly from the enumerable; if it's
            // ICollection we can fast-path the count.
            // if (ack.Nacks)
            {
                writer.WriteArrayHeader(ack.Nacks.Count);
                foreach (var n in ack.Nacks)
                    writer.Write(n.RawValue);
            }
        }

        /// <summary>
        /// Writes a <see cref="SerializedMessage"/> as the MessagePack <c>MpPayload</c>
        /// array shape: <c>[bytes, serializerId, manifest]</c>.
        ///
        /// <para>
        /// <b>Wire-format note</b>: empty byte buffers are written as zero-length
        /// <c>bin</c> (not <c>nil</c>) — this matches the auto-generated
        /// <c>ReadOnlyMemory&lt;byte&gt;?</c> formatter which round-trips
        /// <c>null</c> as <c>bin(0)</c>. Emitting <c>nil</c> here would shave
        /// 2 bytes per empty buffer but would not match the legacy POCO wire format
        /// and break decoder parity.
        /// </para>
        /// </summary>
        private static void WriteMpPayload(ref MP.MessagePackWriter writer, SerializedMessage msg)
        {
            writer.WriteArrayHeader(3);

            // Key 0: Message bytes. Always emit as bin (possibly zero-length).
            writer.Write(msg.Message.Span);

            // Key 1: SerializerId.
            writer.Write(msg.SerializerId);

            // Key 2: Manifest bytes. Always emit as bin (possibly zero-length).
            writer.Write(msg.MessageManifest.Span);
        }

        /// <summary>
        /// Writes <paramref name="path"/> as a MessagePack string, going chars →
        /// pooled char buffer → UTF-8 in a pooled byte buffer → MessagePack writer.
        /// No <see cref="string"/> allocation occurs along the way.
        /// </summary>
        /// <param name="writer">The destination MessagePack writer.</param>
        /// <param name="path">The actor path to serialize.</param>
        /// <param name="fallbackAddress">Address used when <paramref name="path"/> has no host/port.</param>
        private static void WriteActorPathString(
            ref MP.MessagePackWriter writer,
            ActorPath path,
            Address fallbackAddress)
        {
            // 1) Render the path chars into a pooled scratch via the new zero-alloc API.
            using var charScratch = new PooledCharBufferWriter();
            path.WritePathWithAddress(charScratch, fallbackAddress, includeUid: true);
            var chars = charScratch.WrittenSpan;

            // 2) Transcode chars → UTF-8 into a pooled byte buffer. Actor paths are
            //    ASCII in the common case (ValidAscii enforces < 128 for path elements)
            //    so byte count typically equals char count, but Encoding.UTF8 handles the
            //    cold path (e.g. an IDN host name).
            writer.Write(chars);
            // var utf8MaxLen = Encoding.UTF8.GetMaxByteCount(chars.Length);
            // var utf8Buffer = ArrayPool<byte>.Shared.Rent(utf8MaxLen);
            // try
            // {
            //     var utf8Len = Encoding.UTF8.GetBytes(chars, utf8Buffer);
            // 
            //     // 3) Emit as a single MessagePack str with header + body.
            //     writer.WriteString(utf8Buffer.AsSpan(0, utf8Len));
            // }
            // finally
            // {
            //     ArrayPool<byte>.Shared.Return(utf8Buffer);
            // }
        }

        /// <inheritdoc/>
        public override ByteString ConstructPureAck(Ack ack)
        {
            var container = new MpAckAndEnvelope { Ack = BuildMpAck(ack) };
            return UnsafeByteOperations.UnsafeWrap(
                MP.MessagePackSerializer.Serialize(container));
        }

        // ── Private helpers ────────────────────────────────────────────────────

        /// <summary>
        /// Allocates a single <c>byte[1 + tail.Length]</c>, writes <paramref name="tag"/>
        /// at index 0, copies <paramref name="tail"/> after it, and returns a zero-copy
        /// <see cref="ByteString"/> wrapping the array via <see cref="UnsafeByteOperations.UnsafeWrap(System.ReadOnlyMemory{byte})"/>.
        /// </summary>
        private static ByteString PrependTag(byte tag, ReadOnlyMemory<byte> tail)
        {
            var buf = new byte[1 + tail.Length];
            buf[0] = tag;
            tail.CopyTo(buf.AsMemory(1));
            return UnsafeByteOperations.UnsafeWrap(buf);
        }

        /// <summary>Returns a one-byte <see cref="ByteString"/> containing <paramref name="tag"/>.</summary>
        private static ByteString SingleByte(byte tag) =>
            UnsafeByteOperations.UnsafeWrap(new[] { tag });

        private static Associate DecodeAssociate(ReadOnlyMemory<byte> body)
        {
            if (body.IsEmpty)
                throw new PduCodecException(
                    "Associate MessagePack frame is missing HandshakeInfo body.");

            var hi = MP.MessagePackSerializer.Deserialize<MpHandshakeInfo>(body);
            var origin = new Address(hi.Protocol, hi.System, hi.Hostname, hi.Port);
            return new Associate(new HandshakeInfo(origin, (int)hi.Uid));
        }

        private static MpAck BuildMpAck(Ack ack) =>
            new()
            {
                CumulativeAck = ack.CumulativeAck.RawValue,
                Nacks         = ack.Nacks.Select(n => n.RawValue).ToArray()
            };

        // NOTE: BuildMpPayload + SerializeActorRef were removed when ConstructMessage
        // switched to the direct-MessagePackWriter path. Path serialization is now
        // handled by WriteActorPathString via ActorPath.WritePathWithAddress, and the
        // payload bytes are written inline via WriteMpPayload.
    }
}

