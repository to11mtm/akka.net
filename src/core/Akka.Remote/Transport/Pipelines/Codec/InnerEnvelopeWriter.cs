//-----------------------------------------------------------------------
// <copyright file="InnerEnvelopeWriter.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Akka.Remote.Transport;

namespace Akka.Remote.Transport.Pipelines.Codec
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Hand-written, zero-allocation writer for the inner
    /// <c>AckAndEnvelopeContainer</c> protobuf message. ✨
    ///
    /// <para>
    /// Replaces <c>ackAndEnvelope.ToByteString()</c> (copy W4 in the analysis)
    /// with a direct write into a caller-supplied <see cref="IBufferWriter{T}"/>.
    /// Output is <b>byte-identical</b> to the generated protobuf code.
    /// </para>
    ///
    /// <para>
    /// Wire schema (from <c>WireFormats.proto</c>):
    /// <code>
    /// message AckAndEnvelopeContainer {
    ///   AcknowledgementInfo ack      = 1; // tag 0x0A (optional)
    ///   RemoteEnvelope      envelope = 2; // tag 0x12 (optional)
    /// }
    /// message RemoteEnvelope {
    ///   ActorRefData recipient = 1; // tag 0x0A
    ///   Payload      message   = 2; // tag 0x12
    ///   ActorRefData sender    = 4; // tag 0x22 (optional)
    ///   fixed64      seq       = 5; // tag 0x29 (wireType 1 = 64-bit)
    /// }
    /// message AcknowledgementInfo {
    ///   fixed64          cumulativeAck = 1; // tag 0x09
    ///   repeated fixed64 nacks         = 2; // tag 0x12 (packed)
    /// }
    /// message ActorRefData { string path = 1; } // tag 0x0A
    /// message Payload {
    ///   bytes  message         = 1; // tag 0x0A
    ///   int32  serializerId    = 2; // tag 0x10
    ///   bytes  messageManifest = 3; // tag 0x1A
    /// }
    /// </code>
    /// </para>
    ///
    /// <!-- CopilotNotes: The key optimization here vs. generated protobuf code:
    ///      we pre-compute all sub-message sizes, then write everything in a single forward
    ///      pass into one IBufferWriter<byte> — no intermediate byte[] allocation.
    ///      In the current (PR-A) wiring, this is called from EndpointWriter via
    ///      PipeAssociationHandle; in PR-B it will be called before the outer wrapper too.
    ///
    ///      SeqUndefined sentinel matches AkkaPduProtobuffCodec's convention:
    ///        seqOption == null → write Seq = ulong.MaxValue (0xFFFFFFFFFFFFFFFF)
    ///        seqOption.Value.RawValue == 0 → omit (proto3 default; receiver reads 0)
    ///        otherwise → write the raw value
    ///      This is wire-identical to the generated codec's behaviour. 🌸 -->
    /// </summary>
    internal static class InnerEnvelopeWriter
    {
        // ── Wire tag constants ───────────────────────────────────────────────
        // AckAndEnvelopeContainer
        private const byte AackTag      = 0x0A;  // field 1, wireType 2
        private const byte AenvTag      = 0x12;  // field 2, wireType 2

        // RemoteEnvelope
        private const byte EnvRecipient = 0x0A;  // field 1, wireType 2
        private const byte EnvMessage   = 0x12;  // field 2, wireType 2
        private const byte EnvSender    = 0x22;  // field 4, wireType 2
        private const byte EnvSeqTag    = 0x29;  // field 5, wireType 1 (64-bit)

        // AcknowledgementInfo
        private const byte AckCumTag    = 0x09;  // field 1, wireType 1 (64-bit)
        private const byte AckNackTag   = 0x12;  // field 2, wireType 2 (packed)

        // ActorRefData
        private const byte ArPathTag    = 0x0A;  // field 1, wireType 2 (string)

        // Payload (SerializedMessage)
        private const byte PlMsgTag     = 0x0A;  // field 1, wireType 2 (bytes)
        private const byte PlSerIdTag   = 0x10;  // field 2, wireType 0 (varint)
        private const byte PlManifTag   = 0x1A;  // field 3, wireType 2 (bytes)

        // Sentinel for "no sequence number" (matches AkkaPduProtobuffCodec.SeqUndefined)
        internal const ulong SeqUndefined = ulong.MaxValue;

        // ── Public API ───────────────────────────────────────────────────────

        /// <summary>
        /// Writes a complete <c>AckAndEnvelopeContainer</c> message directly into
        /// <paramref name="writer"/> without any intermediate allocations. 🌸
        ///
        /// <para>
        /// All parameters mirror those accepted by
        /// <c>AkkaPduProtobuffCodec.ConstructMessage</c>, allowing a drop-in zero-copy
        /// replacement on the hot write path.
        /// </para>
        /// </summary>
        /// <param name="writer">The destination buffer writer (e.g. a pooled <see cref="PooledFrame"/>).</param>
        /// <param name="recipientPath">Serialised actor path of the recipient (<c>ActorRefData.Path</c>).</param>
        /// <param name="senderPath">Optional serialised actor path of the sender.</param>
        /// <param name="seq">
        ///   Sequence number. Use <see cref="SeqUndefined"/> (<c>ulong.MaxValue</c>) when the message
        ///   does not participate in reliable delivery.
        /// </param>
        /// <param name="ack">Optional acknowledgement. Pass <c>null</c> when no ack is piggybacked.</param>
        /// <param name="messageBytes">The raw serialized user message bytes (<c>Payload.message</c>).</param>
        /// <param name="serializerId">The serializer identifier (<c>Payload.serializerId</c>).</param>
        /// <param name="manifestBytes">The UTF-8 manifest bytes (<c>Payload.messageManifest</c>).</param>
        public static void WriteAckAndEnvelope(
            IBufferWriter<byte> writer,
            string recipientPath,
            string? senderPath,
            ulong seq,
            Ack? ack,
            ReadOnlySpan<byte> messageBytes,
            int serializerId,
            ReadOnlySpan<byte> manifestBytes)
        {
            // ── Pre-compute all sub-message sizes ─────────────────────────────
            int payloadSize   = ComputePayloadSize(messageBytes.Length, serializerId, manifestBytes.Length);
            int recipientSize = ComputeActorRefSize(recipientPath);
            int senderSize    = senderPath != null ? ComputeActorRefSize(senderPath) : 0;
            bool writeSeq     = seq != 0; // proto3: omit when default (0); SeqUndefined is non-zero, so IS written
            int seqFieldSize  = writeSeq ? 9 : 0; // 1 tag + 8 bytes fixed64

            int envSize = FieldSize(EnvRecipient, recipientSize)
                        + FieldSize(EnvMessage, payloadSize)
                        + (senderSize > 0 ? FieldSize(EnvSender, senderSize) : 0)
                        + seqFieldSize;

            int ackInfoSize = ack != null ? ComputeAckInfoSize(ack) : 0;

            // ── Write AckAndEnvelopeContainer ─────────────────────────────────
            // field 1 (ack) if present
            if (ack != null && ackInfoSize > 0)
            {
                WriteLengthDelimitedHeader(writer, AackTag, ackInfoSize);
                WriteAckInfo(writer, ack);
            }

            // field 2 (envelope)
            WriteLengthDelimitedHeader(writer, AenvTag, envSize);

            // ── Write RemoteEnvelope ──────────────────────────────────────────
            // field 1: recipient ActorRefData
            WriteLengthDelimitedHeader(writer, EnvRecipient, recipientSize);
            WriteActorRefData(writer, recipientPath);

            // field 2: Payload (message)
            WriteLengthDelimitedHeader(writer, EnvMessage, payloadSize);
            WritePayload(writer, messageBytes, serializerId, manifestBytes);

            // field 4: sender ActorRefData (optional)
            if (senderSize > 0)
            {
                WriteLengthDelimitedHeader(writer, EnvSender, senderSize);
                WriteActorRefData(writer, senderPath!);
            }

            // field 5: seq fixed64 (written unless default 0)
            if (writeSeq)
            {
                var sp = writer.GetSpan(9);
                sp[0] = EnvSeqTag;
                ProtobufWire.WriteFixed64(sp.Slice(1), seq);
                writer.Advance(9);
            }
        }

        /// <summary>
        /// Writes a pure-ack <c>AckAndEnvelopeContainer</c> (no envelope, only ack).
        /// Mirrors <c>AkkaPduProtobuffCodec.ConstructPureAck</c>. 🌸
        /// </summary>
        public static void WritePureAck(IBufferWriter<byte> writer, Ack ack)
        {
            int ackInfoSize = ComputeAckInfoSize(ack);
            WriteLengthDelimitedHeader(writer, AackTag, ackInfoSize);
            WriteAckInfo(writer, ack);
        }

        /// <summary>
        /// Computes the total byte count that
        /// <see cref="WriteAckAndEnvelope"/> will write for the given parameters.
        /// Use this to pre-rent a <see cref="PooledFrame"/> of the right size.
        /// </summary>
        public static int ComputeAckAndEnvelopeSize(
            string recipientPath,
            string? senderPath,
            ulong seq,
            Ack? ack,
            int messageLength,
            int serializerId,
            int manifestLength)
        {
            int payloadSize   = ComputePayloadSize(messageLength, serializerId, manifestLength);
            int recipientSize = ComputeActorRefSize(recipientPath);
            int senderSize    = senderPath != null ? ComputeActorRefSize(senderPath) : 0;
            int seqFieldSize  = seq != 0 ? 9 : 0;

            int envSize = FieldSize(EnvRecipient, recipientSize)
                        + FieldSize(EnvMessage, payloadSize)
                        + (senderSize > 0 ? FieldSize(EnvSender, senderSize) : 0)
                        + seqFieldSize;

            int ackInfoSize = ack != null ? ComputeAckInfoSize(ack) : 0;

            int total = FieldSize(AenvTag, envSize);
            if (ackInfoSize > 0)
                total += FieldSize(AackTag, ackInfoSize);
            return total;
        }

        // ── Private size helpers ─────────────────────────────────────────────

        /// <summary>Bytes needed to encode a length-delimited field: 1 (tag) + varint(subSize) + subSize.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FieldSize(byte tag, int subSize)
            => 1 + ProtobufWire.ComputeVarintSize((uint)subSize) + subSize;

        private static int ComputeActorRefSize(string path)
        {
            // ActorRefData: string path = 1 → tag(0x0A) + varint(utf8Len) + utf8Bytes
            var utf8Len = Encoding.UTF8.GetByteCount(path);
            if (utf8Len == 0) return 0;
            return 1 + ProtobufWire.ComputeVarintSize((uint)utf8Len) + utf8Len;
        }

        private static int ComputePayloadSize(int msgLen, int serializerId, int manifestLen)
        {
            // Payload: bytes message = 1; int32 serializerId = 2; bytes messageManifest = 3;
            int size = 0;
            if (msgLen > 0)
                size += 1 + ProtobufWire.ComputeVarintSize((uint)msgLen) + msgLen;
            if (serializerId != 0) // proto3: omit default 0
                size += 1 + ProtobufWire.ComputeVarintSize((uint)serializerId);
            if (manifestLen > 0)
                size += 1 + ProtobufWire.ComputeVarintSize((uint)manifestLen) + manifestLen;
            return size;
        }

        private static int ComputeAckInfoSize(Ack ack)
        {
            // AcknowledgementInfo: fixed64 cumulativeAck = 1; repeated fixed64 nacks = 2 (packed);
            int size = 9; // tag(0x09) + 8 bytes fixed64 cumulativeAck
            int nackCount = 0;
            foreach (var _ in ack.Nacks) nackCount++;
            if (nackCount > 0)
            {
                int packedLen = nackCount * 8;
                size += 1 + ProtobufWire.ComputeVarintSize((uint)packedLen) + packedLen;
            }
            return size;
        }

        // ── Private write helpers ────────────────────────────────────────────

        /// <summary>Writes a length-delimited field header: tag byte + varint(length). Does NOT write the body.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteLengthDelimitedHeader(IBufferWriter<byte> writer, byte tag, int length)
        {
            var lenSize = ProtobufWire.ComputeVarintSize((uint)length);
            var sp      = writer.GetSpan(1 + lenSize);
            sp[0]       = tag;
            ProtobufWire.WriteVarint(sp.Slice(1), (uint)length);
            writer.Advance(1 + lenSize);
        }

        /// <summary>Writes an <c>ActorRefData { path = s }</c> sub-message body (without the outer length-delimited header).</summary>
        private static void WriteActorRefData(IBufferWriter<byte> writer, string path)
        {
            if (string.IsNullOrEmpty(path)) return;
            var utf8Len = Encoding.UTF8.GetByteCount(path);
            var lenSize = ProtobufWire.ComputeVarintSize((uint)utf8Len);
            var total   = 1 + lenSize + utf8Len;
            var sp      = writer.GetSpan(total);
            sp[0]       = ArPathTag;
            ProtobufWire.WriteVarint(sp.Slice(1), (uint)utf8Len);
            Encoding.UTF8.GetBytes(path, sp.Slice(1 + lenSize));
            writer.Advance(total);
        }

        /// <summary>Writes a <c>Payload</c> sub-message body (without the outer length-delimited header).</summary>
        private static void WritePayload(
            IBufferWriter<byte> writer,
            ReadOnlySpan<byte> messageBytes,
            int serializerId,
            ReadOnlySpan<byte> manifestBytes)
        {
            if (messageBytes.Length > 0)
                ProtobufWire.WriteLengthDelimited(writer, PlMsgTag, messageBytes);

            if (serializerId != 0) // proto3: skip default
            {
                var serSize = ProtobufWire.ComputeVarintSize((uint)serializerId);
                var sp      = writer.GetSpan(1 + serSize);
                sp[0]       = PlSerIdTag;
                ProtobufWire.WriteVarint(sp.Slice(1), (uint)serializerId);
                writer.Advance(1 + serSize);
            }

            if (manifestBytes.Length > 0)
                ProtobufWire.WriteLengthDelimited(writer, PlManifTag, manifestBytes);
        }

        /// <summary>Writes an <c>AcknowledgementInfo</c> sub-message body (without the outer length-delimited header).</summary>
        private static void WriteAckInfo(IBufferWriter<byte> writer, Ack ack)
        {
            // field 1: fixed64 cumulativeAck
            var cumSp = writer.GetSpan(9);
            cumSp[0] = AckCumTag;
            ProtobufWire.WriteFixed64(cumSp.Slice(1), (ulong)ack.CumulativeAck.RawValue);
            writer.Advance(9);

            // field 2: packed repeated fixed64 nacks
            // First, count and collect nacks.
            var nackList = new List<ulong>();
            foreach (var n in ack.Nacks)
                nackList.Add((ulong)n.RawValue);

            if (nackList.Count > 0)
            {
                int packedLen = nackList.Count * 8;
                WriteLengthDelimitedHeader(writer, AckNackTag, packedLen);
                var sp = writer.GetSpan(packedLen);
                for (int i = 0; i < nackList.Count; i++)
                    ProtobufWire.WriteFixed64(sp.Slice(i * 8), nackList[i]);
                writer.Advance(packedLen);
            }
        }
    }
}

