//-----------------------------------------------------------------------
// <copyright file="InnerEnvelopeReader.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Akka.Remote.Transport;

namespace Akka.Remote.Transport.Pipelines.Codec
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// The decoded fields of an inbound <c>AckAndEnvelopeContainer</c> message,
    /// with lazy / zero-copy access to string and byte fields. 🌸
    ///
    /// <!-- CopilotNotes: All ReadOnlySequence fields are zero-copy slices of the
    ///      PipeReader segment(s). Callers must not hold these past the read-loop
    ///      AdvanceTo call — promote to a copy if the data needs to outlive the
    ///      current iteration (e.g. when crossing an actor mailbox boundary). -->
    /// </summary>
    internal struct DecodedInnerEnvelope
    {
        // ── Ack (optional — present on piggyback or pure-ack messages) ────────
        /// <summary><c>true</c> when the message carries an <c>AcknowledgementInfo</c>.</summary>
        public bool HasAck;

        /// <summary>The cumulative ack sequence number (<c>AcknowledgementInfo.cumulativeAck</c>).</summary>
        public ulong CumulativeAck;

        /// <summary>Raw packed <c>fixed64</c> bytes for the nack list. Length / 8 = nack count.</summary>
        public ReadOnlySequence<byte> PackedNacksBytes;

        // ── Envelope (absent on pure-ack messages) ────────────────────────────
        /// <summary><c>true</c> when the message carries a <c>RemoteEnvelope</c>.</summary>
        public bool HasEnvelope;

        /// <summary>Raw UTF-8 bytes of the recipient actor path (<c>ActorRefData.path</c>).</summary>
        public ReadOnlySequence<byte> RecipientPathBytes;

        /// <summary><c>true</c> when an optional sender is present.</summary>
        public bool HasSender;

        /// <summary>Raw UTF-8 bytes of the sender actor path (<c>ActorRefData.path</c>).</summary>
        public ReadOnlySequence<byte> SenderPathBytes;

        /// <summary>
        /// Raw sequence number (<c>RemoteEnvelope.seq</c> as <c>fixed64</c>).
        /// <see cref="InnerEnvelopeWriter.SeqUndefined"/> (=<c>ulong.MaxValue</c>) means no reliable delivery.
        /// </summary>
        public ulong Seq;

        // ── Payload ───────────────────────────────────────────────────────────
        /// <summary>
        /// Zero-copy slice of the user-serialized payload bytes (<c>Payload.message</c>).
        /// Alias for PipeReader memory — do not hold past AdvanceTo.
        /// </summary>
        public ReadOnlySequence<byte> MessageBytes;

        /// <summary>Serializer identifier (<c>Payload.serializerId</c>).</summary>
        public int SerializerId;

        /// <summary>Raw manifest bytes (<c>Payload.messageManifest</c>).</summary>
        public ReadOnlySequence<byte> ManifestBytes;

        // ── Convenience helpers ───────────────────────────────────────────────

        /// <summary>
        /// Decodes the recipient path to a <see langword="string"/> (UTF-8).
        /// Allocates a new <see langword="string"/>; only call once per message. 🌸
        /// </summary>
        public string DecodeRecipientPath() =>
            HasEnvelope && RecipientPathBytes.Length > 0
                ? Encoding.UTF8.GetString(RecipientPathBytes)
                : string.Empty;

        /// <summary>
        /// Decodes the optional sender path to a <see langword="string"/> (UTF-8).
        /// Returns <see langword="null"/> when no sender is present.
        /// </summary>
        public string? DecodeSenderPath() =>
            HasSender && SenderPathBytes.Length > 0
                ? Encoding.UTF8.GetString(SenderPathBytes)
                : null;

        /// <summary>
        /// Decodes the manifest to a <see langword="string"/> (UTF-8).
        /// Returns <see cref="string.Empty"/> when no manifest is present.
        /// </summary>
        public string DecodeManifest() =>
            ManifestBytes.Length > 0
                ? Encoding.UTF8.GetString(ManifestBytes)
                : string.Empty;

        /// <summary>
        /// Enumerates the nack sequence numbers from the packed <c>fixed64</c> nacks field.
        /// Allocates a <see cref="List{T}"/>; use sparingly (nacks are rare). 🌸
        /// </summary>
        public IReadOnlyList<ulong> DecodeNacks()
        {
            if (PackedNacksBytes.Length == 0) return Array.Empty<ulong>();

            var count  = (int)(PackedNacksBytes.Length / 8);
            var result = new List<ulong>(count);
            var reader = new SequenceReader<byte>(PackedNacksBytes);
            for (int i = 0; i < count; i++)
            {
                ProtobufWire.TryReadFixed64(ref reader, out var val);
                result.Add(val);
            }
            return result;
        }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// Hand-written, zero-copy reader for the inner <c>AckAndEnvelopeContainer</c>
    /// protobuf message. ✨
    ///
    /// <para>
    /// Replaces <c>AckAndEnvelopeContainer.Parser.ParseFrom(raw)</c>
    /// (copy R3 in the analysis) with a direct tag-loop that <b>slices each field's
    /// bytes out of the caller's <see cref="ReadOnlySequence{T}"/> without copying</b>.
    /// </para>
    ///
    /// <para>
    /// Wire schema (from <c>WireFormats.proto</c>):
    /// <code>
    /// message AckAndEnvelopeContainer {
    ///   AcknowledgementInfo ack      = 1; // tag 0x0A (optional)
    ///   RemoteEnvelope      envelope = 2; // tag 0x12 (optional)
    /// }
    /// </code>
    /// </para>
    ///
    /// <!-- CopilotNotes: Fields can arrive in any order (proto3 allows this), but in
    ///      practice the generated code writes them in field-number order. We handle
    ///      out-of-order gracefully via the tag-loop and SkipField for unknowns. 🌸 -->
    /// </summary>
    internal static class InnerEnvelopeReader
    {
        // ── Wire tag constants (AckAndEnvelopeContainer) ─────────────────────
        private const byte AackTag = 0x0A;  // field 1 AcknowledgementInfo
        private const byte AenvTag = 0x12;  // field 2 RemoteEnvelope

        // ── Wire tag constants (RemoteEnvelope) ──────────────────────────────
        private const byte EnvRecipient = 0x0A;  // field 1 ActorRefData
        private const byte EnvMessage   = 0x12;  // field 2 Payload
        private const byte EnvSender    = 0x22;  // field 4 ActorRefData
        private const byte EnvSeqTag    = 0x29;  // field 5 fixed64

        // ── Wire tag constants (AcknowledgementInfo) ─────────────────────────
        private const byte AckCumTag  = 0x09;  // field 1 fixed64 cumulativeAck
        private const byte AckNackTag = 0x12;  // field 2 packed fixed64 nacks

        // ── Wire tag constants (ActorRefData) ────────────────────────────────
        private const byte ArPathTag = 0x0A;  // field 1 string path

        // ── Wire tag constants (Payload / SerializedMessage) ─────────────────
        private const byte PlMsgTag   = 0x0A;  // field 1 bytes message
        private const byte PlSerIdTag = 0x10;  // field 2 int32 serializerId
        private const byte PlManifTag = 0x1A;  // field 3 bytes messageManifest

        // ── Public API ───────────────────────────────────────────────────────

        /// <summary>
        /// Attempts to decode an <c>AckAndEnvelopeContainer</c> from <paramref name="input"/>.
        ///
        /// <para>
        /// All <see cref="ReadOnlySequence{T}"/> fields in <paramref name="result"/> are
        /// <b>zero-copy slices</b> of <paramref name="input"/> — they alias the caller's
        /// memory. Do not hold them past the enclosing <c>PipeReader.AdvanceTo</c> call
        /// unless they have been copied into a long-lived buffer. 🌸
        /// </para>
        ///
        /// Returns <c>false</c> if the buffer is malformed or truncated.
        /// </summary>
        public static bool TryRead(
            ReadOnlySequence<byte> input,
            out DecodedInnerEnvelope result)
        {
            result = default;
            var reader = new SequenceReader<byte>(input);

            while (reader.Remaining > 0)
            {
                if (!ProtobufWire.TryReadVarint(ref reader, out uint rawTag))
                    return false;

                var tag = (byte)(rawTag & 0xFF);
                switch (tag)
                {
                    case AackTag: // AcknowledgementInfo ack (wireType 2)
                    {
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint ackLen)) return false;
                        var ackBody = input.Slice(reader.Position, (long)ackLen);
                        if (!TryReadAckInfo(ackBody, ref result)) return false;
                        reader.Advance((long)ackLen);
                        result.HasAck = true;
                        break;
                    }

                    case AenvTag: // RemoteEnvelope envelope (wireType 2)
                    {
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint envLen)) return false;
                        var envBody = input.Slice(reader.Position, (long)envLen);
                        if (!TryReadRemoteEnvelope(envBody, ref result)) return false;
                        reader.Advance((long)envLen);
                        result.HasEnvelope = true;
                        break;
                    }

                    default:
                    {
                        // Unknown field — skip based on wire type.
                        byte wireType = (byte)(rawTag & 0x7);
                        if (!ProtobufWire.TrySkipField(ref reader, wireType))
                            return false;
                        break;
                    }
                }
            }

            return true;
        }

        // ── Private sub-message readers ──────────────────────────────────────

        private static bool TryReadAckInfo(
            ReadOnlySequence<byte> input,
            ref DecodedInnerEnvelope result)
        {
            var reader = new SequenceReader<byte>(input);
            while (reader.Remaining > 0)
            {
                if (!ProtobufWire.TryReadVarint(ref reader, out uint rawTag)) return false;
                var tag = (byte)(rawTag & 0xFF);

                switch (tag)
                {
                    case AckCumTag: // fixed64 cumulativeAck (wireType 1)
                        if (!ProtobufWire.TryReadFixed64(ref reader, out result.CumulativeAck))
                            return false;
                        break;

                    case AckNackTag: // packed repeated fixed64 nacks (wireType 2)
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint packedLen)) return false;
                        result.PackedNacksBytes = input.Slice(reader.Position, (long)packedLen);
                        reader.Advance((long)packedLen);
                        break;

                    default:
                        if (!ProtobufWire.TrySkipField(ref reader, (byte)(rawTag & 0x7))) return false;
                        break;
                }
            }
            return true;
        }

        private static bool TryReadRemoteEnvelope(
            ReadOnlySequence<byte> input,
            ref DecodedInnerEnvelope result)
        {
            var reader = new SequenceReader<byte>(input);
            while (reader.Remaining > 0)
            {
                if (!ProtobufWire.TryReadVarint(ref reader, out uint rawTag)) return false;
                var tag = (byte)(rawTag & 0xFF);

                switch (tag)
                {
                    case EnvRecipient: // ActorRefData recipient (wireType 2)
                    {
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint arLen)) return false;
                        var arBody = input.Slice(reader.Position, (long)arLen);
                        result.RecipientPathBytes = ExtractStringField(arBody, ArPathTag);
                        reader.Advance((long)arLen);
                        break;
                    }

                    case EnvMessage: // Payload message (wireType 2)
                    {
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint plLen)) return false;
                        var plBody = input.Slice(reader.Position, (long)plLen);
                        TryReadPayload(plBody, ref result);
                        reader.Advance((long)plLen);
                        break;
                    }

                    case EnvSender: // ActorRefData sender (wireType 2, optional)
                    {
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint arLen)) return false;
                        var arBody = input.Slice(reader.Position, (long)arLen);
                        result.SenderPathBytes = ExtractStringField(arBody, ArPathTag);
                        result.HasSender = result.SenderPathBytes.Length > 0;
                        reader.Advance((long)arLen);
                        break;
                    }

                    case EnvSeqTag: // fixed64 seq (wireType 1)
                        if (!ProtobufWire.TryReadFixed64(ref reader, out result.Seq)) return false;
                        break;

                    default:
                        if (!ProtobufWire.TrySkipField(ref reader, (byte)(rawTag & 0x7))) return false;
                        break;
                }
            }
            return true;
        }

        private static void TryReadPayload(
            ReadOnlySequence<byte> input,
            ref DecodedInnerEnvelope result)
        {
            var reader = new SequenceReader<byte>(input);
            while (reader.Remaining > 0)
            {
                if (!ProtobufWire.TryReadVarint(ref reader, out uint rawTag)) return;
                var tag = (byte)(rawTag & 0xFF);

                switch (tag)
                {
                    case PlMsgTag: // bytes message (wireType 2)
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint msgLen)) return;
                        result.MessageBytes = input.Slice(reader.Position, (long)msgLen);
                        reader.Advance((long)msgLen);
                        break;

                    case PlSerIdTag: // int32 serializerId (wireType 0)
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint serId)) return;
                        result.SerializerId = (int)serId;
                        break;

                    case PlManifTag: // bytes messageManifest (wireType 2)
                        if (!ProtobufWire.TryReadVarint(ref reader, out uint manifLen)) return;
                        result.ManifestBytes = input.Slice(reader.Position, (long)manifLen);
                        reader.Advance((long)manifLen);
                        break;

                    default:
                        ProtobufWire.TrySkipField(ref reader, (byte)(rawTag & 0x7));
                        break;
                }
            }
        }

        /// <summary>
        /// Extracts the bytes of a single-field <c>string</c> sub-message like
        /// <c>ActorRefData { string path = 1; }</c> — returns the raw UTF-8 bytes
        /// as a zero-copy sequence. 🌸
        /// </summary>
        private static ReadOnlySequence<byte> ExtractStringField(
            ReadOnlySequence<byte> msgBody,
            byte expectedTag)
        {
            var r = new SequenceReader<byte>(msgBody);
            while (r.Remaining > 0)
            {
                if (!ProtobufWire.TryReadVarint(ref r, out uint rawTag)) break;
                var tag = (byte)(rawTag & 0xFF);
                if (tag == expectedTag)
                {
                    if (!ProtobufWire.TryReadVarint(ref r, out uint sLen)) break;
                    return msgBody.Slice(r.Position, (long)sLen);
                }
                // skip anything else
                ProtobufWire.TrySkipField(ref r, (byte)(rawTag & 0x7));
            }
            return ReadOnlySequence<byte>.Empty;
        }
    }
}

