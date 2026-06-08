//-----------------------------------------------------------------------
// <copyright file="ProtobufWire.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Akka.Remote.Transport.Pipelines.Codec
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Low-level protobuf wire-format helpers — varint encode/decode and
    /// fixed-size integer write helpers — used by the hand-written
    /// zero-copy codec. 🌸
    ///
    /// <para>
    /// Protobuf wire types we care about:
    /// <list type="bullet">
    ///   <item>0 — Varint (<c>int32</c>, <c>uint32</c>, <c>int64</c>, <c>enum</c>)</item>
    ///   <item>1 — 64-bit (<c>fixed64</c>, <c>sfixed64</c>)</item>
    ///   <item>2 — Length-delimited (<c>bytes</c>, <c>string</c>, embedded messages)</item>
    ///   <item>5 — 32-bit (<c>fixed32</c>, <c>sfixed32</c>)</item>
    /// </list>
    /// </para>
    ///
    /// <!-- CopilotNotes: Tag byte = (field_number &lt;&lt; 3) | wire_type.
    ///      All field numbers in AkkaProtocolMessage / AckAndEnvelopeContainer / sub-messages
    ///      are small (≤ 5), so the tag byte is always a single byte (field_num &lt;&lt; 3 &lt; 128). -->
    /// </summary>
    internal static class ProtobufWire
    {
        // ── Wire type constants ──────────────────────────────────────────────
        /// <summary>Wire type 0: varint (int32, uint32, bool, enum).</summary>
        public const byte WireTypeVarint         = 0;
        /// <summary>Wire type 1: 64-bit (fixed64, sfixed64, double).</summary>
        public const byte WireType64Bit          = 1;
        /// <summary>Wire type 2: length-delimited (string, bytes, embedded messages).</summary>
        public const byte WireTypeLengthDelimited = 2;
        /// <summary>Wire type 5: 32-bit (fixed32, sfixed32, float).</summary>
        public const byte WireType32Bit          = 5;

        // ── Tag computation ──────────────────────────────────────────────────

        /// <summary>Computes the tag byte for a field with the given <paramref name="fieldNumber"/> and <paramref name="wireType"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte MakeTag(uint fieldNumber, byte wireType) =>
            (byte)((fieldNumber << 3) | wireType);

        // ── Varint encode ────────────────────────────────────────────────────

        /// <summary>
        /// Returns the number of bytes required to encode <paramref name="value"/> as a protobuf varint.
        /// For a <c>uint32</c>, this is in the range [1, 5].
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeVarintSize(uint value)
        {
            if (value < 0x80u)        return 1;
            if (value < 0x4000u)      return 2;
            if (value < 0x200000u)    return 3;
            if (value < 0x10000000u)  return 4;
            return 5;
        }

        /// <summary>
        /// Encodes <paramref name="value"/> as a protobuf varint into <paramref name="dest"/>,
        /// starting at offset 0. Returns the number of bytes written (1–5).
        /// The caller is responsible for ensuring <paramref name="dest"/> is large enough
        /// (use <see cref="ComputeVarintSize"/> to pre-check). 🌸
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int WriteVarint(Span<byte> dest, uint value)
        {
            int written = 0;
            while (value > 0x7Fu)
            {
                dest[written++] = (byte)((value & 0x7Fu) | 0x80u);
                value >>= 7;
            }
            dest[written++] = (byte)value;
            return written;
        }

        // ── Varint decode ────────────────────────────────────────────────────

        /// <summary>
        /// Attempts to decode a protobuf <c>uint32</c> varint from <paramref name="reader"/>.
        /// Advances the reader by the number of bytes consumed.
        /// Returns <c>false</c> if the buffer doesn't contain a complete varint.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadVarint(ref SequenceReader<byte> reader, out uint value)
        {
            value = 0;
            int shift = 0;

            while (reader.TryRead(out byte b))
            {
                value |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                    return true;

                shift += 7;
                if (shift >= 35)
                {
                    // Overflow — more than 5 bytes for a uint32; treat as corrupt.
                    value = 0;
                    return false;
                }
            }

            value = 0;
            return false;
        }

        // ── Skip helpers ─────────────────────────────────────────────────────

        /// <summary>
        /// Skips one field from <paramref name="reader"/> given its <paramref name="wireType"/>.
        /// Used when an unknown field is encountered during decoding.
        /// Returns <c>false</c> if the buffer is too short to skip the field.
        /// </summary>
        public static bool TrySkipField(ref SequenceReader<byte> reader, byte wireType)
        {
            switch (wireType)
            {
                case WireTypeVarint:
                    // Read and discard the varint value.
                    while (reader.TryRead(out byte b))
                    {
                        if ((b & 0x80) == 0) return true;
                    }
                    return false;

                case WireType64Bit:
                    if (reader.Remaining < 8) return false;
                    reader.Advance(8);
                    return true;

                case WireTypeLengthDelimited:
                    if (!TryReadVarint(ref reader, out uint len)) return false;
                    if (reader.Remaining < (long)len) return false;
                    reader.Advance((long)len);
                    return true;

                case WireType32Bit:
                    if (reader.Remaining < 4) return false;
                    reader.Advance(4);
                    return true;

                default:
                    // Unknown wire type — we cannot safely skip; bail out.
                    return false;
            }
        }

        // ── Fixed-size integer helpers ───────────────────────────────────────

        /// <summary>Reads a <c>fixed64</c> (little-endian <c>ulong</c>) from the reader. Returns <c>false</c> if fewer than 8 bytes remain.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryReadFixed64(ref SequenceReader<byte> reader, out ulong value)
        {
            if (reader.Remaining < 8)
            {
                value = 0;
                return false;
            }
            reader.TryReadLittleEndian(out long signed);
            value = (ulong)signed;
            return true;
        }

        /// <summary>Writes a <c>fixed64</c> (little-endian <c>ulong</c>) into <paramref name="dest"/> at offset 0. Dest must be ≥ 8 bytes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteFixed64(Span<byte> dest, ulong value)
        {
            dest[0] = (byte)(value);
            dest[1] = (byte)(value >> 8);
            dest[2] = (byte)(value >> 16);
            dest[3] = (byte)(value >> 24);
            dest[4] = (byte)(value >> 32);
            dest[5] = (byte)(value >> 40);
            dest[6] = (byte)(value >> 48);
            dest[7] = (byte)(value >> 56);
        }

        // ── Length-delimited write helper ────────────────────────────────────

        /// <summary>
        /// Writes a length-delimited field header (<paramref name="tag"/> + varint length) followed
        /// by the given <paramref name="body"/> bytes into <paramref name="writer"/>.
        /// This is the hot-path helper for the hand-written codec. uwu~ 🌸
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteLengthDelimited(IBufferWriter<byte> writer, byte tag, ReadOnlySpan<byte> body)
        {
            var lenSize = ComputeVarintSize((uint)body.Length);
            var total   = 1 + lenSize + body.Length;
            var span    = writer.GetSpan(total);
            span[0]     = tag;
            WriteVarint(span.Slice(1), (uint)body.Length);
            body.CopyTo(span.Slice(1 + lenSize));
            writer.Advance(total);
        }
    }
}


