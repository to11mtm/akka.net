//-----------------------------------------------------------------------
// <copyright file="OuterEnvelopeWriter.cs" company="Akka.NET Project">
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
    /// Hand-written, zero-allocation writer for the outer
    /// <c>AkkaProtocolMessage</c> protobuf envelope. ✨
    ///
    /// <para>
    /// Replaces <c>new AkkaProtocolMessage { Payload = payload }.ToByteString()</c>
    /// (copy W5 in the analysis) with a direct tag + varint + body write into a
    /// caller-supplied <see cref="IBufferWriter{T}"/>. The output bytes are
    /// <b>byte-identical</b> to those produced by the generated protobuf code.
    /// </para>
    ///
    /// <para>
    /// Wire schema (from <c>WireFormats.proto</c>):
    /// <code>
    /// message AkkaProtocolMessage {
    ///   bytes payload     = 1;  // tag 0x0A  ← hot path
    ///   AkkaControlMessage instruction = 2;  // tag 0x12
    /// }
    /// </code>
    /// </para>
    ///
    /// <!-- CopilotNotes: Every outbound user message goes through WritePayloadFrame.
    ///      WriteControlFrame is infrastructure for PR-C (inline state machine) — control frames
    ///      are rare (handshake / heartbeat / disassociate) and still use the legacy codec in PR-A.
    ///      DO NOT add any allocation here; the whole point is zero-alloc on the hot path. 🌸 -->
    /// </summary>
    internal static class OuterEnvelopeWriter
    {
        // ── Wire tags ────────────────────────────────────────────────────────
        //  field 1 (bytes payload)       = (1 << 3) | 2 = 0x0A
        //  field 2 (AkkaControlMessage)  = (2 << 3) | 2 = 0x12
        private const byte PayloadTag = 0x0A;
        private const byte ControlTag = 0x12;

        // ── Public API ───────────────────────────────────────────────────────

        /// <summary>
        /// Writes an <c>AkkaProtocolMessage { Payload = body }</c> directly into
        /// <paramref name="writer"/>.
        ///
        /// <para>
        /// The output is byte-identical to
        /// <c>new AkkaProtocolMessage { Payload = UnsafeByteOperations.UnsafeWrap(body.ToArray()) }.ToByteString()</c>
        /// from the generated protobuf code. 🌸
        /// </para>
        /// </summary>
        /// <param name="writer">The destination buffer writer. Must have capacity for
        ///   at most <c>1 + 5 + body.Length</c> bytes.</param>
        /// <param name="body">The inner payload bytes (typically an <c>AckAndEnvelopeContainer</c>).
        ///   If empty, nothing is written (proto3 omits default empty bytes fields). 🌸</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WritePayloadFrame(IBufferWriter<byte> writer, ReadOnlySpan<byte> body)
        {
            // Proto3: empty bytes fields are omitted (default value). In practice the body is
            // always non-empty (it's an AckAndEnvelopeContainer) but we match the spec for
            // wire-compatibility with the generated codec. 🌸
            if (body.IsEmpty) return;
            ProtobufWire.WriteLengthDelimited(writer, PayloadTag, body);
        }

        /// <summary>
        /// Writes an <c>AkkaProtocolMessage { Instruction = controlBody }</c> directly into
        /// <paramref name="writer"/>.
        ///
        /// <para>
        /// The <paramref name="controlBody"/> should be the serialized <c>AkkaControlMessage</c>
        /// bytes (not yet wrapped in <c>AkkaProtocolMessage</c>).
        /// Output is byte-identical to the generated
        /// <c>new AkkaProtocolMessage { Instruction = msg }.ToByteString()</c>. 🌸
        /// </para>
        /// </summary>
        /// <param name="writer">The destination buffer writer.</param>
        /// <param name="controlBody">The serialized <c>AkkaControlMessage</c> bytes.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteControlFrame(IBufferWriter<byte> writer, ReadOnlySpan<byte> controlBody)
        {
            // Protobuf: tag(0x12) + varint(controlBody.Length) + controlBody
            ProtobufWire.WriteLengthDelimited(writer, ControlTag, controlBody);
        }

        /// <summary>
        /// Computes the exact number of bytes that
        /// <see cref="WritePayloadFrame"/> will write for a payload of length
        /// <paramref name="bodyLength"/>. Useful for pre-renting a pooled buffer.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputePayloadFrameSize(int bodyLength)
        {
            // Proto3: empty bytes field is omitted.
            if (bodyLength == 0) return 0;
            return 1 + ProtobufWire.ComputeVarintSize((uint)bodyLength) + bodyLength;
        }

        /// <summary>
        /// Computes the exact number of bytes that
        /// <see cref="WriteControlFrame"/> will write for a control body of length
        /// <paramref name="controlBodyLength"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeControlFrameSize(int controlBodyLength)
        {
            return 1 + ProtobufWire.ComputeVarintSize((uint)controlBodyLength) + controlBodyLength;
        }
    }
}



