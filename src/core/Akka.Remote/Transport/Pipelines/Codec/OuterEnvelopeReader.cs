//-----------------------------------------------------------------------
// <copyright file="OuterEnvelopeReader.cs" company="Akka.NET Project">
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
    /// Discriminates between the two shapes of an <c>AkkaProtocolMessage</c> frame. 🌸
    /// </summary>
    internal enum OuterFrameKind : byte
    {
        /// <summary>
        /// The frame contains a <c>bytes Payload</c> field (field 1, tag 0x0A).
        /// The body is an <c>AckAndEnvelopeContainer</c> on the hot-path.
        /// </summary>
        Payload = 1,

        /// <summary>
        /// The frame contains an <c>AkkaControlMessage Instruction</c> field (field 2, tag 0x12).
        /// The body is an <c>AkkaControlMessage</c> (handshake / heartbeat / disassociate).
        /// </summary>
        Control = 2,
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// Hand-written, zero-copy reader for the outer <c>AkkaProtocolMessage</c> envelope. ✨
    ///
    /// <para>
    /// Replaces <c>AkkaProtocolMessage.Parser.ParseFrom(raw)</c> (copy R2 in the analysis)
    /// with a direct tag + varint read that <b>slices the inner bytes out of the caller's
    /// <see cref="ReadOnlySequence{T}"/> without copying</b> — the body lives entirely within
    /// the <see cref="System.IO.Pipelines.PipeReader"/>-owned memory segment(s).
    /// </para>
    ///
    /// <para>
    /// Wire schema (from <c>WireFormats.proto</c>):
    /// <code>
    /// message AkkaProtocolMessage {
    ///   bytes payload             = 1;  // tag 0x0A
    ///   AkkaControlMessage instruction = 2;  // tag 0x12
    /// }
    /// </code>
    /// </para>
    ///
    /// <!-- CopilotNotes: The returned `body` is a zero-copy slice of the input sequence.
    ///      It aliases PipeReader-owned segments that will be recycled after AdvanceTo.
    ///      Callers MUST NOT hold onto the body past the current read-loop iteration
    ///      unless they copy it first. The inline state machine (PR-C) handles this by
    ///      either consuming synchronously or promoting to a RentedInboundPayload. 🌸 -->
    /// </summary>
    internal static class OuterEnvelopeReader
    {
        // ── Wire tags ────────────────────────────────────────────────────────
        //  field 1 (bytes payload)            = (1 << 3) | 2 = 0x0A
        //  field 2 (AkkaControlMessage inst.) = (2 << 3) | 2 = 0x12
        private const byte PayloadTag = 0x0A;
        private const byte ControlTag = 0x12;

        // ── Public API ───────────────────────────────────────────────────────

        /// <summary>
        /// Attempts to decode the outer <c>AkkaProtocolMessage</c> envelope from
        /// <paramref name="input"/>.
        ///
        /// <para>
        /// On success:
        /// <list type="bullet">
        ///   <item><paramref name="kind"/> indicates whether the frame is a <see cref="OuterFrameKind.Payload"/>
        ///     or <see cref="OuterFrameKind.Control"/>.</item>
        ///   <item><paramref name="body"/> is a <b>zero-copy</b> <see cref="ReadOnlySequence{T}"/>
        ///     slice that points into the same memory as <paramref name="input"/>.</item>
        ///   <item><paramref name="input"/> is advanced past the consumed bytes.</item>
        /// </list>
        /// </para>
        ///
        /// Returns <c>false</c> (and leaves <paramref name="input"/> unchanged) if:
        /// <list type="bullet">
        ///   <item>The buffer is too short to read a complete field.</item>
        ///   <item>An unexpected tag byte is encountered (throws <c>PduCodecException</c>).</item>
        /// </list>
        ///
        /// <!-- CopilotNotes: In the case of a malformed frame, this throws PduCodecException
        ///      to match the behavior of AkkaProtocolMessage.Parser.ParseFrom. The caller
        ///      (InlineProtocolState in PR-C) should catch and disassociate. 🌸 -->
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRead(
            ref ReadOnlySequence<byte> input,
            out OuterFrameKind kind,
            out ReadOnlySequence<byte> body)
        {
            var reader = new SequenceReader<byte>(input);

            // ── Tag byte ─────────────────────────────────────────────────────
            if (!reader.TryRead(out byte tag))
                goto fail;

            kind = tag switch
            {
                PayloadTag => OuterFrameKind.Payload,
                ControlTag => OuterFrameKind.Control,
                _ => throw new PduCodecException(
                    $"OuterEnvelopeReader: unexpected tag 0x{tag:X2}; expected 0x0A (Payload) or 0x12 (Control).")
            };

            // ── Length varint ─────────────────────────────────────────────────
            if (!ProtobufWire.TryReadVarint(ref reader, out uint len))
                goto fail;

            // ── Body slice (zero-copy) ────────────────────────────────────────
            if (reader.Remaining < len)
                goto fail;

            body  = input.Slice(reader.Position, (long)len);
            input = input.Slice(reader.Position).Slice((long)len);
            return true;

        fail:
            kind = default;
            body = default;
            return false;
        }
    }
}


