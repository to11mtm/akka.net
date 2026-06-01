//-----------------------------------------------------------------------
// <copyright file="OuterEnvelopeCodecSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Akka.Remote.Serialization.Proto.Msg;
using Akka.Remote.Transport.Pipelines.Codec;
using FluentAssertions;
using Google.Protobuf;
using Xunit;

namespace Akka.Remote.Tests.Transport.Pipelines
{
    /// <summary>
    /// A1 tests: verifies that <see cref="OuterEnvelopeWriter"/> and
    /// <see cref="OuterEnvelopeReader"/> produce and consume bytes that are
    /// <b>byte-identical</b> to the generated <c>AkkaProtocolMessage</c> protobuf
    /// codec. ??
    /// </summary>
    public class OuterEnvelopeCodecSpec
    {
        // -- WritePayloadFrame: byte-identity vs. generated protobuf ----------

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(127)]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(65535)]
        public void WritePayloadFrame_MatchesGeneratedProtobuf(int bodyLength)
        {
            var body = GenerateBody(bodyLength);

            // Legacy: AkkaProtocolMessage{Payload=body}.ToByteString()
            var legacyBytes = new AkkaProtocolMessage
            {
                Payload = UnsafeByteOperations.UnsafeWrap(body)
            }.ToByteString().ToByteArray();

            // New: OuterEnvelopeWriter
            var writer = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WritePayloadFrame(writer, body);

            writer.WrittenSpan.ToArray().Should().Equal(legacyBytes,
                $"WritePayloadFrame output must be byte-identical to generated protobuf for body length {bodyLength}");
        }

        [Fact]
        public void WritePayloadFrame_KnownInput_ProducesCorrectBytes()
        {
            // body = { 0xDE, 0xAD, 0xBE, 0xEF }
            // Expected: tag(0x0A) + varint(4) + DE AD BE EF
            var body     = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
            var expected = new byte[] { 0x0A, 0x04, 0xDE, 0xAD, 0xBE, 0xEF };

            var writer = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WritePayloadFrame(writer, body);

            writer.WrittenSpan.ToArray().Should().Equal(expected);
        }

        // -- WriteControlFrame: byte-identity vs. generated protobuf ----------

        [Fact]
        public void WriteControlFrame_MatchesGeneratedProtobuf()
        {
            // Build a heartbeat control message body
            var controlMsg    = new AkkaControlMessage { CommandType = CommandType.Heartbeat };
            var controlBytes  = controlMsg.ToByteString().ToByteArray();

            // Legacy: AkkaProtocolMessage{Instruction=controlMsg}.ToByteString()
            var legacyBytes   = new AkkaProtocolMessage
            {
                Instruction = controlMsg
            }.ToByteString().ToByteArray();

            // New: WriteControlFrame wrapping the pre-encoded controlBytes
            var writer = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WriteControlFrame(writer, controlBytes);

            writer.WrittenSpan.ToArray().Should().Equal(legacyBytes,
                "WriteControlFrame output must be byte-identical to generated protobuf");
        }

        // -- ComputePayloadFrameSize ------------------------------------------

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(127)]
        [InlineData(128)]
        [InlineData(16383)]
        [InlineData(65536)]
        public void ComputePayloadFrameSize_MatchesActualWritten(int bodyLength)
        {
            var body   = GenerateBody(bodyLength);
            var writer = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WritePayloadFrame(writer, body);

            OuterEnvelopeWriter.ComputePayloadFrameSize(bodyLength)
                .Should().Be(writer.WrittenCount,
                    $"ComputePayloadFrameSize({bodyLength}) must equal bytes actually written");
        }

        // ── TryRead: round-trip (write then read) ────────────────────────────

        [Theory]
        [InlineData(1)]   // empty body (0) is excluded — proto3 omits it, nothing to round-trip
        [InlineData(127)]
        [InlineData(128)]
        [InlineData(1024)]
        public void TryRead_RoundTrips_PayloadFrame(int bodyLength)
        {
            var body   = GenerateBody(bodyLength);
            var writer = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WritePayloadFrame(writer, body);

            var seq = new ReadOnlySequence<byte>(writer.WrittenMemory);
            OuterEnvelopeReader.TryRead(ref seq, out var kind, out var readBody)
                .Should().BeTrue();

            kind.Should().Be(OuterFrameKind.Payload);
            readBody.ToArray().Should().Equal(body);
            seq.IsEmpty.Should().BeTrue("all input should have been consumed");
        }

        [Fact]
        public void TryRead_RoundTrips_ControlFrame()
        {
            var controlBody = new byte[] { 0x08, 0x03 }; // Heartbeat AkkaControlMessage
            var writer      = new ArrayBufferWriter<byte>();
            OuterEnvelopeWriter.WriteControlFrame(writer, controlBody);

            var seq = new ReadOnlySequence<byte>(writer.WrittenMemory);
            OuterEnvelopeReader.TryRead(ref seq, out var kind, out var readBody)
                .Should().BeTrue();

            kind.Should().Be(OuterFrameKind.Control);
            readBody.ToArray().Should().Equal(controlBody);
        }

        // -- TryRead: differential vs. AkkaProtocolMessage.Parser.ParseFrom ---

        [Theory]
        [InlineData(42)]
        public void TryRead_DifferentialTest_MatchesGeneratedParser(int sampleCount)
        {
            var rng = new Random(0xACE_ACE); // seeded for reproducibility ??

            for (int i = 0; i < sampleCount; i++)
            {
                // Build a random-body payload frame via the legacy codec
                var bodyLen  = rng.Next(0, 512);
                var body     = GenerateBody(bodyLen, rng);
                var legacyBs = new AkkaProtocolMessage
                {
                    Payload = UnsafeByteOperations.UnsafeWrap(body)
                }.ToByteString();

                // Parse via legacy
                var parsed = AkkaProtocolMessage.Parser.ParseFrom(legacyBs);
                var legacyPayload = parsed.Payload.ToByteArray();

                // Parse via new reader
                var seq = new ReadOnlySequence<byte>(legacyBs.ToByteArray());
                OuterEnvelopeReader.TryRead(ref seq, out var kind, out var newBody)
                    .Should().BeTrue($"sample {i}");

                kind.Should().Be(OuterFrameKind.Payload, $"sample {i}");
                newBody.ToArray().Should().Equal(legacyPayload, $"sample {i}");
            }
        }

        // -- TryRead: returns false on truncated input ------------------------

        [Fact]
        public void TryRead_EmptyBuffer_ReturnsFalse()
        {
            var seq = ReadOnlySequence<byte>.Empty;
            OuterEnvelopeReader.TryRead(ref seq, out _, out _).Should().BeFalse();
        }

        [Fact]
        public void TryRead_TruncatedBody_ReturnsFalse()
        {
            // tag(0x0A) + varint(100) ? promises 100 bytes but provides only 5
            var buf    = new byte[] { 0x0A, 0x64, 0x01, 0x02, 0x03, 0x04, 0x05 };
            var seq    = new ReadOnlySequence<byte>(buf);
            OuterEnvelopeReader.TryRead(ref seq, out _, out _).Should().BeFalse();
        }

        // -- TryRead: throws on unexpected tag -------------------------------

        [Fact]
        public void TryRead_UnexpectedTag_Throws()
        {
            var buf = new byte[] { 0x99, 0x01, 0x01 }; // unknown tag
            var seq = new ReadOnlySequence<byte>(buf);
            var act = () => { OuterEnvelopeReader.TryRead(ref seq, out _, out _); };
            act.Should().Throw<Akka.Remote.Transport.PduCodecException>().WithMessage("*unexpected tag*");
        }

        // -- Helpers ----------------------------------------------------------

        private static byte[] GenerateBody(int length, Random? rng = null)
        {
            var bytes = new byte[length];
            (rng ?? new Random(42)).NextBytes(bytes);
            return bytes;
        }
    }
}


