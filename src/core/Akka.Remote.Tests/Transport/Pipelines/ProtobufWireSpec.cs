//-----------------------------------------------------------------------
// <copyright file="ProtobufWireSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using Akka.Remote.Transport.Pipelines.Codec;
using FluentAssertions;
using Xunit;

namespace Akka.Remote.Tests.Transport.Pipelines
{
    /// <summary>
    /// Unit tests for the varint encode/decode helpers in <see cref="ProtobufWire"/>. ?
    ///
    /// These are the foundational building blocks for the hand-written codec —
    /// if these are wrong, every byte comparison further up will fail. uwu~ ??
    /// </summary>
    public class ProtobufWireSpec
    {
        // -- ComputeVarintSize ------------------------------------------------

        [Theory]
        [InlineData(0u,          1)]
        [InlineData(1u,          1)]
        [InlineData(127u,        1)]  // 2^7 - 1
        [InlineData(128u,        2)]  // 2^7
        [InlineData(16383u,      2)]  // 2^14 - 1
        [InlineData(16384u,      3)]  // 2^14
        [InlineData(2097151u,    3)]  // 2^21 - 1
        [InlineData(2097152u,    4)]  // 2^21
        [InlineData(268435455u,  4)]  // 2^28 - 1
        [InlineData(268435456u,  5)]  // 2^28
        [InlineData(4294967295u, 5)]  // uint.MaxValue
        public void ComputeVarintSize_ReturnsCorrectByteCount(uint value, int expected)
        {
            ProtobufWire.ComputeVarintSize(value).Should().Be(expected);
        }

        // -- WriteVarint / TryReadVarint round-trip ---------------------------

        [Theory]
        [InlineData(0u)]
        [InlineData(1u)]
        [InlineData(127u)]
        [InlineData(128u)]
        [InlineData(300u)]
        [InlineData(16383u)]
        [InlineData(16384u)]
        [InlineData(100_000u)]
        [InlineData(2097151u)]
        [InlineData(2097152u)]
        [InlineData(268435455u)]
        [InlineData(268435456u)]
        [InlineData(4294967295u)]  // uint.MaxValue
        public void WriteVarint_ThenTryReadVarint_RoundTrips(uint value)
        {
            Span<byte> buf = stackalloc byte[5];
            var written = ProtobufWire.WriteVarint(buf, value);
            written.Should().Be(ProtobufWire.ComputeVarintSize(value));

            var seq    = new ReadOnlySequence<byte>(buf.Slice(0, written).ToArray());
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TryReadVarint(ref reader, out var decoded).Should().BeTrue();
            decoded.Should().Be(value);
        }

        [Fact]
        public void TryReadVarint_EmptyBuffer_ReturnsFalse()
        {
            var seq    = ReadOnlySequence<byte>.Empty;
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TryReadVarint(ref reader, out _).Should().BeFalse();
        }

        [Fact]
        public void TryReadVarint_Truncated_ReturnsFalse()
        {
            // A continuation byte (high bit set) with nothing following it
            var seq    = new ReadOnlySequence<byte>(new byte[] { 0x80 });
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TryReadVarint(ref reader, out _).Should().BeFalse();
        }

        // -- WriteFixed64 / TryReadFixed64 ------------------------------------

        [Theory]
        [InlineData(0UL)]
        [InlineData(1UL)]
        [InlineData(0xDEADBEEFCAFEBABEUL)]
        [InlineData(ulong.MaxValue)]
        public void WriteFixed64_ThenTryReadFixed64_RoundTrips(ulong value)
        {
            Span<byte> buf = stackalloc byte[8];
            ProtobufWire.WriteFixed64(buf, value);

            var seq    = new ReadOnlySequence<byte>(buf.ToArray());
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TryReadFixed64(ref reader, out var decoded).Should().BeTrue();
            decoded.Should().Be(value);
        }

        // -- MakeTag ----------------------------------------------------------

        [Theory]
        [InlineData(1u, 2, 0x0A)]  // field 1, wireType 2 ? AkkaProtocolMessage.payload
        [InlineData(2u, 2, 0x12)]  // field 2, wireType 2 ? AkkaProtocolMessage.instruction
        [InlineData(1u, 1, 0x09)]  // field 1, wireType 1 ? AcknowledgementInfo.cumulativeAck
        [InlineData(5u, 1, 0x29)]  // field 5, wireType 1 ? RemoteEnvelope.seq
        [InlineData(2u, 0, 0x10)]  // field 2, wireType 0 ? Payload.serializerId
        public void MakeTag_ProducesCorrectByte(uint fieldNumber, byte wireType, byte expected)
        {
            ProtobufWire.MakeTag(fieldNumber, wireType).Should().Be(expected);
        }

        // -- TrySkipField -----------------------------------------------------

        [Fact]
        public void TrySkipField_Varint_SkipsCorrectly()
        {
            // Encode a varint of value 300 (2 bytes) + sentinel byte 0xFF
            Span<byte> buf = stackalloc byte[3];
            ProtobufWire.WriteVarint(buf, 300u);
            buf[2] = 0xFF; // sentinel after the field

            var seq    = new ReadOnlySequence<byte>(buf.ToArray());
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TrySkipField(ref reader, ProtobufWire.WireTypeVarint).Should().BeTrue();
            reader.Remaining.Should().Be(1); // sentinel still there
        }

        [Fact]
        public void TrySkipField_64Bit_SkipsCorrectly()
        {
            var data   = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xFF };
            var seq    = new ReadOnlySequence<byte>(data);
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TrySkipField(ref reader, ProtobufWire.WireType64Bit).Should().BeTrue();
            reader.Remaining.Should().Be(1);
        }

        [Fact]
        public void TrySkipField_LengthDelimited_SkipsCorrectly()
        {
            // varint(3) + "abc" + sentinel
            var data   = new byte[] { 0x03, 0x61, 0x62, 0x63, 0xFF };
            var seq    = new ReadOnlySequence<byte>(data);
            var reader = new SequenceReader<byte>(seq);
            ProtobufWire.TrySkipField(ref reader, ProtobufWire.WireTypeLengthDelimited).Should().BeTrue();
            reader.Remaining.Should().Be(1);
        }
    }
}

