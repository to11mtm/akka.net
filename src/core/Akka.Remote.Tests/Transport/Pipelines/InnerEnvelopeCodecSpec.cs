//-----------------------------------------------------------------------
// <copyright file="InnerEnvelopeCodecSpec.cs" company="Akka.NET Project">
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
using Akka.Remote.Transport;
using Akka.Remote.Transport.Pipelines.Codec;
using FluentAssertions;
using Google.Protobuf;
using Xunit;
using SerializedMessage = Akka.Remote.Serialization.Proto.Msg.Payload;

namespace Akka.Remote.Tests.Transport.Pipelines
{
    /// <summary>
    /// A2 tests: verifies that <see cref="InnerEnvelopeWriter"/> and
    /// <see cref="InnerEnvelopeReader"/> produce and consume bytes that are
    /// <b>byte-identical</b> to the generated <c>AckAndEnvelopeContainer</c>
    /// protobuf codec. uwu~ ??
    /// </summary>
    public class InnerEnvelopeCodecSpec
    {
        private const string RecipientPath = "akka.tcp://TestSystem@host1:2552/user/target";
        private const string SenderPath    = "akka.tcp://TestSystem@host2:2552/user/source";

        // -- WriteAckAndEnvelope: byte-identity vs. generated protobuf --------

        [Fact]
        public void WriteAckAndEnvelope_MinimalMessage_MatchesGeneratedCodec()
        {
            var msgBytes = Encoding.UTF8.GetBytes("hello world");

            var legacy = BuildLegacyAckAndEnvelope(
                recipientPath: RecipientPath,
                senderPath:    null,
                seq:           InnerEnvelopeWriter.SeqUndefined,
                ack:           null,
                msgBytes:      msgBytes,
                serializerId:  5,
                manifest:      Array.Empty<byte>()
            );

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WriteAckAndEnvelope(
                writer,
                recipientPath:  RecipientPath,
                senderPath:     null,
                seq:            InnerEnvelopeWriter.SeqUndefined,
                ack:            null,
                messageBytes:   msgBytes,
                serializerId:   5,
                manifestBytes:  Array.Empty<byte>()
            );

            writer.WrittenSpan.ToArray().Should().Equal(legacy,
                "InnerEnvelopeWriter output must be byte-identical to generated protobuf (minimal message)");
        }

        [Fact]
        public void WriteAckAndEnvelope_WithSenderAndAck_MatchesGeneratedCodec()
        {
            var msgBytes  = Encoding.UTF8.GetBytes("greetings from the uwu codec");
            var manifest  = Encoding.UTF8.GetBytes("SomeType, SomeAssembly");
            var ack       = new Ack(new SeqNo(42), new[] { new SeqNo(40), new SeqNo(41) });
            const ulong seq = 99UL;

            var legacy = BuildLegacyAckAndEnvelope(
                recipientPath: RecipientPath,
                senderPath:    SenderPath,
                seq:           seq,
                ack:           ack,
                msgBytes:      msgBytes,
                serializerId:  7,
                manifest:      manifest
            );

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WriteAckAndEnvelope(
                writer,
                recipientPath:  RecipientPath,
                senderPath:     SenderPath,
                seq:            seq,
                ack:            ack,
                messageBytes:   msgBytes,
                serializerId:   7,
                manifestBytes:  manifest
            );

            writer.WrittenSpan.ToArray().Should().Equal(legacy,
                "InnerEnvelopeWriter output must be byte-identical to generated protobuf (with sender + ack)");
        }

        [Fact]
        public void WritePureAck_MatchesGeneratedCodec()
        {
            var ack    = new Ack(new SeqNo(100), new[] { new SeqNo(98), new SeqNo(99) });
            var legacy = BuildLegacyPureAck(ack);

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WritePureAck(writer, ack);

            writer.WrittenSpan.ToArray().Should().Equal(legacy,
                "WritePureAck output must be byte-identical to generated protobuf");
        }

        [Fact]
        public void WriteAckAndEnvelope_EmptyMessage_MatchesGeneratedCodec()
        {
            var legacy = BuildLegacyAckAndEnvelope(
                recipientPath: RecipientPath,
                senderPath:    null,
                seq:           InnerEnvelopeWriter.SeqUndefined,
                ack:           null,
                msgBytes:      Array.Empty<byte>(),
                serializerId:  0,
                manifest:      Array.Empty<byte>()
            );

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WriteAckAndEnvelope(
                writer, RecipientPath, null, InnerEnvelopeWriter.SeqUndefined,
                null, Array.Empty<byte>(), 0, Array.Empty<byte>()
            );

            writer.WrittenSpan.ToArray().Should().Equal(legacy,
                "InnerEnvelopeWriter must match generated codec for empty message");
        }

        // -- WriteAckAndEnvelope: seq = 0 should be omitted from wire ---------

        [Fact]
        public void WriteAckAndEnvelope_SeqZero_OmittedFromWire()
        {
            // Proto3 default: seq = 0 is NOT written on the wire.
            var legacy = BuildLegacyAckAndEnvelope(
                RecipientPath, null, seq: 0UL, null,
                Encoding.UTF8.GetBytes("x"), 1, Array.Empty<byte>()
            );

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WriteAckAndEnvelope(
                writer, RecipientPath, null, 0UL, null,
                Encoding.UTF8.GetBytes("x"), 1, Array.Empty<byte>()
            );

            writer.WrittenSpan.ToArray().Should().Equal(legacy,
                "seq=0 is proto3 default and must be omitted from the wire");
        }

        // -- InnerEnvelopeReader: round-trip ----------------------------------

        [Fact]
        public void TryRead_RoundTrips_FullEnvelope()
        {
            var msgBytes = Encoding.UTF8.GetBytes("hello from zero-copy codec");
            var manifest = Encoding.UTF8.GetBytes("MyManifest");
            const ulong seq = 42UL;
            var ack = new Ack(new SeqNo(10), new[] { new SeqNo(8), new SeqNo(9) });

            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WriteAckAndEnvelope(
                writer, RecipientPath, SenderPath, seq, ack,
                msgBytes, 7, manifest
            );

            var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
            InnerEnvelopeReader.TryRead(input, out var result).Should().BeTrue();

            result.HasEnvelope.Should().BeTrue();
            result.HasAck.Should().BeTrue();
            result.Seq.Should().Be(seq);
            result.DecodeRecipientPath().Should().Be(RecipientPath);
            result.DecodeSenderPath().Should().Be(SenderPath);
            result.MessageBytes.ToArray().Should().Equal(msgBytes);
            result.SerializerId.Should().Be(7);
            result.DecodeManifest().Should().Be("MyManifest");
            result.CumulativeAck.Should().Be(10UL);

            var nacks = result.DecodeNacks();
            nacks.Should().BeEquivalentTo(new ulong[] { 8UL, 9UL });
        }

        [Fact]
        public void TryRead_RoundTrips_PureAck()
        {
            var ack    = new Ack(new SeqNo(55));
            var writer = new ArrayBufferWriter<byte>();
            InnerEnvelopeWriter.WritePureAck(writer, ack);

            var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
            InnerEnvelopeReader.TryRead(input, out var result).Should().BeTrue();

            result.HasAck.Should().BeTrue();
            result.HasEnvelope.Should().BeFalse();
            result.CumulativeAck.Should().Be(55UL);
        }

        // -- InnerEnvelopeReader: differential vs. generated parser -----------

        [Theory]
        [InlineData(42)]
        public void TryRead_DifferentialTest_MatchesGeneratedParser(int sampleCount)
        {
            var rng = new Random(unchecked((int)0xB00B_B00B));

            for (int i = 0; i < sampleCount; i++)
            {
                var msgBytes  = new byte[rng.Next(0, 256)];
                rng.NextBytes(msgBytes);
                var manifest = Encoding.UTF8.GetBytes($"Manifest_{rng.Next(100)}");
                var serId    = rng.Next(0, 20);
                var seq      = (ulong)rng.Next(0, 1000) == 0 ? InnerEnvelopeWriter.SeqUndefined : (ulong)rng.Next(1, 1000);
                Ack? ack     = rng.Next(2) == 0
                    ? new Ack(new SeqNo(rng.Next(1, 100)))
                    : null;

                // Build bytes via legacy codec
                var legacyBytes = BuildLegacyAckAndEnvelope(
                    RecipientPath, null, seq, ack, msgBytes, serId, manifest
                );

                // Parse via new reader
                var seq2 = new ReadOnlySequence<byte>(legacyBytes);
                InnerEnvelopeReader.TryRead(seq2, out var decoded).Should().BeTrue($"sample {i}");

                // Parse via legacy
                var legacyParsed = AckAndEnvelopeContainer.Parser.ParseFrom(legacyBytes);

                // Compare fields
                decoded.HasEnvelope.Should().Be(legacyParsed.Envelope != null, $"sample {i} HasEnvelope");
                decoded.DecodeRecipientPath().Should().Be(
                    legacyParsed.Envelope?.Recipient?.Path ?? string.Empty, $"sample {i} recipient");
                decoded.MessageBytes.ToArray().Should().Equal(
                    legacyParsed.Envelope?.Message?.Message.ToByteArray() ?? Array.Empty<byte>(),
                    $"sample {i} message bytes");
                decoded.SerializerId.Should().Be(
                    legacyParsed.Envelope?.Message?.SerializerId ?? 0, $"sample {i} serializerId");
            }
        }

        // -- Helpers ----------------------------------------------------------

        private static byte[] BuildLegacyAckAndEnvelope(
            string recipientPath,
            string? senderPath,
            ulong seq,
            Ack? ack,
            byte[] msgBytes,
            int serializerId,
            byte[] manifest)
        {
            var container = new AckAndEnvelopeContainer();
            var envelope  = new RemoteEnvelope
            {
                Recipient = new ActorRefData { Path = recipientPath },
                Message   = new SerializedMessage
                {
                    Message          = UnsafeByteOperations.UnsafeWrap(msgBytes),
                    SerializerId     = serializerId,
                    MessageManifest  = manifest.Length > 0
                        ? UnsafeByteOperations.UnsafeWrap(manifest)
                        : ByteString.Empty,
                },
                Seq = seq,
            };
            if (senderPath != null)
                envelope.Sender = new ActorRefData { Path = senderPath };

            container.Envelope = envelope;

            if (ack != null)
            {
                var ackInfo = new AcknowledgementInfo
                {
                    CumulativeAck = (ulong)ack.CumulativeAck.RawValue
                };
                ackInfo.Nacks.AddRange(ack.Nacks.Select(n => (ulong)n.RawValue));
                container.Ack = ackInfo;
            }

            return container.ToByteString().ToByteArray();
        }

        private static byte[] BuildLegacyPureAck(Ack ack)
        {
            var ackInfo = new AcknowledgementInfo
            {
                CumulativeAck = (ulong)ack.CumulativeAck.RawValue
            };
            ackInfo.Nacks.AddRange(ack.Nacks.Select(n => (ulong)n.RawValue));
            return new AckAndEnvelopeContainer { Ack = ackInfo }.ToByteString().ToByteArray();
        }
    }
}

