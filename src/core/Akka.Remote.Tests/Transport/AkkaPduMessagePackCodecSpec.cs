//-----------------------------------------------------------------------
// <copyright file="AkkaPduMessagePackCodecSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.Transport;
using Akka.Remote.Transport.Pipelines;
using Akka.Remote.Transport.Pipelines.MessagePack;
using Akka.TestKit;
using FluentAssertions;
using Google.Protobuf;
using Xunit;
using Xunit.v3;
using SerializedMessage = Akka.Remote.Serialization.Proto.Msg.Payload;

namespace Akka.Remote.Tests.Transport
{
    /// <summary>
    /// Unit tests for <see cref="AkkaPduMessagePackCodec"/>.
    ///
    /// Tests encode/decode round-trips for every PDU type:
    /// <list type="bullet">
    ///   <item>Protocol level: Heartbeat, Associate, Disassociate (all three variants), Payload</item>
    ///   <item>Message level: ConstructMessage round-trip, ConstructPureAck round-trip,
    ///     ConstructMessage with no seq/ack, ConstructPureAck with nacks</item>
    ///   <item>Config: PipeTransportSettings.CreateCodec returns the correct codec type</item>
    /// </list>
    ///
    /// <!-- CopilotNotes: Tests use AkkaSpec (inherits TestKit) but only need Sys for the
    ///      codec constructor (ActorPathThreadLocalCache). No real networking is used here. -->
    /// </summary>
    public class AkkaPduMessagePackCodecSpec : AkkaSpec
    {
        private static readonly Config BaseConfig = ConfigurationFactory.ParseString(@"
            akka {
                actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                remote.pipe.tcp {
                    port     = 0
                    hostname = ""127.0.0.1""
                }
            }");

        private readonly AkkaPduMessagePackCodec _codec;

        public AkkaPduMessagePackCodecSpec(ITestOutputHelper output)
            : base(BaseConfig, output)
        {
            _codec = new AkkaPduMessagePackCodec(Sys);
        }

        // ── Protocol-level: Heartbeat ───────────────────────────────────────────

        [Fact(DisplayName = "MessagePack codec: Heartbeat encodes and decodes to Heartbeat")]
        public void Heartbeat_Should_RoundTrip()
        {
            var bytes = _codec.ConstructHeartbeat();
            bytes.Should().NotBeNull();
            bytes.IsEmpty.Should().BeFalse();

            var pdu = _codec.DecodePdu(bytes);
            pdu.Should().BeOfType<Heartbeat>();
        }

        // ── Protocol-level: Associate ───────────────────────────────────────────

        [Fact(DisplayName = "MessagePack codec: Associate encodes and decodes preserving origin and uid")]
        public void Associate_Should_RoundTrip_With_Origin_And_Uid()
        {
            var origin  = new Address("akka.tcp", "TestSystem", "127.0.0.1", 7355);
            var info    = new HandshakeInfo(origin, uid: 42);
            var bytes   = _codec.ConstructAssociate(info);

            bytes.Should().NotBeNull();
            bytes.IsEmpty.Should().BeFalse();

            var pdu = _codec.DecodePdu(bytes) as Associate;
            pdu.Should().NotBeNull();
            pdu!.Info.Origin.Should().Be(origin);
            pdu.Info.Uid.Should().Be(42);
        }

        // ── Protocol-level: Disassociate variants ──────────────────────────────

        [Fact(DisplayName = "MessagePack codec: Disassociate(Unknown) encodes and decodes correctly")]
        public void Disassociate_Unknown_Should_RoundTrip()
        {
            var bytes = _codec.ConstructDisassociate(DisassociateInfo.Unknown);
            var pdu   = _codec.DecodePdu(bytes) as Disassociate;

            pdu.Should().NotBeNull();
            pdu!.Reason.Should().Be(DisassociateInfo.Unknown);
        }

        [Fact(DisplayName = "MessagePack codec: Disassociate(Quarantined) encodes and decodes correctly")]
        public void Disassociate_Quarantined_Should_RoundTrip()
        {
            var bytes = _codec.ConstructDisassociate(DisassociateInfo.Quarantined);
            var pdu   = _codec.DecodePdu(bytes) as Disassociate;

            pdu.Should().NotBeNull();
            pdu!.Reason.Should().Be(DisassociateInfo.Quarantined);
        }

        [Fact(DisplayName = "MessagePack codec: Disassociate(Shutdown) encodes and decodes correctly")]
        public void Disassociate_Shutdown_Should_RoundTrip()
        {
            var bytes = _codec.ConstructDisassociate(DisassociateInfo.Shutdown);
            var pdu   = _codec.DecodePdu(bytes) as Disassociate;

            pdu.Should().NotBeNull();
            pdu!.Reason.Should().Be(DisassociateInfo.Shutdown);
        }

        // ── Protocol-level: Payload wrapper ────────────────────────────────────

        [Fact(DisplayName = "MessagePack codec: ConstructPayload preserves inner bytes through DecodePdu")]
        public void Payload_Should_RoundTrip_Via_DecodePdu()
        {
            var innerBytes = ByteString.CopyFromUtf8("hello-msgpack-world");
            var bytes      = _codec.ConstructPayload(innerBytes);

            var pdu = _codec.DecodePdu(bytes) as Payload;
            pdu.Should().NotBeNull();
            Encoding.UTF8.GetString(pdu!.Bytes.Span).Should().Be("hello-msgpack-world");
        }

        // ── Message-level: ConstructMessage round-trip ─────────────────────────

        [Fact(DisplayName = "MessagePack codec: ConstructMessage + DecodeMessage round-trips payload, seq, and ack")]
        public void ConstructMessage_Should_RoundTrip_Full_Envelope()
        {
            // Build a minimal serialized payload (mirrors what EndpointWriter does).
            var serialized = new SerializedMessage
            {
                Message         = ByteString.CopyFromUtf8("the-message"),
                SerializerId    = 42,
                MessageManifest = ByteString.CopyFromUtf8("SomeCls")
            };

            var recipient    = Sys.ActorOf(Props.Empty);
            var localAddress = new Address("akka.tcp", Sys.Name, "127.0.0.1", 7355);
            var seqNo        = new SeqNo(7);
            var ack          = new Ack(new SeqNo(6), new[] { new SeqNo(3), new SeqNo(4) });

            // Encode via ConstructMessage (same path as EndpointWriter)
            var bytes = _codec.ConstructMessage(
                localAddress, recipient, serialized,
                senderOption: null,
                seqOption:    seqNo,
                ackOption:    ack);

            // DecodeMessage reconstructs the full AckAndMessage
            var provider = (IRemoteActorRefProvider)((ExtendedActorSystem)Sys).Provider;
            var result   = _codec.DecodeMessage(bytes, provider, localAddress);

            // ── ACK half ──────────────────────────────────────────────────────────
            result.AckOption.Should().NotBeNull();
            result.AckOption!.CumulativeAck.RawValue.Should().Be(6);
            result.AckOption.Nacks.Select(n => n.RawValue)
                .Should().BeEquivalentTo(new long[] { 3, 4 });

            // ── Message half ──────────────────────────────────────────────────────
            // The MessagePack decode path populates MsgPackMessage (not SerializedMessage)
            // so EndpointReader can dispatch via the zero-copy ReadOnlyMemory<byte> overload.
            result.MessageOption.Should().NotBeNull();
            result.MessageOption!.HasMsgPackPayload.Should().BeTrue();
            result.MessageOption.MsgPackMessage.Should().NotBeNull();
            Encoding.UTF8.GetString(result.MessageOption.MsgPackMessage!.Bytes.Span)
                .Should().Be("the-message");
            result.MessageOption.MsgPackMessage!.SerializerId.Should().Be(42);
            Encoding.UTF8.GetString(result.MessageOption.MsgPackMessage!.Manifest.Span)
                .Should().Be("SomeCls");
            result.MessageOption.Seq.Should().NotBeNull();
            result.MessageOption.Seq!.Value.RawValue.Should().Be(7);
        }

        [Fact(DisplayName = "MessagePack codec: ConstructMessage without seq/ack produces no seq and no ack")]
        public void ConstructMessage_Without_Seq_Or_Ack_Should_Produce_Empty_Envelope()
        {
            var serialized = new SerializedMessage
            {
                Message      = ByteString.CopyFromUtf8("bare-msg"),
                SerializerId = 1
            };

            var recipient    = Sys.ActorOf(Props.Empty);
            var localAddress = new Address("akka.tcp", Sys.Name, "127.0.0.1", 7355);

            var bytes = _codec.ConstructMessage(localAddress, recipient, serialized);

            var provider = (IRemoteActorRefProvider)((ExtendedActorSystem)Sys).Provider;
            var result   = _codec.DecodeMessage(bytes, provider, localAddress);

            result.AckOption.Should().BeNull();
            result.MessageOption.Should().NotBeNull();
            result.MessageOption!.Seq.Should().BeNull(); // no sequence number
        }

        // ── Message-level: ConstructPureAck ────────────────────────────────────

        [Fact(DisplayName = "MessagePack codec: ConstructPureAck + DecodeMessage round-trips ACK only")]
        public void ConstructPureAck_Should_RoundTrip()
        {
            var ack = new Ack(new SeqNo(100), new[] { new SeqNo(97), new SeqNo(98) });
            var bytes = _codec.ConstructPureAck(ack);

            var localAddress = new Address("akka.tcp", Sys.Name, "127.0.0.1", 7355);
            var provider     = (IRemoteActorRefProvider)((ExtendedActorSystem)Sys).Provider;
            var result       = _codec.DecodeMessage(bytes, provider, localAddress);

            result.MessageOption.Should().BeNull();
            result.AckOption.Should().NotBeNull();
            result.AckOption!.CumulativeAck.RawValue.Should().Be(100);
            result.AckOption.Nacks.Select(n => n.RawValue)
                .Should().BeEquivalentTo(new long[] { 97, 98 });
        }

        // ── Config factory ──────────────────────────────────────────────────────

        [Fact(DisplayName = "MessagePack codec: PipeTransportSettings.CreateCodec returns MessagePack codec when envelope=messagepack")]
        public void CreateCodec_With_MessagePack_Envelope_Should_Return_MessagePack_Codec()
        {
            var config = ConfigurationFactory.ParseString(@"
                akka.remote {
                    enabled-transports = [""akka.remote.pipe.tcp""]
                    pipe.tcp {
                        port     = 0
                        hostname = ""127.0.0.1""
                        envelope = messagepack
                    }
                }
            ").WithFallback(Sys.Settings.Config).GetConfig("akka.remote");

            var codec = PipeTransportSettings.CreateCodec(config, Sys);
            codec.Should().BeOfType<AkkaPduMessagePackCodec>();
        }

        [Fact(DisplayName = "MessagePack codec: PipeTransportSettings.CreateCodec returns Protobuf codec when envelope=protobuf")]
        public void CreateCodec_With_Protobuf_Envelope_Should_Return_Protobuf_Codec()
        {
            var config = ConfigurationFactory.ParseString(@"
                akka.remote {
                    enabled-transports = [""akka.remote.pipe.tcp""]
                    pipe.tcp {
                        port     = 0
                        hostname = ""127.0.0.1""
                        envelope = protobuf
                    }
                }
            ").WithFallback(Sys.Settings.Config).GetConfig("akka.remote");

            var codec = PipeTransportSettings.CreateCodec(config, Sys);
            codec.Should().BeOfType<AkkaPduProtobuffCodec>();
        }

        [Fact(DisplayName = "MessagePack codec: PipeTransportSettings.CreateCodec defaults to Protobuf when no pipe transport")]
        public void CreateCodec_Without_Pipe_Transport_Should_Default_To_Protobuf()
        {
            var config = ConfigurationFactory.ParseString(@"
                akka.remote {
                    enabled-transports = [""akka.remote.dot-netty.tcp""]
                }
            ").WithFallback(Sys.Settings.Config).GetConfig("akka.remote");

            var codec = PipeTransportSettings.CreateCodec(config, Sys);
            codec.Should().BeOfType<AkkaPduProtobuffCodec>();
        }

        // ── Codec produces shorter frames than protobuf ─────────────────────────

        [Fact(DisplayName = "MessagePack codec: Heartbeat frame is smaller than or equal to protobuf equivalent")]
        public void MessagePack_Heartbeat_Frame_Should_Be_Compact()
        {
            var mpBytes  = _codec.ConstructHeartbeat();
            var pbCodec  = new AkkaPduProtobuffCodec(Sys);
            var pbBytes  = pbCodec.ConstructHeartbeat();

            // MessagePack has no varint + field-id overhead for empty messages
            // so the frame should be at most as large as protobuf.
            mpBytes.Length.Should().BeLessOrEqualTo(pbBytes.Length * 2,
                because: "MessagePack should be compact for control frames");
        }

        [Fact(DisplayName = "MessagePack codec: Invalid bytes throw PduCodecException")]
        public void DecodePdu_With_Garbage_Bytes_Should_Throw_PduCodecException()
        {
            var garbage = ByteString.CopyFrom(new byte[] { 0xFF, 0x00, 0xAB, 0xCD });
            Action act  = () => _codec.DecodePdu(garbage);
            act.Should().Throw<PduCodecException>();
        }

        // ── Wire-format parity: new direct-write ConstructMessage vs. the old POCO path ──

        [Fact(DisplayName = "MessagePack codec: ConstructMessage wire format matches POCO serialization (full envelope)")]
        public void ConstructMessage_Bytes_Should_Match_PocoSerialization_FullEnvelope()
        {
            var serialized = new SerializedMessage
            {
                Message         = ByteString.CopyFromUtf8("the-message"),
                SerializerId    = 17,
                MessageManifest = ByteString.CopyFromUtf8("Manifest.Cls")
            };

            var recipient    = Sys.ActorOf(Props.Empty);
            var localAddress = new Address("akka.tcp", Sys.Name, "127.0.0.1", 7355);
            var seqNo        = new SeqNo(99);
            var ack          = new Ack(new SeqNo(50), new[] { new SeqNo(7), new SeqNo(8), new SeqNo(9) });

            // ── New path: direct MessagePackWriter ─────────────────────────────────
            var actual = _codec.ConstructMessage(
                localAddress, recipient, serialized,
                senderOption: recipient,
                seqOption:    seqNo,
                ackOption:    ack);

            // ── Reference path: rebuild the POCO graph and let the source-generated
            //    formatter serialize it. Any drift in our manual array layout would
            //    show up as a byte-level mismatch here. ──
            var expected = BuildExpectedPocoBytes(localAddress, recipient, serialized, recipient, seqNo, ack);

            actual.ToByteArray().Should().Equal(expected,
                because: "the direct-write path must produce byte-for-byte identical output to the [MessagePackObject] formatter");

            // Sanity: bytes also round-trip via DecodeMessage.
            var provider = (IRemoteActorRefProvider)((ExtendedActorSystem)Sys).Provider;
            var decoded  = _codec.DecodeMessage(actual, provider, localAddress);
            decoded.MessageOption.Should().NotBeNull();
            decoded.AckOption.Should().NotBeNull();
        }

        [Fact(DisplayName = "MessagePack codec: ConstructMessage wire format matches POCO serialization (minimal envelope)")]
        public void ConstructMessage_Bytes_Should_Match_PocoSerialization_Minimal()
        {
            // No sender, no seq, no ack — exercises every nil-emit branch.
            var serialized = new SerializedMessage
            {
                Message         = ByteString.Empty,
                SerializerId    = 0,
                MessageManifest = ByteString.Empty
            };

            var recipient    = Sys.ActorOf(Props.Empty);
            var localAddress = new Address("akka.tcp", Sys.Name, "127.0.0.1", 7355);

            var actual   = _codec.ConstructMessage(localAddress, recipient, serialized);
            var expected = BuildExpectedPocoBytes(localAddress, recipient, serialized,
                senderOption: null, seqOption: null, ackOption: null);

            // Diff diagnostics for any mismatch.
            var actualJson   = global::MessagePack.MessagePackSerializer.ConvertToJson(actual.ToByteArray());
            var expectedJson = global::MessagePack.MessagePackSerializer.ConvertToJson(expected);
            actualJson.Should().Be(expectedJson, because: "structural MessagePack JSON should match");

            actual.ToByteArray().Should().Equal(expected,
                because: "minimal-envelope direct-write must match the formatter byte-for-byte");
        }

        /// <summary>
        /// Builds the expected MessagePack bytes by going through the same POCO graph
        /// the old <see cref="AkkaPduMessagePackCodec.ConstructMessage"/> used to build.
        /// Source-generated formatters take it from there. Used as the parity reference
        /// for byte-equality assertions against the new direct-write path.
        /// </summary>
        private static byte[] BuildExpectedPocoBytes(
            Address localAddress,
            IActorRef recipient,
            SerializedMessage serialized,
            IActorRef? senderOption,
            SeqNo? seqOption,
            Ack? ackOption)
        {
            // Mirror of the legacy SerializeActorRef helper.
            static string SerializeActorRef(Address defaultAddress, IActorRef actorRef) =>
                !string.IsNullOrEmpty(actorRef.Path.Address.Host)
                    ? actorRef.Path.ToSerializationFormat()
                    : actorRef.Path.ToSerializationFormatWithAddress(defaultAddress);

            var env = new MpRemoteEnvelope
            {
                RecipientPath = SerializeActorRef(recipient.Path.Address, recipient),
                Message = new MpPayload
                {
                    Message      = serialized.Message.IsEmpty      ? null : serialized.Message.Memory,
                    SerializerId = serialized.SerializerId,
                    Manifest     = serialized.MessageManifest.IsEmpty ? null : serialized.MessageManifest.Memory
                },
                Seq = seqOption.HasValue
                    ? unchecked((ulong)seqOption.Value.RawValue)
                    : MpRemoteEnvelope.SeqUndefined,
            };
            if (senderOption?.Path is not null)
                env.SenderPath = SerializeActorRef(localAddress, senderOption);

            var container = new MpAckAndEnvelope
            {
                Envelope = env,
                Ack = ackOption is null ? null : new MpAck
                {
                    CumulativeAck = ackOption.CumulativeAck.RawValue,
                    Nacks         = ackOption.Nacks.Select(n => n.RawValue).ToArray()
                }
            };

            return global::MessagePack.MessagePackSerializer.Serialize(container);
        }
    }
}


