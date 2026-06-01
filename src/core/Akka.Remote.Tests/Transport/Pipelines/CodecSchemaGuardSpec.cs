//-----------------------------------------------------------------------
// <copyright file="CodecSchemaGuardSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using Akka.Remote.Serialization.Proto.Msg;
using Akka.Remote.Transport.Pipelines.Codec;
using FluentAssertions;
using Google.Protobuf.Reflection;
using Xunit;

namespace Akka.Remote.Tests.Transport.Pipelines
{
    /// <summary>
    /// A5 — <b>Schema guard</b>: reflects over the generated protobuf descriptors and
    /// asserts that the field numbers / wire types hard-coded in the hand-written codec
    /// (<see cref="ProtobufWire"/>, <see cref="OuterEnvelopeWriter"/>,
    /// <see cref="OuterEnvelopeReader"/>, <see cref="InnerEnvelopeWriter"/>,
    /// <see cref="InnerEnvelopeReader"/>) match the <c>.proto</c> definitions in
    /// <c>WireFormats.proto</c> and <c>ContainerFormats.proto</c>. ??
    ///
    /// <para>
    /// If this test fails after a protobuf schema change, update the tag byte constants
    /// in the codec classes to match. The test message tells you which field drifted.
    /// </para>
    ///
    /// <!-- CopilotNotes: This is Risk #1 from the redesign doc — the hand-written codec
    ///      can silently produce wrong bytes if the .proto field numbers change. This test
    ///      is the catch. It runs on every CI build regardless of the zero-copy-codec flag. -->
    /// </summary>
    public class CodecSchemaGuardSpec
    {
        // -- Expected tag bytes ------------------------------------------------
        // These constants mirror those in the codec source files.
        // If this test starts failing, update the codec constants to match.

        // AkkaProtocolMessage
        private const byte APM_PayloadTag     = 0x0A;  // field 1 (bytes), wireType 2
        private const byte APM_InstructionTag = 0x12;  // field 2 (message), wireType 2

        // AckAndEnvelopeContainer
        private const byte AAE_AckTag         = 0x0A;  // field 1 (message AcknowledgementInfo), wireType 2
        private const byte AAE_EnvelopeTag    = 0x12;  // field 2 (message RemoteEnvelope), wireType 2

        // RemoteEnvelope
        private const byte RE_RecipientTag    = 0x0A;  // field 1 (message ActorRefData), wireType 2
        private const byte RE_MessageTag      = 0x12;  // field 2 (message Payload), wireType 2
        private const byte RE_SenderTag       = 0x22;  // field 4 (message ActorRefData), wireType 2
        private const byte RE_SeqTag          = 0x29;  // field 5 (fixed64), wireType 1

        // AcknowledgementInfo
        private const byte AI_CumAckTag       = 0x09;  // field 1 (fixed64), wireType 1
        private const byte AI_NacksTag        = 0x12;  // field 2 (packed fixed64), wireType 2

        // ActorRefData
        private const byte ARD_PathTag        = 0x0A;  // field 1 (string), wireType 2

        // Payload / SerializedMessage
        private const byte PL_MessageTag      = 0x0A;  // field 1 (bytes), wireType 2
        private const byte PL_SerializerIdTag = 0x10;  // field 2 (int32), wireType 0
        private const byte PL_ManifestTag     = 0x1A;  // field 3 (bytes), wireType 2

        // -- AkkaProtocolMessage -----------------------------------------------

        [Fact]
        public void AkkaProtocolMessage_PayloadField_IsField1_LengthDelimited()
        {
            var field = AkkaProtocolMessage.Descriptor.FindFieldByName("payload");
            field.Should().NotBeNull("AkkaProtocolMessage must have a 'payload' field. Update OuterEnvelopeWriter.cs if the schema changed.");
            field!.FieldNumber.Should().Be(1);
            field.FieldType.Should().Be(FieldType.Bytes);

            var expectedTag = ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited);
            expectedTag.Should().Be(APM_PayloadTag,
                "OuterEnvelopeWriter/Reader PayloadTag constant must match the proto field. Edit OuterEnvelopeWriter.cs to use 0x{0:X2}.", expectedTag);
        }

        [Fact]
        public void AkkaProtocolMessage_InstructionField_IsField2_LengthDelimited()
        {
            var field = AkkaProtocolMessage.Descriptor.FindFieldByName("instruction");
            field.Should().NotBeNull("AkkaProtocolMessage must have an 'instruction' field. Update OuterEnvelopeWriter.cs if the schema changed.");
            field!.FieldNumber.Should().Be(2);
            field.FieldType.Should().Be(FieldType.Message);

            var expectedTag = ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited);
            expectedTag.Should().Be(APM_InstructionTag,
                "OuterEnvelopeWriter/Reader ControlTag constant must match the proto field. Edit OuterEnvelopeWriter.cs to use 0x{0:X2}.", expectedTag);
        }

        // -- AckAndEnvelopeContainer -------------------------------------------

        [Fact]
        public void AckAndEnvelopeContainer_AckField_IsField1_LengthDelimited()
        {
            var field = AckAndEnvelopeContainer.Descriptor.FindFieldByName("ack");
            field.Should().NotBeNull("AckAndEnvelopeContainer must have an 'ack' field. Update InnerEnvelopeWriter.cs if the schema changed.");
            field!.FieldNumber.Should().Be(1);
            field.FieldType.Should().Be(FieldType.Message);

            var expectedTag = ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited);
            expectedTag.Should().Be(AAE_AckTag,
                "InnerEnvelopeWriter/Reader AackTag constant must match the proto field. Edit InnerEnvelopeWriter.cs to use 0x{0:X2}.", expectedTag);
        }

        [Fact]
        public void AckAndEnvelopeContainer_EnvelopeField_IsField2_LengthDelimited()
        {
            var field = AckAndEnvelopeContainer.Descriptor.FindFieldByName("envelope");
            field.Should().NotBeNull("AckAndEnvelopeContainer must have an 'envelope' field. Update InnerEnvelopeWriter.cs if the schema changed.");
            field!.FieldNumber.Should().Be(2);
            field.FieldType.Should().Be(FieldType.Message);

            var expectedTag = ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited);
            expectedTag.Should().Be(AAE_EnvelopeTag,
                "InnerEnvelopeWriter/Reader AenvTag constant must match the proto field. Edit InnerEnvelopeWriter.cs to use 0x{0:X2}.", expectedTag);
        }

        // -- RemoteEnvelope ---------------------------------------------------

        [Fact]
        public void RemoteEnvelope_RecipientField_IsField1_LengthDelimited()
        {
            var field = RemoteEnvelope.Descriptor.FindFieldByName("recipient");
            field.Should().NotBeNull("RemoteEnvelope must have a 'recipient' field.");
            field!.FieldNumber.Should().Be(1);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(RE_RecipientTag, "InnerEnvelopeWriter EnvRecipient tag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void RemoteEnvelope_MessageField_IsField2_LengthDelimited()
        {
            var field = RemoteEnvelope.Descriptor.FindFieldByName("message");
            field.Should().NotBeNull("RemoteEnvelope must have a 'message' field.");
            field!.FieldNumber.Should().Be(2);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(RE_MessageTag, "InnerEnvelopeWriter EnvMessage tag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void RemoteEnvelope_SenderField_IsField4_LengthDelimited()
        {
            var field = RemoteEnvelope.Descriptor.FindFieldByName("sender");
            field.Should().NotBeNull("RemoteEnvelope must have a 'sender' field.");
            field!.FieldNumber.Should().Be(4);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(RE_SenderTag, "InnerEnvelopeWriter EnvSender tag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void RemoteEnvelope_SeqField_IsField5_Fixed64()
        {
            var field = RemoteEnvelope.Descriptor.FindFieldByName("seq");
            field.Should().NotBeNull("RemoteEnvelope must have a 'seq' field.");
            field!.FieldNumber.Should().Be(5);
            field.FieldType.Should().Be(FieldType.Fixed64);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireType64Bit)
                .Should().Be(RE_SeqTag, "InnerEnvelopeWriter EnvSeqTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        // -- AcknowledgementInfo ----------------------------------------------

        [Fact]
        public void AcknowledgementInfo_CumulativeAckField_IsField1_Fixed64()
        {
            var field = AcknowledgementInfo.Descriptor.FindFieldByName("cumulativeAck");
            field.Should().NotBeNull("AcknowledgementInfo must have a 'cumulativeAck' field.");
            field!.FieldNumber.Should().Be(1);
            field.FieldType.Should().Be(FieldType.Fixed64);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireType64Bit)
                .Should().Be(AI_CumAckTag, "InnerEnvelopeWriter AckCumTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void AcknowledgementInfo_NacksField_IsField2_PackedFixed64()
        {
            var field = AcknowledgementInfo.Descriptor.FindFieldByName("nacks");
            field.Should().NotBeNull("AcknowledgementInfo must have a 'nacks' field.");
            field!.FieldNumber.Should().Be(2);
            field.FieldType.Should().Be(FieldType.Fixed64);
            field.IsRepeated.Should().BeTrue();
            // Packed repeated uses wireType 2 (length-delimited) for the packed bytes
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(AI_NacksTag, "InnerEnvelopeWriter AckNackTag (packed) mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        // -- Payload (SerializedMessage) --------------------------------------

        [Fact]
        public void Payload_MessageField_IsField1_LengthDelimited()
        {
            var field = Payload.Descriptor.FindFieldByName("message");
            field.Should().NotBeNull("Payload must have a 'message' field.");
            field!.FieldNumber.Should().Be(1);
            field.FieldType.Should().Be(FieldType.Bytes);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(PL_MessageTag, "InnerEnvelopeWriter PlMsgTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void Payload_SerializerIdField_IsField2_Varint()
        {
            var field = Payload.Descriptor.FindFieldByName("serializerId");
            field.Should().NotBeNull("Payload must have a 'serializerId' field.");
            field!.FieldNumber.Should().Be(2);
            field.FieldType.Should().Be(FieldType.Int32);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeVarint)
                .Should().Be(PL_SerializerIdTag, "InnerEnvelopeWriter PlSerIdTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        [Fact]
        public void Payload_MessageManifestField_IsField3_LengthDelimited()
        {
            var field = Payload.Descriptor.FindFieldByName("messageManifest");
            field.Should().NotBeNull("Payload must have a 'messageManifest' field.");
            field!.FieldNumber.Should().Be(3);
            field.FieldType.Should().Be(FieldType.Bytes);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(PL_ManifestTag, "InnerEnvelopeWriter PlManifTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }

        // -- ActorRefData -----------------------------------------------------

        [Fact]
        public void ActorRefData_PathField_IsField1_LengthDelimited()
        {
            var field = ActorRefData.Descriptor.FindFieldByName("path");
            field.Should().NotBeNull("ActorRefData must have a 'path' field.");
            field!.FieldNumber.Should().Be(1);
            field.FieldType.Should().Be(FieldType.String);
            ProtobufWire.MakeTag((uint)field.FieldNumber, ProtobufWire.WireTypeLengthDelimited)
                .Should().Be(ARD_PathTag, "InnerEnvelopeWriter ArPathTag mismatch. Edit InnerEnvelopeWriter.cs.");
        }
    }
}

