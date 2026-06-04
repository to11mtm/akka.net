//-----------------------------------------------------------------------
// <copyright file="MessageSerializer.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using Akka.Actor;
using Akka.Serialization;
using Akka.Util;
using Google.Protobuf;
using SerializedMessage = Akka.Remote.Serialization.Proto.Msg.Payload;

namespace Akka.Remote
{
    public sealed class SysandTransportInfo
    {
        public ExtendedActorSystem System { get; }
        public Information TransportInformation { get; }

        public SysandTransportInfo(ExtendedActorSystem system, Information transportInformation)
        {
            System = system;
            TransportInformation = transportInformation;
        }
    }
    
    /// <summary>
    /// INTERNAL API.
    ///
    /// MessageSerializer is a helper for serializing and deserialize messages.
    /// </summary>
    internal static class MessageSerializer
    {
        /// <summary>
        /// Uses Akka Serialization for the specified ActorSystem to transform the given MessageProtocol to a message.
        /// </summary>
        /// <param name="system">The system.</param>
        /// <param name="messageProtocol">The message protocol.</param>
        /// <returns>System.Object.</returns>
        public static object Deserialize(ExtendedActorSystem system,
            SerializedMessage messageProtocol)
        {
            return system.Serialization.Deserialize(
                messageProtocol.Message.Memory,
                messageProtocol.SerializerId,
                !messageProtocol.MessageManifest.IsEmpty ? messageProtocol.MessageManifest.ToStringUtf8() : null);
        }

        /// <summary>
        /// Serializes the specified message.
        /// </summary>
        /// <param name="system">The system.</param>
        /// <param name="transportInformation">The address for the current transport</param>
        /// <param name="message">The message.</param>
        /// <returns>SerializedMessage.</returns>
        public static SerializedMessage Serialize(ExtendedActorSystem system, Information transportInformation,
            object message)
        {
            var serializer = system.Serialization.FindSerializerFor(message);

            var oldInfo = Akka.Serialization.Serialization.CurrentTransportInformation;
            try
            {
                Akka.Serialization.Serialization.CurrentTransportInformation = transportInformation; 
                
                var serializedMsg = new SerializedMessage
                {
                    Message = UnsafeByteOperations.UnsafeWrap(serializer.ToBinary(message)), //ByteString.CopyFrom(serializer.ToBinary(message)),
                    SerializerId = serializer.Identifier
                };

                if (serializer is SerializerWithStringManifest serializer2)
                {
                    var manifest = serializer2.Manifest(message);
                    if (!string.IsNullOrEmpty(manifest))
                    {
                        serializedMsg.MessageManifest = ByteString.CopyFromUtf8(manifest);
                    }
                }
                else
                {
                    if (serializer.IncludeManifest)
                        serializedMsg.MessageManifest = ByteString.CopyFromUtf8(message.GetType().TypeQualifiedName());
                }

                return serializedMsg;
            }
            finally
            {
                Akka.Serialization.Serialization.CurrentTransportInformation = oldInfo;
            }
        }

        /// <summary>
        /// Zero-copy serialize overload: writes the serialized message bytes directly into
        /// <paramref name="writer"/> via <see cref="Serializer.WriteTo"/>, avoiding the
        /// intermediate <c>byte[]</c> allocation from <see cref="Serialize(ExtendedActorSystem,Information,object)"/>. 🌸
        /// 
        /// <para>
        /// Returns the number of bytes written. The manifest and serializer-id are NOT written
        /// to the buffer — callers should obtain those from the returned
        /// <c>(serializerId, manifest)</c> tuple separately if needed.
        /// </para>
        /// 
        /// <!-- CopilotNotes: Used by PR-A's InnerEnvelopeWriter to write the serialized message
        ///      payload directly into the pooled outbound frame, eliminating W1 allocation. 🌸 -->
        /// </summary>
        /// <param name="sysandTransportInfo">Endpoint-held system and transport info to lower arg count.</param>
        /// <param name="message">The message to serialize.</param>
        /// <param name="writer">The buffer writer to receive the serialized bytes.</param>
        /// <returns>
        /// A tuple of (bytesWritten, serializerId, manifest) so callers can build the
        /// inner-envelope header without a separate <c>Serialize</c> call.
        /// </returns>
        public static (int bytesWritten, int serializerId, string? manifest) Serialize(
            SysandTransportInfo sysandTransportInfo,
            object message,
            IBufferWriter<byte> writer)
        {
            var serializer = sysandTransportInfo.System.Serialization.FindSerializerFor(message);
            var oldInfo    = Akka.Serialization.Serialization.CurrentTransportInformation;
            try
            {
                Akka.Serialization.Serialization.CurrentTransportInformation = sysandTransportInfo.TransportInformation;

                var bytesWritten = serializer.WriteTo(writer, message);

                string? manifest = null;
                if (serializer is SerializerWithStringManifest sm2)
                {
                    var m = sm2.Manifest(message);
                    if (!string.IsNullOrEmpty(m))
                        manifest = m;
                }
                else if (serializer.IncludeManifest)
                {
                    manifest = message.GetType().TypeQualifiedName();
                }

                return (bytesWritten, serializer.Identifier, manifest);
            }
            finally
            {
                Akka.Serialization.Serialization.CurrentTransportInformation = oldInfo;
            }
        }
    }
}
