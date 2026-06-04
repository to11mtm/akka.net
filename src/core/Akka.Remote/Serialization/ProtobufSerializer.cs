//-----------------------------------------------------------------------
// <copyright file="ProtobufSerializer.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Buffers;
using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Serialization;
using Akka.Util;
using Google.Protobuf;

namespace Akka.Remote.Serialization
{
    /// <summary>
    /// This is a special <see cref="Serializer"/> that serializes and deserializes Google protobuf messages only.
    /// </summary>
    public class ProtobufSerializer : Serializer
    {
        private static readonly ConcurrentDictionary<string, MessageParser> TypeLookup = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtobufSerializer"/> class.
        /// </summary>
        /// <param name="system">The actor system to associate with this serializer. </param>
        public ProtobufSerializer(ExtendedActorSystem system) : base(system)
        {
        }

        /// <inheritdoc />
        public override bool IncludeManifest => true;

        /// <inheritdoc />
        public override byte[] ToBinary(object obj)
        {
            var message = obj as IMessage;
            if (message != null)
            {
                return message.ToByteArray();
            }

            throw new ArgumentException($"Can't serialize a non-protobuf message using protobuf [{obj.GetType().TypeQualifiedName()}]");
        }

        /// <inheritdoc />
        public override object FromBinary(byte[] bytes, Type type)
        {
            if (TypeLookup.TryGetValue(type.FullName, out var parser))
            {
                return parser.ParseFrom(bytes);
            }
            // MethodParser is not in the cache, look it up with reflection
            IMessage msg = Activator.CreateInstance(type) as IMessage;
            if(msg == null) throw new ArgumentException($"Can't deserialize a non-protobuf message using protobuf [{type.TypeQualifiedName()}]");
            parser = msg.Descriptor.Parser;
            TypeLookup.TryAdd(type.FullName, parser);
            return parser.ParseFrom(bytes);
        }

        /// <summary>
        /// Zero-copy overload: parses the protobuf message from <paramref name="bytes"/> without
        /// calling <c>bytes.ToArray()</c>. Uses <c>MessageParser.ParseFrom(ReadOnlySequence&lt;byte&gt;)</c>
        /// (available in Google.Protobuf ≥ 3.21). 🌸
        ///
        /// <!-- CopilotNotes: new ReadOnlySequence<byte>(bytes) wraps the Memory without copying.
        ///      Protobuf reads directly from the sequence, so no extra byte[] alloc on the hot path.
        ///      This eliminates R4-fallback for protobuf-backed serializers (B.3 in PR-B). 🌸 -->
        /// </summary>
        public override object FromBinary(ReadOnlyMemory<byte> bytes, Type type)
        {
            if (!TypeLookup.TryGetValue(type.FullName, out var parser))
            {
                IMessage msg = Activator.CreateInstance(type) as IMessage;
                if (msg == null)
                    throw new ArgumentException($"Can't deserialize a non-protobuf message using protobuf [{type.TypeQualifiedName()}]");
                parser = msg.Descriptor.Parser;
                TypeLookup.TryAdd(type.FullName, parser);
            }

            return parser.ParseFrom(new ReadOnlySequence<byte>(bytes));
        }

        /// <summary>
        /// Writes the serialized protobuf message directly to <paramref name="writer"/>
        /// without a temporary byte[] allocation (B.4). 🌸
        /// </summary>
        public override int WriteTo(IBufferWriter<byte> writer, object obj)
        {
            var message = obj as IMessage;
            if (message == null)
                throw new ArgumentException($"Can't serialize a non-protobuf message using protobuf [{obj.GetType().TypeQualifiedName()}]");
            var size = message.CalculateSize();
            var span = writer.GetSpan(size);
            message.WriteTo(span);
            writer.Advance(size);
            return size;
        }
    }
}
