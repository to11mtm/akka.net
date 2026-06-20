//-----------------------------------------------------------------------
// <copyright file="MsgPackSerializerV2.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Runtime.Serialization;
using Akka.Actor;
using Akka.Configuration;
using Akka.Serialization.MessagePack.Internal;
using Akka.Util;
using Akka.Util.Reflection;
using MessagePack;

namespace Akka.Serialization.MessagePack
{
    /// <summary>
    /// A buffer-first, high-performance variant of <see cref="MsgPackSerializer"/> built on top of
    /// <see cref="SerializerV2"/>. UwU~ ✨
    ///
    /// <para>
    /// Unlike the classic <see cref="MsgPackSerializer"/> (which always materializes a fresh
    /// <c>byte[]</c> per operation), this serializer writes directly into a caller-owned
    /// <see cref="IBufferWriter{T}"/> and reads from a <see cref="ReadOnlySequence{T}"/>, allowing
    /// the remoting/persistence pipelines to avoid intermediate allocations and copies for better
    /// throughput. (◕‿◕)
    /// </para>
    /// </summary>
    /// <remarks>
    /// CopilotNote: this serializer shares the exact same resolver chain and
    /// <see cref="MessagePackSerializerOptions"/> as <see cref="MsgPackSerializer"/> via
    /// <see cref="MsgPackSerializer.BuildSerializerOptions"/>, so payloads are wire-compatible
    /// between the two. Only the I/O surface differs. It uses a distinct serializer
    /// <see cref="Identifier"/> (152) so both can be registered side-by-side.
    /// </remarks>
    public sealed class MsgPackSerializerV2 : SerializerV2
    {
        /// <summary>
        /// The shared MessagePack options (resolver chain, compression, type filtering).
        /// </summary>
        public readonly MessagePackSerializerOptions SerializerOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="MsgPackSerializerV2"/> class with default settings.
        /// </summary>
        public MsgPackSerializerV2(ExtendedActorSystem system)
            : this(system, MsgPackSerializerSettings.Default)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MsgPackSerializerV2"/> class from HOCON config.
        /// </summary>
        public MsgPackSerializerV2(ExtendedActorSystem system, Config config)
            : this(system, MsgPackSerializerSettings.Create(config))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MsgPackSerializerV2"/> class.
        /// </summary>
        public MsgPackSerializerV2(ExtendedActorSystem system, MsgPackSerializerSettings settings)
            : base(system)
        {
            SerializerOptions = MsgPackSerializer.BuildSerializerOptions(system, settings, out _, out _);
        }

        /// <summary>
        /// Distinct identifier from the classic <see cref="MsgPackSerializer"/> (151) so both can
        /// co-exist within the same actor system.
        /// </summary>
        public override int Identifier => 152;

        /// <inheritdoc />
        public override string Manifest(object obj)
            => obj.GetType().TypeQualifiedName();

        /// <inheritdoc />
        public override int Serialize(object obj, IBufferWriter<byte> writer)
        {
            // CopilotNote: MessagePack writes directly into the buffer writer — no temp byte[] uwu.
            // We wrap the caller's writer so we can count bytes reliably for *any* IBufferWriter,
            // not just ArrayBufferWriter, by observing every Advance() call. (◕‿◕)
            var counting = new WrappedBufferWriter(writer);
            MessagePackSerializer.Serialize(obj.GetType(), counting, obj, SerializerOptions);
            return checked((int)counting.BytesWritten);
        }

        /// <inheritdoc />
        public override object Deserialize(ReadOnlySequence<byte> bytes, string manifest)
        {
            if (string.IsNullOrEmpty(manifest))
                throw new SerializationException(
                    $"Cannot deserialize with an empty manifest for serializer id [{Identifier}].");

            Type type;
            try
            {
                type = TypeCache.GetType(manifest);
            }
            catch (Exception ex)
            {
                throw new SerializationException(
                    $"Cannot find manifest class [{manifest}] for serializer with id [{Identifier}].", ex);
            }

            return MessagePackSerializer.Deserialize(type, bytes, SerializerOptions)!;
        }

        /// <inheritdoc />
        public override byte[] ToBinary(object obj)
            => MessagePackSerializer.Serialize(obj.GetType(), obj, SerializerOptions);

        /// <inheritdoc />
        public override object FromBinary(byte[] bytes, string manifest)
            => Deserialize(new ReadOnlySequence<byte>(bytes), manifest);

        /// <inheritdoc />
        public override object FromBinary(byte[] bytes, Type type)
            => MessagePackSerializer.Deserialize(type, bytes, SerializerOptions)!;
    }
}






