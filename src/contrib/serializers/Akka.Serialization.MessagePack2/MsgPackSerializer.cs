//-----------------------------------------------------------------------
// <copyright file="MsgPackSerializer.cs" company="Akka.NET Project">
//     Copyright (C) 2017 Akka.NET Contrib <https://github.com/AkkaNetContrib/Akka.Serialization.MessagePack>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Akka.Serialization.MessagePack.Resolvers;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.ImmutableCollection;
using MessagePack.Resolvers;
using Newtonsoft.Json;

namespace Akka.Serialization.MessagePack
{
    public sealed class MsgPackSerializer : Serializer
    {
        public readonly MessagePackSerializerOptions SerializerOptions;

        public MsgPackSerializer(ExtendedActorSystem system) : this(system, MsgPackSerializerSettings.Default)
        {

        }

        public MsgPackSerializer(ExtendedActorSystem system, Config config) 
            : this(system, MsgPackSerializerSettings.Create(config))
        {
        }
        
        

        ///<remarks>
        /// Borrowed from <see cref="NewtonSoftJsonSerializer"/>,
        /// The concept is very similar if slightly more layered for perf.
        /// </remarks>
        public static IFormatterResolver LoadFormatterResolverByType(Type type, ExtendedActorSystem system)
        {
            //This -should- be double checked by others, but just in case :)
            if (typeof(IFormatterResolver).IsAssignableFrom(type))
            {
                //We look In this order:
                // - Is there a Ctor that will take ActorSystem/ExtendedActorSystem?
                // - Is there a Static 'Instance' Property/Field?
                // - Is there a Public, Parameterless Ctor?
                
                var ctors = type.GetConstructors();
                var actorSystemCtorMaybe = ctors.FirstOrDefault(r =>
                {
                    var p = r.GetParameters();
                    if (p.Length == 1 && p[0].ParameterType
                            .IsAssignableFrom(typeof(ExtendedActorSystem)))
                    {
                        return true;
                    }

                    return false;
                });
                if (actorSystemCtorMaybe != null)
                {
                    return (IFormatterResolver)actorSystemCtorMaybe.Invoke(new[]
                        { system });
                }
                
                var props = type.GetProperties(BindingFlags.Static | BindingFlags.Public);
                foreach (var propertyInfo in props)
                {
                    if (propertyInfo.Name == "Instance")
                    {
                        return (IFormatterResolver)propertyInfo.GetValue(null);
                    }
                }
                var fields = type.GetFields(BindingFlags.Static | BindingFlags.Public);
                foreach (var fieldInfo in fields)
                {
                    if (fieldInfo.Name == "Instance")
                    {
                        return (IFormatterResolver)fieldInfo.GetValue(null);
                    }
                }

                var defaultCtor =
                    ctors.FirstOrDefault(r => r.GetParameters().Length == 0);
                if (defaultCtor != null)
                {
                    return (IFormatterResolver)defaultCtor.Invoke(Array.Empty<object>());
                }

                throw new ArgumentException(
                    $"Type {type} does not contain a static 'Instance' Property/Field, Ctor that takes ActorSystem, or Parameterless Ctor!");
            }
            else
            {
                throw new ArgumentException(
                    $"Type {type} is not assignable to IMessageFormatter!");
            }
        }

        public MsgPackSerializer(ExtendedActorSystem system,
            MsgPackSerializerSettings settings) : base(system)
        {
            SerializerOptions = BuildSerializerOptions(system, settings, out _, out _);
        }

        /// <summary>
        /// Builds the shared <see cref="MessagePackSerializerOptions"/> (resolver chain, LZ4 compression,
        /// assembly-version handling and type filtering) used by both <see cref="MsgPackSerializer"/> and
        /// <see cref="MsgPackSerializerV2"/>. UwU — keeping things DRY so both serializers stay in sync~ ✨
        /// </summary>
        /// <remarks>
        /// CopilotNote: the V1 and V2 serializers MUST share this exact resolver chain so that payloads
        /// remain wire-compatible between them. Only the I/O surface (buffer-first vs. byte[]) differs.
        /// </remarks>
        internal static MessagePackTypeFilteringOptions BuildSerializerOptions(
            ExtendedActorSystem system,
            MsgPackSerializerSettings settings,
            out IFormatterResolver resolver,
            out IFormatterResolver polymorphicResolver)
        {
            //Set up the chain of resolvers;
            //First, we allow 'Overrides' that users put in at their own peril.
            //Then, we load our standard set of converters, which includes
            //Serializable(really just exceptions),
            //Immutable Collections, and our Surrogate Resolver
            //Then we add whatever custom serializers are specified,
            //Lastly dropping into the TypelessContractLess Resolver.
            //
            resolver = CompositeResolver.Create(
                settings.OverrideConverters.Select(t =>
                        LoadFormatterResolverByType(t, system))
                    .Concat(new[]
                    {
                        SerializableResolver.Instance,
                        ImmutableCollectionResolver.Instance,
                        settings.UseOldFormatterCompatibility
                            ? (IFormatterResolver)new
                                BackwardsCompatibleSurrogatedFormatterResolver(
                                    system)
                            : new SurrogatedFormatterResolver(system)
                    })
                    .Concat(settings.Converters.Select(t =>
                        LoadFormatterResolverByType(t, system)))
                    .Concat(new[]
                    {
                        TypelessContractlessStandardResolver.Instance
                    })
                    .ToArray());
            polymorphicResolver = new PolymorphicFormatterResolver(resolver);
            var opts =
                new MessagePackSerializerOptions(polymorphicResolver);
            if (settings.EnableLz4Compression == MsgPackSerializerSettings.Lz4Settings.Lz4Block)
            {
                opts = opts.WithCompression(MessagePackCompression.Lz4Block);
            }
            else if (settings.EnableLz4Compression ==
                     MsgPackSerializerSettings.Lz4Settings.Lz4BlockArray)
            {
                opts = opts.WithCompression(
                    MessagePackCompression.Lz4BlockArray);
            }

            opts = opts.WithAllowAssemblyVersionMismatch(settings
                .AllowAssemblyVersionMismatch);
            opts = opts.WithOmitAssemblyVersion(settings.OmitAssemblyVersion);
            //We handle type filtering via our own options set.
            //By doing so, the existing Typeless API will hook in,
            //i.e. we don't have to write our own Typeless Filter.
            return new MessagePackTypeFilteringOptions(opts);
        }

        public override byte[] ToBinary(object obj)
        {
            {
                return MessagePackSerializer.Serialize(obj.GetType(), obj,SerializerOptions);
            }
        }

        public override object FromBinary(byte[] bytes, Type type)
        {
            return MessagePackSerializer.Deserialize(type, bytes,SerializerOptions);
        }

        /*
        public override object FromBinary(ReadOnlyMemory<byte> bytes, Type type)
        {
            return MessagePackSerializer.Deserialize(type, bytes, SerializerOptions);
        }
        */

        public override int Identifier => 151;

        public override bool IncludeManifest => true;
        
    }
}
