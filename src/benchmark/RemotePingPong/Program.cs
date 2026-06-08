//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Util.Internal;
using MessagePack;

namespace RemotePingPong
{
    [MessagePackObject(keyAsPropertyName:true)]
        public sealed class BenchmarkEnvelope
        {
            public long SequenceNr { get; set; }

            public string Marker { get; set; } = "hit";

            public DateTime TimestampUtc { get; set; }
        }

        // ── Deep-nested payload classes ────────────────────────────────────────
        // CopilotNotes: Three-level hierarchy where every level has 3× as many scalar
        // fields as its parent (root = 3, level-2 = 9, level-3 = 27). This is the LEAF.

        /// <summary>
        /// Leaf node of the deep-nested benchmark payload — 27 mixed-type fields. 🌿
        /// </summary>
        // [Serializable]
        [MessagePackObject(keyAsPropertyName:true)]
        public sealed class DeepLevel3
        {
            // 9 longs
            public long L1 { get; set; } public long L2 { get; set; } public long L3 { get; set; }
            public long L4 { get; set; } public long L5 { get; set; } public long L6 { get; set; }
            public long L7 { get; set; } public long L8 { get; set; } public long L9 { get; set; }
            // 9 strings
            public string S1 { get; set; } = "a"; public string S2 { get; set; } = "b"; public string S3 { get; set; } = "c";
            public string S4 { get; set; } = "d"; public string S5 { get; set; } = "e"; public string S6 { get; set; } = "f";
            public string S7 { get; set; } = "g"; public string S8 { get; set; } = "h"; public string S9 { get; set; } = "i";
            // 6 doubles + 3 bools (= 9 more fields → total 27)
            public double D1 { get; set; } public double D2 { get; set; } public double D3 { get; set; }
            public double D4 { get; set; } public double D5 { get; set; } public double D6 { get; set; }
            public bool   B1 { get; set; } public bool   B2 { get; set; } public bool   B3 { get; set; }
        }

        /// <summary>
        /// Middle node of the deep-nested benchmark payload — 9 scalar fields plus 3
        /// <see cref="DeepLevel3"/> children. 🌿
        /// </summary>
        // [Serializable]
        [MessagePackObject(keyAsPropertyName:true)]
        public sealed class DeepLevel2
        {
            // 3 longs, 3 strings, 2 doubles, 1 bool = 9 fields
            public long   L1 { get; set; } public long   L2 { get; set; } public long   L3 { get; set; }
            public string S1 { get; set; } = "x"; public string S2 { get; set; } = "y"; public string S3 { get; set; } = "z";
            public double D1 { get; set; } public double D2 { get; set; }
            public bool   B1 { get; set; }
            // 3 leaf children
            public DeepLevel3 Sub1 { get; set; } = new();
            public DeepLevel3 Sub2 { get; set; } = new();
            public DeepLevel3 Sub3 { get; set; } = new();
        }

        /// <summary>
        /// Root of the deep-nested benchmark payload — 3 scalar fields plus 3
        /// <see cref="DeepLevel2"/> children (each of which contains 3 <see cref="DeepLevel3"/>
        /// leaf nodes). UwU 🌳
        /// </summary>
        //[Serializable]
        [MessagePackObject(keyAsPropertyName:true)]
        public sealed class DeepObject
        {
            // 3 fields at root level
            public long     SequenceNr   { get; set; }
            public string   Marker       { get; set; } = "deep";
            public DateTime TimestampUtc { get; set; }
            // 3 level-2 children
            public DeepLevel2 Sub1 { get; set; } = new();
            public DeepLevel2 Sub2 { get; set; } = new();
            public DeepLevel2 Sub3 { get; set; } = new();
        }

        // ── Large payload class ────────────────────────────────────────────────

        /// <summary>
        /// Large benchmark payload: one <see cref="long"/>, one <see cref="Guid"/>,
        /// and a 37 KB <see cref="byte"/> array.  Tests the heavy-allocation path
        /// of every serializer. nyaa~ 📦
        /// </summary>
        //[Serializable]
        [MessagePackObject(keyAsPropertyName:true)]
        public sealed class LargePayloadEnvelope
        {
            public long   SequenceNr { get; set; }
            public Guid   Id         { get; set; }
            /// <summary>37 KB payload buffer. Pre-filled with deterministic bytes. 🗜️</summary>
            public byte[] Data       { get; set; } = Array.Empty<byte>();
        }
        
    public static class Messages
    {
        public class Msg { public override string ToString() { return "msg"; } }
        public class Run { public override string ToString() { return "run"; } }
        public class Started { public override string ToString() { return "started"; } }
    }

    internal class Program
    {
        public static uint CpuSpeed()
        {
#if THREADS
            var mo = new System.Management.ManagementObject("Win32_Processor.DeviceID='CPU0'");
            var sp = (uint)(mo["CurrentClockSpeed"]);
            mo.Dispose();
            return sp;
#else
            return 0;
            
#endif
        }

        /// <summary>
        /// Holds the measured result for a single (combo × client-count) benchmark run.
        /// Collected by <see cref="RunBattleRoyaleAsync"/> and formatted into a markdown table. 📊
        /// </summary>
        private sealed record BenchmarkRunResult(
            TransportMode  Transport,
            SerializerMode Serializer,
            PayloadMode    Payload,
            int            NumberOfClients,
            long           TotalMessages,
            long           ThroughputMsgPerSec,
            double         ElapsedMs);

        private static readonly (SerializerMode, PayloadMode, TransportMode)[] BattleRoyale =
        [
            // ── DotNetty baseline ─────────────────────────────────────────────
            (SerializerMode.Default,  PayloadMode.Primitive,        TransportMode.DotNetty),
            (SerializerMode.Default,  PayloadMode.SerializedObject, TransportMode.DotNetty),
            (SerializerMode.Hyperion, PayloadMode.SerializedObject, TransportMode.DotNetty),
            (SerializerMode.MsgPack,  PayloadMode.SerializedObject, TransportMode.DotNetty),

            // ── Pipe/Protobuf (zero-copy = off) ──────────────────────────────
            (SerializerMode.Default,  PayloadMode.Primitive,        TransportMode.PipeProtobuf),
            (SerializerMode.Default,  PayloadMode.SerializedObject, TransportMode.PipeProtobuf),
            (SerializerMode.Hyperion, PayloadMode.SerializedObject, TransportMode.PipeProtobuf),
            (SerializerMode.MsgPack,  PayloadMode.SerializedObject, TransportMode.PipeProtobuf),

            // ── Pipe/Protobuf + zero-copy-codec ──────────────────────────────
            // CopilotNotes: Same wire format as PipeProtobuf but with the zero-copy outbound
            // codec enabled (akka.remote.pipe.tcp.zero-copy-codec = on). UwU 🌸
            (SerializerMode.Default,  PayloadMode.Primitive,        TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.Default,  PayloadMode.SerializedObject, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.Hyperion, PayloadMode.SerializedObject, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.MsgPack,  PayloadMode.SerializedObject, TransportMode.PipeProtobufZeroCopy),

            /*
            // ── Deep nested object (3 levels, 3× fields per level) ────────────
            // CopilotNotes: Tests graph-traversal serializer overhead with a small but deeply
            // nested object tree (root → 3 level-2 nodes → 9 leaf nodes). nyaa~ 🌳
            (SerializerMode.Default,  PayloadMode.DeepObject, TransportMode.DotNetty),
            (SerializerMode.Hyperion, PayloadMode.DeepObject, TransportMode.DotNetty),
            (SerializerMode.MsgPack,  PayloadMode.DeepObject, TransportMode.DotNetty),
            (SerializerMode.Default,  PayloadMode.DeepObject, TransportMode.PipeProtobuf),
            (SerializerMode.Hyperion, PayloadMode.DeepObject, TransportMode.PipeProtobuf),
            (SerializerMode.MsgPack,  PayloadMode.DeepObject, TransportMode.PipeProtobuf),
            (SerializerMode.Default,  PayloadMode.DeepObject, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.Hyperion, PayloadMode.DeepObject, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.MsgPack,  PayloadMode.DeepObject, TransportMode.PipeProtobufZeroCopy),

            // ── Large payload (int64 + Guid + 37 KB byte[]) ──────────────────
            // CopilotNotes: Tests heavy-allocation / memory-pressure path; each message
            // carries a 37 KB byte array through the full serializer + transport stack. 📦
            (SerializerMode.Default,  PayloadMode.LargePayload, TransportMode.DotNetty),
            (SerializerMode.Hyperion, PayloadMode.LargePayload, TransportMode.DotNetty),
            (SerializerMode.MsgPack,  PayloadMode.LargePayload, TransportMode.DotNetty),
            (SerializerMode.Default,  PayloadMode.LargePayload, TransportMode.PipeProtobuf),
            (SerializerMode.Hyperion, PayloadMode.LargePayload, TransportMode.PipeProtobuf),
            (SerializerMode.MsgPack,  PayloadMode.LargePayload, TransportMode.PipeProtobuf),
            (SerializerMode.Default,  PayloadMode.LargePayload, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.Hyperion, PayloadMode.LargePayload, TransportMode.PipeProtobufZeroCopy),
            (SerializerMode.MsgPack,  PayloadMode.LargePayload, TransportMode.PipeProtobufZeroCopy),
            */
        ];

        /// <summary>
        /// Selects which message-body serializer is used for user messages during the benchmark.
        /// This is orthogonal to the transport-envelope codec (protobuf vs messagepack).
        /// </summary>
        internal enum SerializerMode
        {
            /// <summary>
            /// Akka.NET built-in NewtonSoft JSON serializer (the historic default).
            /// Good for correctness checks; not the fastest~
            /// </summary>
            Default,

            /// <summary>
            /// Hyperion binary serializer — fast, schema-tolerant.
            /// Requires <c>Akka.Serialization.Hyperion</c> package. UwU
            /// </summary>
            Hyperion,

            /// <summary>
            /// MessagePack typeless serializer — very compact + fast.
            /// Requires <c>Akka.Serialization.MessagePack</c> package. Nyaa~!
            /// </summary>
            MsgPack,
        }

        /// <summary>
        /// Controls what message body type is sent in the ping-pong loop.
        /// </summary>
        internal enum PayloadMode
        {
            /// <summary>
            /// Sends a primitive <see cref="long"/> payload (fast path / primitive serializer path).
            /// </summary>
            Primitive,

            /// <summary>
            /// Sends a custom object payload so user serializers (Hyperion/MessagePack/JSON)
            /// are actually exercised end-to-end.
            /// </summary>
            SerializedObject,

            /// <summary>
            /// Sends a 3-level deeply nested object where each successive level has 3× as many
            /// fields as the previous (root = 3 fields, level-2 nodes = 9 fields each,
            /// level-3 leaf nodes = 27 fields each). Each root object fans out to 3 level-2
            /// nodes, each of which contains 3 level-3 leaf nodes — 9 leaves total per message.
            /// Great for stress-testing graph-serializer overhead! uwu 🌳
            /// </summary>
            DeepObject,

            /// <summary>
            /// Sends an object containing an <see cref="long"/>, a <see cref="Guid"/>,
            /// and a 37 KB <see cref="byte"/> array.  Exercises the large-payload /
            /// memory-pressure path of every serializer. nyaa~ 📦
            /// </summary>
            LargePayload,
        }

        private static PayloadMode ParsePayloadMode(string? arg)
        {
            return (arg ?? "").ToLowerInvariant() switch
            {
                "object" or "serialized" or "serializer"         => PayloadMode.SerializedObject,
                "deep" or "deepobject" or "nested"               => PayloadMode.DeepObject,
                "large" or "largepayload" or "big" or "bigpayload" => PayloadMode.LargePayload,
                _                                                => PayloadMode.Primitive,
            };
        }

        private static string PayloadLabel(PayloadMode mode) => mode switch
        {
            PayloadMode.SerializedObject => "Custom object (serializer path)",
            PayloadMode.DeepObject       => "Deep nested object (3 levels, 3×fields)",
            PayloadMode.LargePayload     => "Large payload (int64 + Guid + 37 KB byte[])",
            _                            => "Primitive long",
        };

        /// <summary>
        /// Parses a serializer mode string from the command line.
        /// Valid values (case-insensitive): "default", "hyperion", "msgpack", "messagepack".
        /// Defaults to <see cref="SerializerMode.Default"/> when unrecognised.
        /// </summary>
        private static SerializerMode ParseSerializerMode(string? arg)
        {
            return (arg ?? "").ToLowerInvariant() switch
            {
                "hyperion"                    => SerializerMode.Hyperion,
                "msgpack" or "messagepack"    => SerializerMode.MsgPack,
                _                             => SerializerMode.Default,
            };
        }

        private static string SerializerLabel(SerializerMode mode) => mode switch
        {
            SerializerMode.Hyperion => "Hyperion",
            SerializerMode.MsgPack  => "MessagePack (typeless)",
            _                       => "JSON (default)",
        };

        /// <summary>
        /// Selects which Akka.Remote transport driver + PDU codec to use for the benchmark run.
        /// </summary>
        internal enum TransportMode
        {
            /// <summary>Legacy DotNetty TCP transport with protobuf codec (the historical baseline).</summary>
            DotNetty,

            /// <summary>System.IO.Pipelines TCP transport with protobuf codec (wire-compatible).</summary>
            PipeProtobuf,

            /// <summary>
            /// System.IO.Pipelines TCP transport with protobuf codec <em>and</em>
            /// <c>akka.remote.pipe.tcp.zero-copy-codec = on</c>.
            /// Same wire format as <see cref="PipeProtobuf"/>; the zero-copy path avoids
            /// an extra ByteString allocation on the outbound write path. uwu~ ✨
            /// </summary>
            PipeProtobufZeroCopy,
        }

        /// <summary>
        /// Parses a transport mode string from the command line.
        /// Valid values (case-insensitive): "dotnetty", "pipe", "pipe-protobuf", "pipelines",
        /// "pipe-zc", "pipe-zerocopy", "pipe-protobuf-zerocopy".
        /// Defaults to <see cref="TransportMode.DotNetty"/> when the string is empty / unrecognised.
        /// </summary>
        private static TransportMode ParseTransportMode(string? arg)
        {
            return (arg ?? "").ToLowerInvariant() switch
            {
                "pipe" or "pipe-protobuf" or "pipelines"                          => TransportMode.PipeProtobuf,
                "pipe-zc" or "pipe-zerocopy" or "pipe-protobuf-zerocopy" or "zc"  => TransportMode.PipeProtobufZeroCopy,
                _                                                                   => TransportMode.DotNetty,
            };
        }

        private static string TransportLabel(TransportMode mode) => mode switch
        {
            TransportMode.PipeProtobuf         => "Pipe/TCP + Protobuf",
            TransportMode.PipeProtobufZeroCopy => "Pipe/TCP + Protobuf + ZeroCopy",
            _                                  => "DotNetty/TCP + Protobuf",
        };

        

        // CopilotNotes: Pre-allocate the 37 KB buffer once so CreatePingPayload is cheap.
        // The byte[] is shared across all Tell() calls (same reference) — that's intentional:
        // we're benchmarking serializer/transport overhead, not allocation cost. UwU
        private static readonly byte[] _largeBuffer = BuildLargeBuffer(37 * 1024);

        private static byte[] BuildLargeBuffer(int size)
        {
            var buf = new byte[size];
            for (var i = 0; i < size; i++)
                buf[i] = (byte)(i & 0xFF);
            return buf;
        }

        private static object CreatePingPayload(PayloadMode mode) => mode switch
        {
            // CopilotNotes: Primitive mode intentionally keeps serializer overhead minimal.
            PayloadMode.Primitive => 1L,

            // CopilotNotes: DeepObject fans out into 9 leaf nodes (3 level-2 × 3 level-3) — a
            // nice stress-test for graph-traversal serializers. 🌳
            PayloadMode.DeepObject => new DeepObject
            {
                SequenceNr   = 1L,
                Marker       = "deep",
                TimestampUtc = DateTime.UtcNow,
                Sub1 = new DeepLevel2 { L1 = 10, L2 = 11, L3 = 12, S1 = "aa", D1 = 1.1, B1 = true,
                    Sub1 = new DeepLevel3 { L1 = 100, S1 = "aaa", D1 = 1.11 },
                    Sub2 = new DeepLevel3 { L1 = 101, S1 = "bbb", D1 = 1.12 },
                    Sub3 = new DeepLevel3 { L1 = 102, S1 = "ccc", D1 = 1.13 } },
                Sub2 = new DeepLevel2 { L1 = 20, L2 = 21, L3 = 22, S1 = "bb", D1 = 2.1, B1 = false,
                    Sub1 = new DeepLevel3 { L1 = 200, S1 = "ddd", D1 = 2.11 },
                    Sub2 = new DeepLevel3 { L1 = 201, S1 = "eee", D1 = 2.12 },
                    Sub3 = new DeepLevel3 { L1 = 202, S1 = "fff", D1 = 2.13 } },
                Sub3 = new DeepLevel2 { L1 = 30, L2 = 31, L3 = 32, S1 = "cc", D1 = 3.1, B1 = true,
                    Sub1 = new DeepLevel3 { L1 = 300, S1 = "ggg", D1 = 3.11 },
                    Sub2 = new DeepLevel3 { L1 = 301, S1 = "hhh", D1 = 3.12 },
                    Sub3 = new DeepLevel3 { L1 = 302, S1 = "iii", D1 = 3.13 } },
            },

            // CopilotNotes: LargePayloadEnvelope reuses the pre-built 37 KB buffer so we're
            // only measuring serializer/transport overhead, not allocation. nyaa~ 📦
            PayloadMode.LargePayload => new LargePayloadEnvelope
            {
                SequenceNr = 1L,
                Id         = Guid.NewGuid(),
                Data       = _largeBuffer,
            },

            _ => new BenchmarkEnvelope
            {
                SequenceNr   = 1L,
                Marker       = "hit",
                TimestampUtc = DateTime.UtcNow,
            },
        };

        public static Config CreateActorSystemConfig(
            string actorSystemName,
            string ipOrHostname,
            int port,
            TransportMode mode = TransportMode.DotNetty,
            SerializerMode serializerMode = SerializerMode.Default)
        {
            // ── Base config shared by all modes ──────────────────────────────
            var baseConfig = ConfigurationFactory.ParseString(@"
            akka {
              actor.provider = remote
              loglevel = ERROR
              suppress-json-serializer-warning = on
              log-dead-letters = off
              remote {
                log-remote-lifecycle-events = off
              }
            }");

            // ── Serializer-specific overrides ─────────────────────────────────
            // CopilotNotes: Both Hyperion and MessagePack bind System.Object so they
            // handle every user message. Default leaves the out-of-box JSON serializer.
            Config serializerConfig = serializerMode switch
            {
                SerializerMode.Hyperion => ConfigurationFactory.ParseString(@"
                    akka.actor {
                        serializers.hyperion = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
                        serialization-bindings {
                            ""System.Object"" = hyperion
                        }
                    }"),

                SerializerMode.MsgPack => ConfigurationFactory.ParseString(@"
                    akka.actor {
                        serializers.messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                        serialization-bindings {
                            ""System.Object"" = messagepack
                        }
                    }"),

                _ => Config.Empty, // nyaa~ nothing extra needed for default JSON
            };

            // ── Transport-specific overrides ─────────────────────────────────
            Config transportConfig = mode switch
            {
                TransportMode.PipeProtobuf => ConfigurationFactory.ParseString($@"
                    akka.remote {{
                        enabled-transports = [""akka.remote.pipe.tcp""]
                        pipe.tcp {{
                            hostname = ""{ipOrHostname}""
                            port     = {port}
                            envelope = protobuf
                        }}
                    }}"),

                // CopilotNotes: PipeProtobufZeroCopy is identical to PipeProtobuf on the wire;
                // the only difference is zero-copy-codec = on which avoids an extra ByteString
                // copy on the outbound write path. nyaa~ 🌸
                TransportMode.PipeProtobufZeroCopy => ConfigurationFactory.ParseString($@"
                    akka.remote {{
                        enabled-transports = [""akka.remote.pipe.tcp""]
                        pipe.tcp {{
                            hostname        = ""{ipOrHostname}""
                            port            = {port}
                            envelope        = protobuf
                            zero-copy-codec = on
                        }}
                    }}"),

                // Default: DotNetty
                _ => ConfigurationFactory.ParseString($@"
                    akka.remote {{
                        enabled-transports = [""akka.remote.dot-netty.tcp""]
                        dot-netty.tcp {{
                            hostname = ""{ipOrHostname}""
                            port     = {port}
                        }}
                    }}"),
            };

            // CopilotNotes: Merge order is: transport > serializer > base.
            // WithFallback means "use this if not already set", so highest priority goes first.
            return transportConfig
                .WithFallback(serializerConfig)
                .WithFallback(baseConfig);
        }

        private static async Task Main(params string[] args)
        {
            try
            {
                Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync(
                    $"Attempted to elevate process priority, but failed due to {ex.Message} - carrying on at normal process priority.");
            }

            // ── Parse args ────────────────────────────────────────────────────
            // Single-run usage: RemotePingPong [timesToRun] [transport] [serializer] [payload]
            //   transport:  dotnetty | pipe | pipe-protobuf | pipe-zc | pipe-zerocopy | pipe-protobuf-zerocopy
            //   serializer: default  | hyperion | msgpack
            //   payload:    primitive | object
            //
            // Battle-royale usage: RemotePingPong battle [outputFile?]
            //   outputFile: path to write the markdown table; omit to print to stdout. 🎖️
            //   Runs all (transport × serializer × payload) combos including zero-copy variants! uwu ✨
            if ((args.Length >= 1 && args[0].Equals("battle", StringComparison.OrdinalIgnoreCase)) ||
                (args.Length >= 1 && args[0].Equals("--battle-royale", StringComparison.OrdinalIgnoreCase)))
            {
                var outputFile = args.Length >= 2 ? args[1] : null;
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine("⚔️  Battle Royale mode! Running all transport/serializer/payload combos~ uwu ✨");
                Console.ResetColor();
                await RunBattleRoyaleAsync(outputFile);
                return;
            }

            uint timesToRun = 1;
            var  transportMode   = TransportMode.DotNetty;
            var  serializerMode  = SerializerMode.Default;
            var  payloadMode     = PayloadMode.Primitive;

            if (args.Length >= 1 && !uint.TryParse(args[0], out timesToRun))
                timesToRun = 1;
            if (args.Length >= 2)
                transportMode = ParseTransportMode(args[1]);
            if (args.Length >= 3)
                serializerMode = ParseSerializerMode(args[2]);
            if (args.Length >= 4)
                payloadMode = ParsePayloadMode(args[3]);

            
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"Transport mode: {TransportLabel(transportMode)}");
            Console.WriteLine($"Serializer:     {SerializerLabel(serializerMode)}");
            Console.WriteLine($"Payload:        {PayloadLabel(payloadMode)}");
            Console.ResetColor();

            await Start(timesToRun, transportMode, serializerMode, payloadMode);
        }

        private static bool _firstRun = true;

        private static void PrintSysInfo(TransportMode mode, SerializerMode serializerMode, PayloadMode payloadMode)
        {
            var processorCount = Environment.ProcessorCount;
            if (processorCount == 0)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Failed to read processor count..");
                return;
            }

            Console.WriteLine("Transport:                         {0}", TransportLabel(mode));
            Console.WriteLine("Serializer:                        {0}", SerializerLabel(serializerMode));
            Console.WriteLine("Payload:                           {0}", PayloadLabel(payloadMode));
            Console.WriteLine("OSVersion:                         {0}", Environment.OSVersion);
            Console.WriteLine("ProcessorCount:                    {0}", processorCount);
            Console.WriteLine("ClockSpeed:                        {0} MHZ", CpuSpeed());
            Console.WriteLine("Actor Count:                       {0}", processorCount * 2);
            Console.WriteLine("Messages sent/received per client: {0}  ({0:0e0})", repeat * 2);
            Console.WriteLine("Is Server GC:                      {0}", GCSettings.IsServerGC);
            Console.WriteLine("Thread count:                      {0}", Process.GetCurrentProcess().Threads.Count);
            Console.WriteLine();
            Console.WriteLine("Num clients, Total [msg], Msgs/sec, Total [ms], Start Threads, End Threads");

            _firstRun = false;
        }

        const long repeat = 50000L;

        private static async Task Start(
            uint timesToRun,
            TransportMode mode,
            SerializerMode serializerMode,
            PayloadMode payloadMode)
        {
            for (var i = 0; i < timesToRun; i++)
            {
                var redCount = 0;
                var bestThroughput = 0L;
                foreach (var throughput in GetClientSettings())
                {
                    var result1 = await Benchmark(
                        throughput,
                        repeat,
                        bestThroughput,
                        redCount,
                        mode,
                        serializerMode,
                        payloadMode);
                    bestThroughput = result1.Item2;
                    redCount = result1.Item3;
                }
            }

            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine("Done..");
        }

        public static IEnumerable<int> GetClientSettings()
        {
            yield return 1;
            yield return 5;
            yield return 10;
            yield return 15;
            yield return 20;
            yield return 25;
            yield return 30;
        }

        private static long GetTotalMessagesReceived(int numberOfClients, long numberOfRepeats)
        {
            return numberOfClients * numberOfRepeats * 2;
        }

        private static async Task<(bool, long, int)> Benchmark(
            int numberOfClients,
            long numberOfRepeats,
            long bestThroughput,
            int redCount,
            TransportMode mode,
            SerializerMode serializerMode,
            PayloadMode payloadMode,
            List<BenchmarkRunResult>? collector = null)
        {
            var totalMessagesReceived = GetTotalMessagesReceived(numberOfClients, numberOfRepeats);
            var system1 = ActorSystem.Create("SystemA", CreateActorSystemConfig("SystemA", "127.0.0.1", 0, mode, serializerMode));
            var system2 = ActorSystem.Create("SystemB", CreateActorSystemConfig("SystemB", "127.0.0.1", 0, mode, serializerMode));

            List<Task<long>> tasks = new List<Task<long>>();
            List<IActorRef> receivers = new List<IActorRef>();

            var canStart = system1.ActorOf(Props.Create(() => new AllStartedActor()), "canStart");

            var system1Address = ((ExtendedActorSystem)system1).Provider.DefaultAddress;
            var system2Address = ((ExtendedActorSystem)system2).Provider.DefaultAddress;

            var echoProps = Props.Create(() => new EchoActor()).WithDeploy(new Deploy(new RemoteScope(system2Address)));

            for (var i = 0; i < numberOfClients; i++)
            {
                var echo = system1.ActorOf(echoProps, "echo" + i);
                var ts = new TaskCompletionSource<long>();
                tasks.Add(ts.Task);
                var receiver =
                    system1.ActorOf(
                        Props.Create(() => new BenchmarkActor(numberOfRepeats, ts, echo)),
                        "benchmark" + i);

                receivers.Add(receiver);

                canStart.Tell(echo);
                canStart.Tell(receiver);
            }

            var rsp = await canStart.Ask(new AllStartedActor.AllStarted(), TimeSpan.FromSeconds(10));
            var testReady = (bool)rsp;
            if (!testReady)
            {
                throw new Exception(
                    "Received report that 1 or more remote actor is unable to begin the test. Aborting run.");
            }

            if (_firstRun)
            {
                PrintSysInfo(mode, serializerMode, payloadMode);
            }

            var startThreads = Process.GetCurrentProcess().Threads.Count;
            var pingPayload = CreatePingPayload(payloadMode);

            var sw = Stopwatch.StartNew();
            receivers.ForEach(c =>
            {
                for (var i = 0; i < 50; i++) // prime the pump
                    c.Tell(pingPayload);
            });
            await Task.WhenAll(tasks);
            sw.Stop();

            var endThreads = Process.GetCurrentProcess().Threads.Count;

            await Task.WhenAll(new[] { system1.Terminate(), system2.Terminate() });

            var elapsedMilliseconds = sw.ElapsedMilliseconds;
            long throughput = elapsedMilliseconds == 0
                ? -1
                : (long)Math.Ceiling((double)totalMessagesReceived / elapsedMilliseconds * 1000);

            var foregroundColor = Console.ForegroundColor;
            if (throughput >= bestThroughput)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                bestThroughput = throughput;
                redCount = 0;
            }
            else
            {
                redCount++;
                Console.ForegroundColor = ConsoleColor.Red;
            }

            Console.ForegroundColor = foregroundColor;
            Console.WriteLine(
                "{0,10},{1,8},{2,10},{3,11}, {4,13}, {5,15}",
                numberOfClients,
                totalMessagesReceived,
                throughput,
                sw.Elapsed.TotalMilliseconds.ToString("F2", CultureInfo.InvariantCulture),
                startThreads,
                endThreads);

            // CopilotNotes: If a collector is provided (battle royale mode) we stash the
            // raw measurements for later markdown table rendering. 📊
            collector?.Add(new BenchmarkRunResult(
                mode,
                serializerMode,
                payloadMode,
                numberOfClients,
                totalMessagesReceived,
                throughput,
                sw.Elapsed.TotalMilliseconds));

            return (redCount <= 3, bestThroughput, redCount);
        }

        // ── Battle Royale ──────────────────────────────────────────────────────

        /// <summary>
        /// Runs every (transport × serializer × payload) combo in <see cref="BattleRoyale"/>,
        /// printing live console progress while collecting <see cref="BenchmarkRunResult"/> entries.
        /// On completion renders a markdown table to <paramref name="outputFile"/> (or stdout). 🎖️
        /// </summary>
        private static async Task RunBattleRoyaleAsync(string? outputFile)
        {
            var all = new List<BenchmarkRunResult>();

            foreach (var (serializer, payload, transport) in BattleRoyale)
            {
                _firstRun = true; // reset header so sysinfo prints for each combo

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine();
                Console.WriteLine(
                    "── {0} | {1} | {2} ──",
                    TransportLabel(transport),
                    SerializerLabel(serializer),
                    PayloadLabel(payload));
                Console.ResetColor();

                var redCount       = 0;
                var bestThroughput = 0L;

                foreach (var clients in GetClientSettings())
                {
                    var result = await Benchmark(
                        clients, repeat, bestThroughput, redCount,
                        transport, serializer, payload,
                        collector: all);
                    bestThroughput = result.Item2;
                    redCount       = result.Item3;
                }
            }

            // ── Emit markdown ──────────────────────────────────────────────
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine();
            Console.WriteLine(outputFile is null
                ? "✨ Battle Royale complete! Printing markdown table to stdout~ nyaa~"
                : $"✨ Battle Royale complete! Writing markdown table to: {outputFile}");
            Console.ResetColor();

            var md = BuildMarkdownTable(all);

            if (outputFile is null)
            {
                Console.WriteLine(md);
            }
            else
            {
                await File.WriteAllTextAsync(outputFile, md);
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"Written {md.Length:N0} chars to {outputFile} 🌸");
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Builds a markdown document from the collected benchmark results.
        ///
        /// <para>
        /// Emits two tables:
        /// <list type="bullet">
        ///   <item><b>Summary</b> — one row per combo with best throughput and the client count that achieved it.</item>
        ///   <item><b>Detail</b> — one row per (combo × client-count) measurement.</item>
        /// </list>
        /// </para>
        ///
        /// <!-- CopilotNotes: We group by (Transport, Serializer, Payload) using LINQ to produce the
        ///      summary rows, then flatten back to individual rows for the detail table. -->
        /// </summary>
        private static string BuildMarkdownTable(List<BenchmarkRunResult> results)
        {
            var sb = new StringBuilder();

            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            sb.AppendLine($"# RemotePingPong Battle Royale — {timestamp} UTC");
            sb.AppendLine();
            sb.AppendLine($"> Generated by `RemotePingPong battle`. Repeat count per client: `{repeat:N0}` messages.");
            sb.AppendLine();

            // ── Summary table ──────────────────────────────────────────────
            sb.AppendLine("## Summary — Best Throughput per Combo");
            sb.AppendLine();
            sb.AppendLine("| Transport | Serializer | Payload | Best Msgs/sec | At Clients |");
            sb.AppendLine("|-----------|------------|---------|--------------:|:----------:|");

            foreach (var g in results
                         .GroupBy(r => (r.Transport, r.Serializer, r.Payload))
                         .OrderByDescending(g => g.Max(r => r.ThroughputMsgPerSec)))
            {
                var best = g.MaxBy(r => r.ThroughputMsgPerSec)!;
                if (best.Payload == PayloadMode.Primitive)
                {
                    // we want to put PrimitiveSerializer here because it's the one actually used for a primitive:
                    sb.AppendLine(
                        $"| {TransportLabel(best.Transport)} | PrimitiveSerializer | {PayloadLabel(best.Payload)} " +
                        $"| {best.ThroughputMsgPerSec:N0} | {best.NumberOfClients} |");
                }
                else
                {
                    sb.AppendLine(
                        $"| {TransportLabel(best.Transport)} | {SerializerLabel(best.Serializer)} | {PayloadLabel(best.Payload)} " +
                        $"| {best.ThroughputMsgPerSec:N0} | {best.NumberOfClients} |");    
                }
                
            }

            sb.AppendLine();

            // ── Detail table ───────────────────────────────────────────────
            sb.AppendLine("## Detail — All Measurements");
            sb.AppendLine();
            sb.AppendLine("| Transport | Serializer | Payload | Clients | Msgs/sec | Total Msgs | Time (ms) |");
            sb.AppendLine("|-----------|------------|---------|--------:|---------:|-----------:|----------:|");

            // CopilotNotes: Group ordering matches BattleRoyale declaration order, then by client count.
            foreach (var g in results.GroupBy(r => (r.Transport, r.Serializer, r.Payload)))
            {
                foreach (var row in g.OrderBy(r => r.NumberOfClients))
                {
                    if (row.Payload == PayloadMode.Primitive)
                    {
                        sb.AppendLine(
                            $"| {TransportLabel(row.Transport)} | PrimitiveSerializer | {PayloadLabel(row.Payload)} " +
                            $"| {row.NumberOfClients} | {row.ThroughputMsgPerSec:N0} | {row.TotalMessages:N0} " +
                            $"| {row.ElapsedMs:F2} |");
                    }
                    else
                    {
                        sb.AppendLine(
                            $"| {TransportLabel(row.Transport)} | {SerializerLabel(row.Serializer)} | {PayloadLabel(row.Payload)} " +
                            $"| {row.NumberOfClients} | {row.ThroughputMsgPerSec:N0} | {row.TotalMessages:N0} " +
                            $"| {row.ElapsedMs:F2} |");    
                    }
                }
            }

            return sb.ToString();
        }

        private class AllStartedActor : UntypedActor
        {
            public class AllStarted { }

            private readonly HashSet<IActorRef> _actors = new();
            private int _correlationId = 0;

            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case IActorRef a:
                        _actors.Add(a);
                        break;
                    case AllStarted a:
                        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                        var s = Sender;
                        var count = _actors.Count;
                        var c = _correlationId++;
                        var t = Task.WhenAll(_actors.Select(
                            x => x.Ask<ActorIdentity>(new Identify(c), cts.Token)));
                        t.ContinueWith(tr =>
                        {
                            return tr.Result.Length == count && tr.Result.All(x => x.MessageId.Equals(c));
                        }, TaskContinuationOptions.OnlyOnRanToCompletion).PipeTo(s);
                        break;
                }
            }
        }

        private class EchoActor : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                Sender.Tell(message);
            }
        }

        private class BenchmarkActor : UntypedActor
        {
            private readonly long _maxExpectedMessages;
            private readonly IActorRef _echo;
            private long _currentMessages = 0;
            private readonly TaskCompletionSource<long> _completion;

            public BenchmarkActor(long maxExpectedMessages, TaskCompletionSource<long> completion, IActorRef echo)
            {
                _maxExpectedMessages = maxExpectedMessages;
                _completion = completion;
                _echo = echo;
            }
            protected override void OnReceive(object message)
            {
                if (_currentMessages < _maxExpectedMessages)
                {
                    _currentMessages++;
                    _echo.Tell(message);
                }
                else
                {
                    _completion.TrySetResult(_maxExpectedMessages);
                }
            }
        }
    }
}
