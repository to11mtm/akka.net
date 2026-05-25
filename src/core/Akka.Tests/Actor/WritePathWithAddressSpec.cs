//-----------------------------------------------------------------------
// <copyright file="WritePathWithAddressSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------
#nullable enable
using System.Buffers;
using Akka.Actor;
using FluentAssertions;
using Xunit;
namespace Akka.Tests.Actor;
/// <summary>
/// Byte-for-byte parity tests for ActorPath.WritePathWithAddress vs. the public
/// string overloads (ToStringWithAddress(Address) and ToSerializationFormatWithAddress).
/// </summary>
public class WritePathWithAddressSpec
{
    private static string WriteToString(ActorPath path, Address addr, bool includeUid)
    {
        var writer = new ArrayBufferWriter<char>(64);
        path.WritePathWithAddress(writer, addr, includeUid);
        return new string(writer.WrittenSpan);
    }
    private static string Expected(ActorPath path, Address addr, bool includeUid)
        => includeUid
            ? path.ToSerializationFormatWithAddress(addr)
            : path.ToStringWithAddress(addr);
    [Fact]
    public void Should_Match_ToStringWithAddress_For_RootLocalPath()
    {
        var addr = new Address("akka", "sys");
        var path = new RootActorPath(addr);
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_SingleChild()
    {
        var addr = new Address("akka", "sys");
        var path = new RootActorPath(addr) / "user";
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_RemotePathOwnedAddress()
    {
        var ownAddr   = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        var otherAddr = new Address("akka.tcp", "sys", "10.0.0.1",  9999);
        var path = new RootActorPath(ownAddr) / "user" / "a" / "b" / "c";
        WriteToString(path, otherAddr, false).Should().Be(Expected(path, otherAddr, false));
        WriteToString(path, otherAddr, false).Should().StartWith("akka.tcp://sys@127.0.0.1:1234/");
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_LocalPathWithRemoteArg()
    {
        var localAddr  = new Address("akka", "sys");
        var remoteAddr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        var path = new RootActorPath(localAddr) / "user" / "child";
        WriteToString(path, remoteAddr, false).Should().Be(Expected(path, remoteAddr, false));
        WriteToString(path, remoteAddr, false).Should().Be("akka.tcp://sys@127.0.0.1:1234/user/child");
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_WithUid()
    {
        var addr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        var path = (new RootActorPath(addr) / "user" / "child").WithUid(42L);
        WriteToString(path, addr, true).Should().Be(Expected(path, addr, true));
        WriteToString(path, addr, true).Should().EndWith("#42");
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_WithUid_NotIncludedWhenIncludeUidFalse()
    {
        var addr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        var path = (new RootActorPath(addr) / "user" / "child").WithUid(42L);
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
        WriteToString(path, addr, false).Should().NotContain("#");
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_ForIgnoreActorRefPath()
    {
        var ignoreAddr = new Address("akka", "all-systems");
        var ignorePath = new RootActorPath(ignoreAddr) / "Nobody";
        var bogusAddr = new Address("akka.tcp", "other", "1.2.3.4", 9);
        if (!IgnoreActorRef.IsIgnoreRefPath(ignorePath))
            return;
        WriteToString(ignorePath, bogusAddr, false).Should().Be(Expected(ignorePath, bogusAddr, false));
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_DeepPath()
    {
        var addr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        ActorPath path = new RootActorPath(addr);
        for (var i = 0; i < 32; i++) path = path / ("seg" + i);
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_VeryDeepPath()
    {
        var addr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        ActorPath path = new RootActorPath(addr);
        for (var i = 0; i < 128; i++) path = path / ("n" + i);
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
    }
    [Fact]
    public void Should_Match_ToStringWithAddress_For_AddressWithoutPort()
    {
        var addr = new Address("akka", "sys", "host", port: null);
        var path = new RootActorPath(addr) / "user";
        WriteToString(path, addr, false).Should().Be(Expected(path, addr, false));
        WriteToString(path, addr, false).Should().Be("akka://sys@host/user");
    }
    [Fact]
    public void Should_Match_ToSerializationFormat_WithAddress()
    {
        var addr = new Address("akka.tcp", "sys", "127.0.0.1", 1234);
        var path = (new RootActorPath(addr) / "user" / "deep" / "child").WithUid(123456789L);
        WriteToString(path, addr, true).Should().Be(path.ToSerializationFormatWithAddress(addr));
    }
}