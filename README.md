[![Build Status](https://travis-ci.org/AsterNET/AsterNET.svg?branch=master)](https://travis-ci.org/AsterNET/AsterNET)
[![NuGet](https://img.shields.io/nuget/v/AsterNET.svg)](https://www.nuget.org/packages/AsterNET)

AsterNET is an open source framework for Asterisk AMI and FastAGI. AsterNET allows you to talk to Asterisk AMI from any .NET application and create FastAGI applications in any .NET language.

## Components
AsterNET is made up of two key components, FastAGI and Manager Interface. Each allows you to interact with Asterisk in different ways. FastAGI allows you to control the flow of a call from another machine (a dedicated AGI server for example) and Manager Interface allows you to obtain state information and interact with Asterisk. 

## AMI receive buffering

Each connection uses a socket reader, a protocol parser, and an event dispatcher on
dedicated background threads. The socket reader only copies incoming bytes into a
FIFO queue. Decoding, line framing, and event construction happen in the parser,
outside the receive queue lock. A stateful decoder preserves characters split
across TCP reads. Partial messages belong to their socket and are discarded when
that socket is replaced.

Action responses and response events are completed by the parser before subscriber
delivery. With `UseASyncEvents = false` (the default), subscriber callbacks run on
one event worker in receive order. A slow callback can therefore wait for
`SendAction` or `SendEventGeneratingAction` without blocking incoming responses.
With `UseASyncEvents = true`, callbacks are scheduled concurrently on the
ThreadPool, as before. Callback exceptions are logged and later events continue.
Logoff discards queued callbacks; a callback already running may finish afterward.

`ManagerConnection.PendingReceiveBytes` reports raw bytes awaiting parsing;
`PendingEventCount` reports events awaiting subscriber delivery. They exclude the
buffer or callback currently being processed, and tasks already scheduled by
`UseASyncEvents`. Queues are in memory and unbounded to avoid dropping events or
blocking socket reads during bursts. Persistently increasing backlog requires
faster processing or fewer subscribed events; buffering alone cannot compensate
for sustained overload. Queued data is not durable across shutdown or reconnect.

The wire parser follows the [AMI message framing rules](https://docs.asterisk.org/Configuration/Interfaces/Asterisk-Manager-Interface-AMI/AMI-Command-Syntax/).
Loopback regression tests cover blocked parsing, slow or throwing subscribers,
ThreadPool starvation, action responses, command output, fragmented UTF-8, and
reconnect/logoff. Run them on the two supported runtime paths:

```text
dotnet test Asterisk.2013/AsterNET.Tests/AsterNET.Tests.csproj -c Release -f net472
dotnet test Asterisk.2013/AsterNET.Tests/AsterNET.Tests.csproj -c Release -f net10.0
```

## AsterNET on IRC
Join in the conversation on Freenode, [#asternet](https://webchat.freenode.net/?channels=asternet)

## How to Get AsterNET
- Download a release: https://github.com/AsterNET/AsterNET/releases

- Download the latest source code from GitHub project

- Download via NuGet:
> PM> Install-Package AsterNET

or by visiting: https://www.nuget.org/packages/AsterNET/

## Documentation
- [AsterNET Documentation](http://asternet.github.io/AsterNET)

- [Official Asterisk Documentation](https://wiki.asterisk.org/wiki/display/AST/Asterisk+14+Documentation)

## History
AsterNET is a fork of Asterisk.NET. Now we've reached release status and AsterNET version 1.0.0 we feel this is a good separation point from the original project.

## Special Thanks
http://www.zapappi.com and http://www.jetbrains.com/resharper/

We'd like to thank JetBrains for our community licenses of ReShaper. A very useful tool that's helping us to refactor the code and improve general development much quicker. If you'd like to try it yourself, visit http://www.jetbrains.com
