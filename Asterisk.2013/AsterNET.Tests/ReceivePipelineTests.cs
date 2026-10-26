using AsterNET.Manager;
using AsterNET.Manager.Action;
using AsterNET.Manager.Event;
using AsterNET.Manager.Response;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace AsterNET.Tests
{
    public class ReceivePipelineTests
    {
        private readonly ITestOutputHelper output;

        public ReceivePipelineTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void SlowSubscriberDoesNotBlockResponsesAndPreservesOrder()
        {
            using (var test = new ConnectionFixture())
            using (var entered = new ManualResetEventSlim())
            using (var release = new ManualResetEventSlim())
            {
                var received = new ConcurrentQueue<int>();
                test.Connection.Hangup += (sender, e) =>
                {
                    if (e.UniqueId == "0")
                    {
                        entered.Set();
                        release.Wait(TimeSpan.FromSeconds(20));
                    }
                    received.Enqueue(int.Parse(e.UniqueId));
                };
                try
                {
                    test.Peer.Send(Hangup(0));
                    Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                    test.Peer.Send(string.Concat(Enumerable.Range(1, 2000).Select(Hangup)));
                    WaitUntil(() => test.Connection.PendingEventCount >= 2000);
                    Assert.True(test.Connection.SendAction(new PingAction(), 2000).IsSuccess());
                    var response = test.Connection.SendEventGeneratingAction(new StatusAction { ActionId = "status-test" }, 2000);
                    Assert.True(response.Complete);
                    Assert.Collection(response.Events,
                        e => Assert.IsType<StatusEvent>(e),
                        e => Assert.IsType<StatusCompleteEvent>(e));
                    Assert.All(response.Events, e => Assert.Equal("status-test", e.ActionId));
                }
                finally
                {
                    release.Set();
                }
                WaitUntil(() => received.Count == 2001);
                Assert.Equal(Enumerable.Range(0, 2001), received.ToArray());
                WaitUntil(() => test.Connection.PendingEventCount == 0 && test.Connection.PendingReceiveBytes == 0);
            }
        }

        [Fact]
        public void SocketKeepsDrainingWhileObjectConstructionIsBlocked()
        {
            using (var test = new ConnectionFixture())
            {
                SlowParseEvent.Entered.Reset();
                SlowParseEvent.Release.Reset();
                test.Connection.RegisterUserEventClass(typeof(SlowParseEvent));
                int received = 0;
                test.Connection.Hangup += (sender, e) => Interlocked.Increment(ref received);
                try
                {
                    test.Peer.Send("Event: UserEvent\r\nUserEvent: SlowParse\r\n\r\n");
                    Assert.True(SlowParseEvent.Entered.Wait(TimeSpan.FromSeconds(5)));
                    const int count = 12000;
                    string frame = Hangup(1).Replace("\r\n\r\n", "\r\nPayload: " + new string('x', 512) + "\r\n\r\n");
                    byte[] burst = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(frame, count)));
                    var watch = Stopwatch.StartNew();
                    var write = Task.Factory.StartNew(() => test.Peer.Send(burst),
                        CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                    Assert.True(write.Wait(TimeSpan.FromSeconds(10)), "Server send stalled while the AMI parser was blocked.");
                    WaitUntil(() => test.Connection.PendingReceiveBytes >= burst.Length - 16384);
                    output.WriteLine("Buffered {0:N0} bytes before object construction; server wrote burst in {1} ms.",
                        test.Connection.PendingReceiveBytes, watch.ElapsedMilliseconds);
                    SlowParseEvent.Release.Set();
                    WaitUntil(() => Volatile.Read(ref received) == count, 20000);
                    WaitUntil(() => test.Connection.PendingReceiveBytes == 0);
                }
                finally
                {
                    SlowParseEvent.Release.Set();
                }
            }
        }

        [Fact]
        public void SubscriberCanSynchronouslySendAnEventGeneratingAction()
        {
            using (var test = new ConnectionFixture())
            using (var completed = new ManualResetEventSlim())
            {
                Exception failure = null;
                test.Connection.Hangup += (sender, e) =>
                {
                    try
                    {
                        var result = test.Connection.SendEventGeneratingAction(new StatusAction(), 2000);
                        Assert.True(result.Complete);
                        Assert.Equal(2, result.Events.Count);
                    }
                    catch (Exception ex) { failure = ex; }
                    finally { completed.Set(); }
                };
                test.Peer.Send(Hangup(1));
                Assert.True(completed.Wait(TimeSpan.FromSeconds(5)));
                Assert.Null(failure);
            }
        }

        [Fact]
        public void Utf8CharactersAndLongLinesSurviveFragmentedReads()
        {
            using (var test = new ConnectionFixture())
            using (var completed = new ManualResetEventSlim())
            {
                UserEvent received = null;
                test.Connection.UserEvents += (sender, e) => { received = e; completed.Set(); };
                byte[] prefix = Encoding.UTF8.GetBytes("Event: UserEvent\nUserEvent: Example\nName: سلام دنیا 👋\nPayload: ");
                foreach (byte value in prefix)
                {
                    test.Peer.Send(new[] { value });
                    Thread.Sleep(2);
                }
                string payload = new string('z', 50000) + "پایان";
                test.Peer.Send(payload + "\nDuplicate: one\nDuplicate: two\n\n");
                Assert.True(completed.Wait(TimeSpan.FromSeconds(5)));
                Assert.Equal("سلام دنیا 👋", received.Attributes["name"]);
                Assert.Equal(payload, received.Attributes["payload"]);
                Assert.Equal("one" + Environment.NewLine + "two", received.Attributes["duplicate"]);
            }
        }

        [Theory]
        [InlineData("13.38.3")]
        [InlineData("20.9.0")]
        public void CommandOutputFramingDoesNotConsumeTheFollowingEvent(string version)
        {
            using (var test = new ConnectionFixture(version))
            using (var received = new ManualResetEventSlim())
            {
                test.Connection.Hangup += (sender, e) => received.Set();
                var response = Assert.IsType<CommandResponse>(test.Connection.SendAction(new CommandAction("test command"), 2000));
                Assert.Equal(new[] { "first line", "second line" }, response.Result);
                test.Peer.Send(Hangup(1));
                Assert.True(received.Wait(TimeSpan.FromSeconds(5)));
            }
        }

        [Fact]
        public void ThrowingSubscriberDoesNotDisconnectOrStopLaterEvents()
        {
            using (var test = new ConnectionFixture())
            using (var received = new ManualResetEventSlim())
            {
                test.Connection.Hangup += (sender, e) =>
                {
                    if (e.UniqueId == "1")
                        throw new InvalidOperationException("Deliberately failing subscriber.");
                    received.Set();
                };
                test.Peer.Send(Hangup(1) + Hangup(2));
                Assert.True(received.Wait(TimeSpan.FromSeconds(5)));
                Assert.True(test.Connection.SendAction(new PingAction(), 2000).IsSuccess());
            }
        }

        [Fact]
        public void ConcurrentAsyncPingsAreMatchedToTheirOwnActionId()
        {
            using (var test = new ConnectionFixture())
            {
                var ids = new ConcurrentQueue<string>();
                test.Peer.PingReceived = id => ids.Enqueue(id);
                var first = test.Connection.SendActionAsync(new PingAction { ActionId = "first" });
                var second = test.Connection.SendActionAsync(new PingAction { ActionId = "second" });
                WaitUntil(() => ids.Count == 2);
                string firstId, secondId;
                Assert.True(ids.TryDequeue(out firstId));
                Assert.True(ids.TryDequeue(out secondId));
                test.Peer.Send("Response: Success\r\nActionID: " + firstId + "\r\nPing: Pong\r\n\r\n");
                Assert.True(first.Wait(TimeSpan.FromSeconds(2)));
                Assert.Equal("first", first.Result.ActionId);
                Assert.False(second.IsCompleted);
                // A late/duplicate reply to an already removed handler must not be
                // mistaken for an old AMI Pong without an ActionID.
                test.Peer.Send("Response: Success\r\nActionID: " + firstId + "\r\nPing: Pong\r\n\r\n");
                test.Connection.SendAction(new CommandAction("test command"), 2000);
                Assert.False(second.IsCompleted);
                test.Peer.Send("Response: Success\r\nActionID: " + secondId + "\r\nPing: Pong\r\n\r\n");
                Assert.True(second.Wait(TimeSpan.FromSeconds(2)));
                Assert.Equal("second", second.Result.ActionId);
            }
        }

        [Fact]
        public void SocketReadsDoNotDependOnAvailableThreadPoolWorkers()
        {
            using (var test = new ConnectionFixture())
            using (var release = new ManualResetEventSlim())
            using (var completed = new CountdownEvent(8))
            {
                int minWorkers, minIo, maxWorkers, maxIo;
                ThreadPool.GetMinThreads(out minWorkers, out minIo);
                ThreadPool.GetMaxThreads(out maxWorkers, out maxIo);
                test.Connection.UseASyncEvents = true;
                test.Connection.Hangup += (sender, e) =>
                {
                    release.Wait(TimeSpan.FromSeconds(15));
                    completed.Signal();
                };
                try
                {
                    Assert.True(ThreadPool.SetMinThreads(1, minIo));
                    Assert.True(ThreadPool.SetMaxThreads(4, maxIo));
                    test.Peer.Send(string.Concat(Enumerable.Range(0, 8).Select(Hangup)));
                    WaitUntil(() =>
                    {
                        int availableWorkers, availableIo;
                        ThreadPool.GetAvailableThreads(out availableWorkers, out availableIo);
                        return availableWorkers == 0;
                    }, 5000);
                    Assert.True(test.Connection.SendAction(new PingAction(), 2000).IsSuccess());
                }
                finally
                {
                    release.Set();
                    ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                    ThreadPool.SetMinThreads(minWorkers, minIo);
                    Assert.True(completed.Wait(TimeSpan.FromSeconds(10)));
                }
            }
        }

        [Fact]
        public void KeepAliveContinuesWhileSubscriberIsBlocked()
        {
            using (var test = new ConnectionFixture())
            using (var entered = new ManualResetEventSlim())
            using (var release = new ManualResetEventSlim())
            {
                test.Connection.Hangup += (sender, e) => { entered.Set(); release.Wait(TimeSpan.FromSeconds(10)); };
                try
                {
                    test.Peer.Send(Hangup(1));
                    Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                    test.Connection.PingInterval = 100;
                    WaitUntil(() => Volatile.Read(ref test.Server.PingCount) >= 3, 5000);
                    Assert.True(test.Connection.SendAction(new PingAction(), 2000).IsSuccess());
                }
                finally { release.Set(); }
            }
        }

        [Fact]
        public void EndOfStreamDrainsCompleteFramesBeforeReportingDisconnect()
        {
            using (var test = new ConnectionFixture())
            using (var disconnected = new ManualResetEventSlim())
            {
                int received = 0;
                int receivedAtDisconnect = -1;
                test.Connection.Hangup += (sender, e) => Interlocked.Increment(ref received);
                test.Connection.ConnectionState += (sender, e) =>
                {
                    if (e is DisconnectEvent)
                    {
                        receivedAtDisconnect = Volatile.Read(ref received);
                        disconnected.Set();
                    }
                };
                test.Peer.Send(string.Concat(Enumerable.Range(0, 1000).Select(Hangup)));
                test.Peer.Close();
                Assert.True(disconnected.Wait(TimeSpan.FromSeconds(5)));
                Assert.Equal(1000, receivedAtDisconnect);
                Assert.False(test.Connection.IsConnected());
            }
        }

        [Fact]
        public void ReconnectDiscardsIncompleteOldFrames()
        {
            using (var test = new ConnectionFixture(keepAlive: true))
            using (var reconnected = new ManualResetEventSlim())
            using (var received = new ManualResetEventSlim())
            {
                var events = new ConcurrentQueue<HangupEvent>();
                test.Connection.ConnectionState += (sender, e) =>
                {
                    if (e is ConnectEvent && e.Reconnect)
                        reconnected.Set();
                };
                test.Connection.Hangup += (sender, e) => { events.Enqueue(e); received.Set(); };
                test.Peer.Send("Event: Hangup\r\nChannel: PJSIP/old\r\nUniqueID: incomplete");
                test.Peer.Close();
                var nextPeer = test.Server.NextPeer();
                Assert.True(reconnected.Wait(TimeSpan.FromSeconds(5)));
                nextPeer.Send(Hangup(99));
                Assert.True(received.Wait(TimeSpan.FromSeconds(5)));
                Assert.Single(events);
                Assert.Equal("99", events.Single().UniqueId);
                Assert.Equal("PJSIP/test", events.Single().Channel);
                Assert.True(test.Connection.SendAction(new PingAction(), 2000).IsSuccess());
            }
        }

        [Fact]
        public void LogoffFromDisconnectCallbackCancelsPendingReconnect()
        {
            using (var test = new ConnectionFixture(keepAlive: true))
            using (var loggedOff = new ManualResetEventSlim())
            {
                test.Connection.ReconnectIntervalFast = 500;
                test.Connection.ConnectionState += (sender, e) =>
                {
                    if (e is DisconnectEvent && e.Reconnect)
                    {
                        // Let the parser enter its reconnect delay before cancelling.
                        Thread.Sleep(50);
                        test.Connection.Logoff();
                        loggedOff.Set();
                    }
                };
                test.Peer.Close();
                Assert.True(loggedOff.Wait(TimeSpan.FromSeconds(5)));
                Thread.Sleep(600);
                Assert.False(test.Server.HasPendingConnection);
                Assert.False(test.Connection.IsConnected());
            }
        }

        [Fact]
        public void LogoffClearsQueuedEventsAndNewLoginHasIndependentWorkers()
        {
            using (var test = new ConnectionFixture())
            using (var entered = new ManualResetEventSlim())
            using (var release = new ManualResetEventSlim())
            using (var received = new ManualResetEventSlim())
            {
                var ids = new ConcurrentQueue<string>();
                test.Connection.Hangup += (sender, e) =>
                {
                    if (e.UniqueId == "0") { entered.Set(); release.Wait(TimeSpan.FromSeconds(15)); }
                    ids.Enqueue(e.UniqueId);
                    if (e.UniqueId == "999") received.Set();
                };
                try
                {
                    test.Peer.Send(Hangup(0));
                    Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                    test.Peer.Send(string.Concat(Enumerable.Range(1, 100).Select(Hangup)));
                    WaitUntil(() => test.Connection.PendingEventCount == 100);
                    test.Connection.Logoff();
                    Assert.Equal(0, test.Connection.PendingEventCount);
                    Assert.Equal(0, test.Connection.PendingReceiveBytes);
                    test.Connection.Login(5000);
                    test.Server.NextPeer().Send(Hangup(999));
                    Assert.True(received.Wait(TimeSpan.FromSeconds(5)));
                }
                finally { release.Set(); }
                WaitUntil(() => ids.Count == 2);
                Assert.All(ids, id => Assert.True(id == "0" || id == "999"));
            }
        }

        private static string Hangup(int id)
        {
            return "Event: Hangup\r\nChannel: PJSIP/test\r\nUniqueID: " + id + "\r\nCause: 16\r\n\r\n";
        }

        private static void WaitUntil(Func<bool> condition, int milliseconds = 10000)
        {
            Assert.True(SpinWait.SpinUntil(condition, milliseconds), "Timed out waiting for the AMI receive pipeline.");
        }

        private sealed class ConnectionFixture : IDisposable
        {
            internal readonly AmiServer Server;
            internal readonly ManagerConnection Connection;
            internal readonly AmiServer.Peer Peer;

            internal ConnectionFixture(string version = "20.9.0", bool keepAlive = false)
            {
                Server = new AmiServer(version);
                Connection = new ManagerConnection("127.0.0.1", Server.Port, "test", "test", Encoding.UTF8)
                {
                    KeepAlive = keepAlive,
                    PingInterval = 0,
                    SleepTime = 1,
                    DefaultResponseTimeout = 3000,
                    ReconnectIntervalFast = 10,
                    ReconnectIntervalMax = 20
                };
                try
                {
                    Connection.Login(5000);
                    Peer = Server.NextPeer();
                }
                catch
                {
                    Connection.Logoff();
                    Server.Dispose();
                    throw;
                }
            }

            public void Dispose()
            {
                Connection.Logoff();
                Server.Dispose();
            }
        }
    }

    public sealed class SlowParseEvent : UserEvent
    {
        public static readonly ManualResetEventSlim Entered = new ManualResetEventSlim();
        public static readonly ManualResetEventSlim Release = new ManualResetEventSlim();

        public SlowParseEvent(ManagerConnection source) : base(source) { }

        public override Dictionary<string, string> ParseSpecial(Dictionary<string, string> attributes)
        {
            Entered.Set();
            if (!Release.Wait(TimeSpan.FromSeconds(30)))
                throw new System.TimeoutException("Test did not release the custom event parser.");
            return attributes;
        }
    }
}
