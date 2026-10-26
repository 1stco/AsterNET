using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace AsterNET.Tests
{
    // A loopback AMI peer exercises the real socket, parser and public connection API.
    internal sealed class AmiServer : IDisposable
    {
        private readonly TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
        private readonly BlockingCollection<Peer> accepted = new BlockingCollection<Peer>();
        private readonly ConcurrentBag<Peer> peers = new ConcurrentBag<Peer>();
        private readonly Thread acceptThread;
        private volatile bool stopping;
        internal readonly string Version;
        internal int PingCount;

        internal AmiServer(string version = "20.9.0")
        {
            Version = version;
            listener.Start();
            acceptThread = new Thread(Accept) { IsBackground = true };
            acceptThread.Start();
        }

        internal int Port { get { return ((IPEndPoint)listener.LocalEndpoint).Port; } }
        internal bool HasPendingConnection { get { return accepted.Count != 0; } }

        internal Peer NextPeer()
        {
            Peer peer;
            if (!accepted.TryTake(out peer, 10000))
                throw new System.TimeoutException("AMI client did not connect to the loopback server.");
            return peer;
        }

        private void Accept()
        {
            try
            {
                while (!stopping)
                {
                    var peer = new Peer(this, listener.AcceptTcpClient());
                    peers.Add(peer);
                    peer.Start();
                    accepted.Add(peer);
                }
            }
            catch (SocketException) when (stopping) { }
            catch (ObjectDisposedException) when (stopping) { }
        }

        public void Dispose()
        {
            stopping = true;
            listener.Stop();
            foreach (var peer in peers)
                peer.Dispose();
            acceptThread.Join(2000);
            accepted.Dispose();
        }

        internal sealed class Peer : IDisposable
        {
            private readonly AmiServer server;
            private readonly TcpClient client;
            private readonly NetworkStream stream;
            private readonly object writeLock = new object();
            private readonly Thread actionThread;
            private volatile bool closed;
            internal Action<string> PingReceived;

            internal Peer(AmiServer server, TcpClient client)
            {
                this.server = server;
                this.client = client;
                client.NoDelay = true;
                client.SendBufferSize = 4096;
                stream = client.GetStream();
                actionThread = new Thread(ReadActions) { IsBackground = true };
            }

            internal void Start()
            {
                Send("Asterisk Call Manager/5.0\r\n");
                actionThread.Start();
            }

            internal void Send(string text)
            {
                Send(Encoding.UTF8.GetBytes(text));
            }

            internal void Send(byte[] bytes)
            {
                lock (writeLock)
                    stream.Write(bytes, 0, bytes.Length);
            }

            private void ReadActions()
            {
                try
                {
                    using (var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, true))
                    {
                        var action = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        string line;
                        while (!closed && (line = reader.ReadLine()) != null)
                        {
                            if (line.Length == 0)
                            {
                                if (action.Count > 0)
                                    Reply(action);
                                action.Clear();
                                continue;
                            }
                            int colon = line.IndexOf(':');
                            if (colon > 0)
                                action[line.Substring(0, colon)] = line.Substring(colon + 1).Trim();
                        }
                    }
                }
                catch (IOException) { }
                catch (ObjectDisposedException) { }
                catch (SocketException) { }
            }

            private void Reply(Dictionary<string, string> action)
            {
                string id;
                action.TryGetValue("ActionID", out id);
                string header = "ActionID: " + id + "\r\n";
                switch (action["Action"].ToLowerInvariant())
                {
                    case "challenge":
                        Send("Response: Success\r\n" + header + "Challenge: test-challenge\r\n\r\n");
                        break;
                    case "login":
                        Send("Response: Success\r\n" + header + "Message: Authentication accepted\r\n\r\n");
                        break;
                    case "command":
                        string command = action["Command"];
                        string output = command == "core show version"
                            ? "Asterisk " + server.Version : "first line\r\nOutput: second line";
                        Send("Response: Follows\r\n" + header + "Output: " + output + "\r\n"
                            + (server.Version.StartsWith("13.") ? "--END COMMAND--\r\n\r\n" : "\r\n"));
                        break;
                    case "status":
                        Send("Response: Success\r\n" + header + "Message: Channel status will follow\r\n\r\n"
                            + "Event: Status\r\n" + header + "Channel: PJSIP/test\r\n\r\n"
                            + "Event: StatusComplete\r\n" + header + "Items: 1\r\n\r\n");
                        break;
                    case "ping":
                        Interlocked.Increment(ref server.PingCount);
                        if (PingReceived != null)
                            PingReceived(id);
                        else
                            Send("Response: Success\r\n" + header + "Ping: Pong\r\n\r\n");
                        break;
                    default:
                        Send("Response: Success\r\n" + header + "\r\n");
                        break;
                }
            }

            internal void Close()
            {
                closed = true;
                client.Close();
            }

            public void Dispose()
            {
                Close();
                actionThread.Join(2000);
            }
        }
    }
}
