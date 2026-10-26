using AsterNET.IO;
using AsterNET.Manager.Action;
using AsterNET.Manager.Event;
using AsterNET.Manager.Response;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace AsterNET.Manager
{
    /// <summary>
    /// Reads and buffers AMI data independently of protocol parsing and event subscribers.
    /// </summary>
    public class ManagerReader
    {
#if LOGGER
        private readonly Logger logger = Logger.Instance();
#endif
        private const int ReadBufferSize = 16 * 1024;
        private readonly ManagerConnection mrConnector;
        private readonly object socketLock = new object();
        private readonly object eventLock = new object();
        private readonly Queue<ManagerEvent> eventQueue = new Queue<ManagerEvent>();
        private SocketConnection mrSocket;
        private ReadSession readSession;
        private volatile bool die;
        private volatile bool is_logoff;

        /// <summary>
        /// Creates a reader for the supplied connection.
        /// </summary>
        /// <param name="connection">The connection receiving events and responses.</param>
        public ManagerReader(ManagerConnection connection)
        {
            mrConnector = connection;
        }

        internal SocketConnection Socket
        {
            set
            {
                ReadSession previousSession;
                SocketConnection previousSocket;
                lock (socketLock)
                {
                    if (ReferenceEquals(mrSocket, value))
                        return;
                    previousSocket = mrSocket;
                    previousSession = readSession;
                    mrSocket = value;
                    readSession = null;
                    Monitor.PulseAll(socketLock);
                }
                if (previousSession != null)
                    previousSession.Stop();
                else if (previousSocket != null)
                    previousSocket.Dispose();
            }
        }

        internal bool Die
        {
            get { return die; }
            set
            {
                die = value;
                if (value)
                {
                    Socket = null;
                    lock (socketLock)
                        Monitor.PulseAll(socketLock);
                    lock (eventLock)
                    {
                        eventQueue.Clear();
                        Monitor.PulseAll(eventLock);
                    }
                }
            }
        }

        internal bool IsLogoff
        {
            set { is_logoff = value; }
        }

        internal long PendingReceiveBytes
        {
            get
            {
                lock (socketLock)
                    return readSession == null ? 0 : readSession.PendingBytes;
            }
        }

        internal int PendingEventCount
        {
            get
            {
                lock (eventLock)
                    return eventQueue.Count;
            }
        }

        internal void EnqueueEvent(ManagerEvent e)
        {
            lock (eventLock)
            {
                if (die)
                    return;
                eventQueue.Enqueue(e);
                Monitor.Pulse(eventLock);
            }
        }

        private void DispatchEvents()
        {
            while (true)
            {
                ManagerEvent e;
                lock (eventLock)
                {
                    while (!die && eventQueue.Count == 0)
                        Monitor.Wait(eventLock);
                    if (die)
                        return;
                    e = eventQueue.Dequeue();
                }
                mrConnector.DispatchBufferedEvent(e);
            }
        }

        internal void Reinitialize()
        {
            ReadSession session;
            lock (socketLock)
            {
                if (die || mrSocket == null || readSession != null)
                    return;
                mrSocket.Initial = false;
                is_logoff = false;
                session = new ReadSession(mrSocket);
                readSession = session;
                Monitor.PulseAll(socketLock);
            }

            // Socket reads must not depend on ThreadPool threads occupied by callbacks.
            new Thread(() => Receive(session))
            {
                IsBackground = true,
                Name = "ManagerSocketReader-" + mrConnector.GetHashCode()
            }.Start();
        }

        private void Receive(ReadSession session)
        {
            try
            {
                var buffer = new byte[ReadBufferSize];
                while (!die && !session.Stopped)
                {
                    int count = session.Stream.Read(buffer, 0, buffer.Length);
                    if (count == 0)
                        break;
                    var received = new byte[count];
                    Buffer.BlockCopy(buffer, 0, received, 0, count);
                    session.Enqueue(received);
                }
            }
            catch (Exception ex)
            {
                if (!die && !session.Stopped)
                {
#if LOGGER
                    logger.Error("Read data error", ex);
#else
                    System.Diagnostics.Trace.TraceError("AMI read data error: {0}", ex);
#endif
                }
            }
            finally
            {
                session.Complete();
            }
        }

        /// <summary>
        /// Parses buffered data and completes action responses. Subscriber callbacks run
        /// on a separate worker so a callback may safely wait for an AMI response.
        /// </summary>
        internal void Run()
        {
            new Thread(DispatchEvents)
            {
                IsBackground = true,
                Name = "ManagerEventDispatcher-" + mrConnector.GetHashCode()
            }.Start();

            try
            {
                while (!die)
                {
                    ReadSession session;
                    lock (socketLock)
                    {
                        while (!die && readSession == null)
                            Monitor.Wait(socketLock);
                        session = readSession;
                    }
                    if (die)
                        break;

                    try
                    {
                        while (!die && IsCurrent(session))
                        {
                            string line;
                            if (session.TryReadLine(out line))
                            {
                                if (!IsCurrent(session))
                                    break;
                                session.LastPacketTime = DateTime.Now;
                                ProcessLine(session, line);
                            }
                            else if (session.Completed)
                            {
                                Disconnect(session);
                                break;
                            }
                            else
                                CheckPing(session);
                        }
                    }
                    catch (Exception ex)
                    {
#if LOGGER
                        logger.Error("Error processing AMI data", ex);
#else
                        System.Diagnostics.Trace.TraceError("Error processing AMI data: {0}", ex);
#endif
                        Disconnect(session);
                    }
                    finally
                    {
                        if (session.PingHandler != null)
                        {
                            mrConnector.RemoveResponseHandler(session.PingHandler);
                            session.PingHandler.Free();
                            session.PingHandler = null;
                        }
                    }
                }
            }
            finally
            {
                Die = true;
            }
        }

        private bool IsCurrent(ReadSession session)
        {
            lock (socketLock)
                return ReferenceEquals(readSession, session);
        }

        private void Disconnect(ReadSession session)
        {
            lock (socketLock)
            {
                if (die || !ReferenceEquals(readSession, session))
                    return;
                Socket = null;
            }
            if (!is_logoff)
                mrConnector.DispatchEvent(new DisconnectEvent(mrConnector));
        }

        private void CheckPing(ReadSession session)
        {
            if (die || !IsCurrent(session) || is_logoff || session.WaitingForIdentifier
                || mrConnector.PingInterval <= 0
                || session.LastPacketTime.AddMilliseconds(mrConnector.PingInterval) >= DateTime.Now)
                return;

            if (session.PingHandler != null)
            {
                bool missingPong = session.PingHandler.Response == null;
                mrConnector.RemoveResponseHandler(session.PingHandler);
                session.PingHandler.Free();
                session.PingHandler = null;
                if (missingPong)
                    Disconnect(session);
            }
            else
            {
                session.PingHandler = new ResponseHandler(new PingAction(), null);
                mrConnector.SendAction(session.PingHandler.Action, session.PingHandler);
            }
            session.LastPacketTime = DateTime.Now;
        }

        private void ProcessLine(ReadSession session, string line)
        {
#if LOGGER
            logger.Debug(line);
#endif
            var packet = session.Packet;
            var commandList = session.CommandList;
            if (session.ProcessingCommandResult)
            {
                if (IsEndOfCommand(line, commandList))
                {
                    var response = new CommandResponse();
                    Helper.SetAttributes(response, packet);
                    response.Result = GetTrimEndList(commandList);
                    session.ProcessingCommandResult = false;
                    packet.Clear();
                    mrConnector.DispatchResponse(response);
                }
                else if (line.StartsWith("Privilege: ", StringComparison.OrdinalIgnoreCase)
                    || line.StartsWith("ActionID: ", StringComparison.OrdinalIgnoreCase)
                    || line.StartsWith("Timestamp: ", StringComparison.OrdinalIgnoreCase)
                    || line.StartsWith("Server: ", StringComparison.OrdinalIgnoreCase))
                    Helper.AddKeyValue(packet, line);
                else
                {
                    if (line.StartsWith("Output:", StringComparison.OrdinalIgnoreCase))
                        line = line.Substring(7).TrimStart(' ');
                    commandList.Add(line);
                }
                return;
            }

            if (line.Length != 0)
            {
                if (session.WaitingForIdentifier && (line.StartsWith("Asterisk Call Manager")
                    || line.StartsWith("VoIP API/AMI")))
                {
                    session.WaitingForIdentifier = false;
                    mrConnector.DispatchEvent(new ConnectEvent(mrConnector) { ProtocolIdentifier = line });
                }
                else
                {
                    if (string.Equals(line, "Response: Follows", StringComparison.OrdinalIgnoreCase)
                        || line.EndsWith("command output follows", StringComparison.OrdinalIgnoreCase))
                    {
                        session.ProcessingCommandResult = true;
                        commandList.Clear();
                    }
                    Helper.AddKeyValue(packet, line);
                }
                return;
            }

            if (packet.ContainsKey("event"))
                mrConnector.DispatchEvent(packet);
            else if (packet.ContainsKey("response"))
                mrConnector.DispatchResponse(packet);
            packet.Clear();
        }

        private bool IsEndOfCommand(string line, List<string> commandList)
        {
            bool marker = string.Equals(line, "--END COMMAND--", StringComparison.OrdinalIgnoreCase);
            if (mrConnector.asteriskVersion.HasValue)
                return mrConnector.asteriskVersion.Value <= AsteriskVersion.ASTERISK_13
                    ? marker : commandList.Count > 0 && line.Length == 0;
            return marker || (commandList.Count > 0 && line.Length == 0);
        }

        private static List<string> GetTrimEndList(List<string> commandList)
        {
            var result = new List<string>(commandList);
            while (result.Count > 0 && string.IsNullOrEmpty(result[result.Count - 1]))
                result.RemoveAt(result.Count - 1);
            return result;
        }

        // Each socket owns its buffers and decoding state. Reads completing after a
        // reconnect cannot enqueue bytes into the new connection's queue.
        private sealed class ReadSession
        {
            private readonly SocketConnection socket;
            private readonly object queueLock = new object();
            private readonly Queue<byte[]> buffers = new Queue<byte[]>();
            private readonly Decoder decoder;
            private readonly char[] characters;
            private readonly StringBuilder partialLine = new StringBuilder();
            private int characterOffset;
            private int characterCount;
            private long pendingBytes;
            private bool completed;
            private volatile bool stopped;

            internal readonly NetworkStream Stream;
            internal readonly Dictionary<string, string> Packet = new Dictionary<string, string>();
            internal readonly List<string> CommandList = new List<string>();
            internal bool WaitingForIdentifier = true;
            internal bool ProcessingCommandResult;
            internal DateTime LastPacketTime = DateTime.Now;
            internal ResponseHandler PingHandler;

            internal ReadSession(SocketConnection socket)
            {
                this.socket = socket;
                Stream = socket.NetworkStream;
                decoder = socket.Encoding.GetDecoder();
                characters = new char[socket.Encoding.GetMaxCharCount(ReadBufferSize)];
            }

            internal bool Stopped { get { return stopped; } }

            internal long PendingBytes
            {
                get { lock (queueLock) return pendingBytes; }
            }

            internal bool Completed
            {
                get { lock (queueLock) return completed && buffers.Count == 0; }
            }

            internal void Enqueue(byte[] buffer)
            {
                lock (queueLock)
                {
                    if (stopped)
                        return;
                    buffers.Enqueue(buffer);
                    pendingBytes += buffer.Length;
                    Monitor.Pulse(queueLock);
                }
            }

            internal void Complete()
            {
                lock (queueLock)
                {
                    completed = true;
                    Monitor.PulseAll(queueLock);
                }
            }

            internal void Stop()
            {
                lock (queueLock)
                {
                    stopped = true;
                    completed = true;
                    buffers.Clear();
                    pendingBytes = 0;
                    Monitor.PulseAll(queueLock);
                }
                socket.Dispose();
            }

            internal bool TryReadLine(out string line)
            {
                line = null;
                while (!stopped)
                {
                    if (characterOffset < characterCount)
                    {
                        int end = Array.IndexOf(characters, '\n', characterOffset, characterCount - characterOffset);
                        if (end >= 0)
                        {
                            if (partialLine.Length == 0)
                                line = new string(characters, characterOffset, end - characterOffset).Trim();
                            else
                            {
                                partialLine.Append(characters, characterOffset, end - characterOffset);
                                line = partialLine.ToString().Trim();
                                partialLine.Length = 0;
                            }
                            characterOffset = end + 1;
                            return true;
                        }
                        partialLine.Append(characters, characterOffset, characterCount - characterOffset);
                        characterOffset = characterCount;
                    }

                    byte[] buffer;
                    lock (queueLock)
                    {
                        if (!stopped && !completed && buffers.Count == 0)
                            Monitor.Wait(queueLock, 100);
                        if (buffers.Count == 0)
                            return false;
                        buffer = buffers.Dequeue();
                        pendingBytes -= buffer.Length;
                    }
                    // No decoding, line splitting or user code holds the receive queue lock.
                    characterCount = decoder.GetChars(buffer, 0, buffer.Length, characters, 0, false);
                    characterOffset = 0;
                }
                return false;
            }
        }
    }
}
