using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessengerApp
{
    internal class Program
    {
        private class ConnectedUser
        {
            public TcpClient ClientSocket { get; set; }
            public string Nickname { get; set; }
            public string ConnectionId { get; set; }
            public IPAddress UserAddress { get; set; }
        }

        private static ConcurrentDictionary<string, ConnectedUser> _activeSessions = new();
        private static TcpListener _tcpAcceptor;
        private static CancellationTokenSource _shutdownSignal = new();
        private static int _tcpCommunicationPort;
        private static IPAddress _localHostAddress;

        private static string _currentUserName;
        private static IPAddress _clientLocalAddress;
        private static int _clientTcpPort;
        private static IPAddress _serverNetworkAddress;
        private static int _serverTcpEndpoint;
        private static TcpClient _tcpSession;
        private static NetworkStream _dataChannel;
        private static bool _isSessionActive;

        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.Title = "Chat";

            while (true)
            {
                Console.Clear();
                Console.WriteLine("+==================================================+");
                Console.WriteLine("|                      CHAT                         |");
                Console.WriteLine("+==================================================+");
                Console.WriteLine("|  [1]  Start server                               |");
                Console.WriteLine("|  [2]  Connect to server                          |");
                Console.WriteLine("|  [3]  Exit                                       |");
                Console.WriteLine("+==================================================+");
                Console.Write("> Select action: ");

                string selection = Console.ReadLine();

                if (selection == "1")
                {
                    await StartServerAsync();
                }
                else if (selection == "2")
                {
                    await StartClientAsync();
                }
                else if (selection == "3")
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("\n[!] Program terminated. Goodbye!");
                    Console.ResetColor();
                    break;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("[!] Invalid choice. Try again.");
                    Console.ResetColor();
                    await Task.Delay(1000);
                }
            }
        }
        private static async Task StartServerAsync()
        {
            Console.Clear();
            Console.WriteLine("+==================================================+");
            Console.WriteLine("|              SERVER CONFIGURATION               |");
            Console.WriteLine("+==================================================+\n");

            Console.Write("|- Server IP address: ");
            string ipString = Console.ReadLine();
            _localHostAddress = IPAddress.Parse(ipString);

            Console.Write("|- TCP port for messages: ");
            _tcpCommunicationPort = int.Parse(Console.ReadLine());

            if (!IsTcpPortAvailable(_tcpCommunicationPort))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\n+--------------------------------------------------+");
                Console.WriteLine($"|  ERROR! TCP port {_tcpCommunicationPort} is already in use. |");
                Console.WriteLine("+--------------------------------------------------+");
                Console.ResetColor();
                Console.WriteLine("\nPress any key to return to menu...");
                Console.ReadKey();
                return;
            }

            _tcpAcceptor = new TcpListener(_localHostAddress, _tcpCommunicationPort);
            _tcpAcceptor.Start();

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("\n+--------------------------------------------------+");
            Console.WriteLine("|  SERVER STARTED                                    |");
            Console.WriteLine($"|  Address: {_localHostAddress}:{_tcpCommunicationPort,-5}                             |");
            Console.WriteLine("|  Waiting for connections...                       |");
            Console.WriteLine("+--------------------------------------------------+");
            Console.ResetColor();
            Console.WriteLine();

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("\n+--------------------------------------------------+");
                Console.WriteLine("|  [!] Shutdown signal received                    |");
                Console.WriteLine("+--------------------------------------------------+");
                Console.ResetColor();
                _shutdownSignal.Cancel();
                _tcpAcceptor.Stop();
            };

            try
            {
                while (!_shutdownSignal.Token.IsCancellationRequested)
                {
                    TcpClient newClient = await _tcpAcceptor.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClientCommunication(newClient, _shutdownSignal.Token));
                }
            }
            catch (OperationCanceledException)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("\n+--------------------------------------------------+");
                Console.WriteLine("|  SERVER: operation stopped                       |");
                Console.WriteLine("+--------------------------------------------------+");
                Console.ResetColor();
            }
            finally
            {
                StopServer();
            }
        }

        private static async Task HandleClientCommunication(TcpClient client, CancellationToken token)
        {
            client.NoDelay = true;

            string clientIdentifier = client.Client.RemoteEndPoint.ToString();
            IPAddress clientNetworkAddress = ((IPEndPoint)client.Client.RemoteEndPoint).Address;

            NetworkStream dataStream = client.GetStream();
            byte[] receiveBuffer = new byte[4096];

            try
            {
                int bytesRead = await dataStream.ReadAsync(receiveBuffer, 0, receiveBuffer.Length, token);
                if (bytesRead == 0)
                {
                    client.Close();
                    return;
                }

                string username = Encoding.UTF8.GetString(receiveBuffer, 0, bytesRead);

                var userInfo = new ConnectedUser
                {
                    ClientSocket = client,
                    Nickname = username,
                    ConnectionId = clientIdentifier,
                    UserAddress = clientNetworkAddress
                };

                _activeSessions.TryAdd(clientIdentifier, userInfo);
                LogSystemEvent($"[+] Connected: {username} | Address: {clientNetworkAddress} | Total: {_activeSessions.Count}");

                var existingUsers = _activeSessions.Values
    .Where(u => u.ConnectionId != clientIdentifier)
    .Select(u => u.Nickname)
    .ToList();

                foreach (var existing in existingUsers)
                {
                    byte[] userInfoMessage = Encoding.UTF8.GetBytes($"[SYSTEM] {existing} is already in the chat");
                    await dataStream.WriteAsync(userInfoMessage, 0, userInfoMessage.Length);
                }

                await DistributeSystemMessage($"[SYSTEM] {username} joined the chat", clientIdentifier);

                while (!token.IsCancellationRequested)
                {
                    bytesRead = await dataStream.ReadAsync(receiveBuffer, 0, receiveBuffer.Length, token);
                    if (bytesRead == 0) break;

                    string incomingData = Encoding.UTF8.GetString(receiveBuffer, 0, bytesRead);

                    string messageContent;
                    if (incomingData.StartsWith(username + ": "))
                        messageContent = incomingData.Substring(username.Length + 2);
                    else
                        messageContent = incomingData;

                    string formattedMessage = $"{username}: {messageContent}";
                    LogMessageEvent($"{username}: {messageContent}");

                    await DistributeMessage(formattedMessage, clientIdentifier);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                LogSystemEvent($"[!] Error: {ex.Message}");
            }
            finally
            {
                if (_activeSessions.TryRemove(clientIdentifier, out var disconnectedUser))
                {
                    LogSystemEvent($"[-] Disconnected: {disconnectedUser.Nickname} | Remaining: {_activeSessions.Count}");
                    await DistributeSystemMessage($"[SYSTEM] {disconnectedUser.Nickname} left the chat", clientIdentifier);
                }
                client.Close();
            }
        }

        private static async Task DistributeMessage(string message, string excludeIdentifier)
        {
            byte[] messageData = Encoding.UTF8.GetBytes(message);
            var sendOperations = _activeSessions
                .Where(pair => pair.Key != excludeIdentifier)
                .Select(async pair =>
                {
                    try
                    {
                        pair.Value.ClientSocket.NoDelay = true;
                        await pair.Value.ClientSocket.GetStream().WriteAsync(messageData, 0, messageData.Length);
                    }
                    catch { }
                });

            await Task.WhenAll(sendOperations);
        }

        private static async Task DistributeSystemMessage(string systemMessage, string excludeIdentifier)
        {
            byte[] messageData = Encoding.UTF8.GetBytes(systemMessage);
            var sendOperations = _activeSessions
                .Where(pair => pair.Key != excludeIdentifier)
                .Select(async pair =>
                {
                    try
                    {
                        await pair.Value.ClientSocket.GetStream().WriteAsync(messageData, 0, messageData.Length);
                    }
                    catch { }
                });

            await Task.WhenAll(sendOperations);
        }

        private static bool IsTcpPortAvailable(int port)
        {
            IPGlobalProperties networkProperties = IPGlobalProperties.GetIPGlobalProperties();

            var activeConnections = networkProperties.GetActiveTcpConnections();
            if (activeConnections.Any(conn => conn.LocalEndPoint.Port == port))
                return false;

            var activeListeners = networkProperties.GetActiveTcpListeners();
            if (activeListeners.Any(endpoint => endpoint.Port == port))
                return false;

            return true;
        }

        private static void LogSystemEvent(string message)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write($"[{DateTime.Now:HH:mm:ss}] ");
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.Write("[SYSTEM]");
            Console.ResetColor();
            Console.WriteLine($" {message}");
        }

        private static void LogMessageEvent(string message)
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write($"[{DateTime.Now:HH:mm:ss}] ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("[MESSAGE]");
            Console.ResetColor();
            Console.WriteLine($" {message}");
        }

        private static void StopServer()
        {
            foreach (var user in _activeSessions.Values)
                user.ClientSocket?.Close();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("\n+--------------------------------------------------+");
            Console.WriteLine("|  [*] All connections closed                       |");
            Console.WriteLine("+--------------------------------------------------+");
            Console.ResetColor();
            Console.WriteLine("\nPress Enter to return to menu...");
            Console.ReadLine();
        }

        private static async Task StartClientAsync()
        {
            Console.Clear();
            Console.WriteLine("+==================================================+");
            Console.WriteLine("|           CONNECT TO SERVER                      |");
            Console.WriteLine("+==================================================+\n");

            Console.Write("|- Name: ");
            _currentUserName = Console.ReadLine();

            Console.Write("|- Local IP address: ");
            _clientLocalAddress = IPAddress.Parse(Console.ReadLine());

            Console.Write("|- Local TCP port (0 for auto): ");
            _clientTcpPort = int.Parse(Console.ReadLine());

            Console.Write("|- Server IP address: ");
            _serverNetworkAddress = IPAddress.Parse(Console.ReadLine());

            Console.Write("|- Server TCP port: ");
            _serverTcpEndpoint = int.Parse(Console.ReadLine());

            try
            {
                var localEndpoint = new IPEndPoint(_clientLocalAddress, _clientTcpPort);
                _tcpSession = new TcpClient(localEndpoint);

                _tcpSession.NoDelay = true;

                Console.Write("\n[*] Establishing connection");
                await _tcpSession.ConnectAsync(_serverNetworkAddress, _serverTcpEndpoint);
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(" [OK] Connection established");
                Console.ResetColor();

                _dataChannel = _tcpSession.GetStream();

                byte[] nameData = Encoding.UTF8.GetBytes(_currentUserName);
                await _dataChannel.WriteAsync(nameData, 0, nameData.Length);

                _isSessionActive = true;

                _ = Task.Run(ReceiveMessagesAsync);

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("\n+==================================================+");
                Console.WriteLine("|                 CHAT ACTIVE                      |");
                Console.WriteLine("+--------------------------------------------------+");
                Console.WriteLine("|  To exit chat type: /exit                       |");
                Console.WriteLine("+==================================================+");
                Console.ResetColor();
                Console.WriteLine();

                while (_isSessionActive)
                {
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.Write($"{_currentUserName}");
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.Write(" > ");
                    Console.ResetColor();
                    string userInput = Console.ReadLine();

                    if (string.IsNullOrWhiteSpace(userInput))
                        continue;

                    if (userInput.Equals("/exit", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("\n[*] Shutting down...");
                        Console.ResetColor();
                        break;
                    }

                    string messageToSend = $"{_currentUserName}: {userInput}";
                    byte[] messageBuffer = Encoding.UTF8.GetBytes(messageToSend);
                    await _dataChannel.WriteAsync(messageBuffer, 0, messageBuffer.Length);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n+--------------------------------------------------+");
                Console.WriteLine($"|  ERROR! {ex.Message,-34} |");
                Console.WriteLine($"+--------------------------------------------------+");
                Console.ResetColor();
            }
            finally
            {
                CloseClientConnections();
            }
        }

        private static async Task ReceiveMessagesAsync()
        {
            byte[] receiveBuffer = new byte[4096];
            try
            {
                while (_isSessionActive)
                {
                    int bytesReceived = await _dataChannel.ReadAsync(receiveBuffer, 0, receiveBuffer.Length);
                    if (bytesReceived == 0)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        ClearCurrentLine();
                        Console.WriteLine("\n+--------------------------------------------------+");
                        Console.WriteLine("|  [!] Connection to server lost                  |");
                        Console.WriteLine("+--------------------------------------------------+");
                        Console.ResetColor();
                        break;
                    }

                    string message = Encoding.UTF8.GetString(receiveBuffer, 0, bytesReceived);
                    ClearCurrentLine();
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.WriteLine($"\n[*] {message}");
                    Console.ResetColor();
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.Write($"{_currentUserName}");
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.Write(" > ");
                    Console.ResetColor();
                }
            }
            catch
            {
                if (_isSessionActive)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    ClearCurrentLine();
                    Console.WriteLine("\n+--------------------------------------------------+");
                    Console.WriteLine("|  [!] Connection to server lost                  |");
                    Console.WriteLine("+--------------------------------------------------+");
                    Console.ResetColor();
                }
            }
        }

        private static void ClearCurrentLine()
        {
            int originalLeft = Console.CursorLeft;
            int originalTop = Console.CursorTop;

            Console.SetCursorPosition(0, originalTop);
            Console.Write(new string(' ', Console.WindowWidth - 1));
            Console.SetCursorPosition(originalLeft, originalTop);
        }

        private static void CloseClientConnections()
        {
            _isSessionActive = false;
            _tcpSession?.Close();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("\n+--------------------------------------------------+");
            Console.WriteLine("|  [*] Connection closed                            |");
            Console.WriteLine("+--------------------------------------------------+");
            Console.ResetColor();
            Console.WriteLine("\nPress Enter to return to menu...");
            Console.ReadLine();
        }
    }
}