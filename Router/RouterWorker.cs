using System.Collections.Concurrent;
using System.Net.Sockets;
using Router.Channels;
using Router.Control;
using Shared;
using Shared.Channels;
using Shared.Control;
using Shared.Frames;
using Shared.Transport;

namespace Router;

public sealed class RouterWorker : BackgroundService
{
    private readonly ConcurrentDictionary<Task, byte> _connections = new();
    private readonly ILogger<RouterWorker> _log;
    private readonly string _path;

    public RouterWorker(IConfiguration config, ILogger<RouterWorker> log)
    {
        _path = config["Router:SocketPath"] ?? "/tmp/router.sock";
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (File.Exists(_path)) File.Delete(_path);

        using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(_path));
        listener.Listen(16);
        _log.LogInformation("Listening on {Path}", _path);
        _ = Consumer.RunAsync(9000, stoppingToken); // TEMP

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var client = await listener.AcceptAsync(stoppingToken);

                var task = HandleConnectionAsync(client, stoppingToken);
                _connections[task] = 0;
                _ = task.ContinueWith(t => _connections.TryRemove(t, out _), TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await Task.WhenAll(_connections.Keys);
            File.Delete(_path);
        }
    }

    private static async Task HandleConnectionAsync(Socket socket, CancellationToken ct)
    {
        var transport = new Transport(socket, true);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            var firstFrame = await FrameReader.ReadFrameAsync(transport.Input, timeout.Token);

            switch (firstFrame)
            {
                case Frame.Connect:
                {
                    await using var control = ControlChannel<IRouterControlApi, ISidecarControlApi>.Start(
                        transport,
                        new RouterControlApi()
                    );

                    var sum = await control.Remote.GetSum(2, 3);
                    Console.WriteLine($"sidecar says 2+3={sum}");

                    await control.Completed;
                    break;
                }

                // case Frame.Open:
                //     throw new NotImplementedException();
                case Frame.Open open:
                {
                    timeout.CancelAfter(TimeSpan.FromSeconds(5)); // restart the clock for the outbound connect

                    var outbound = new Socket(SocketType.Stream, ProtocolType.Tcp);
                    try
                    {
                        await outbound.ConnectAsync(open.Host, open.Port, timeout.Token);
                    }
                    catch
                    {
                        outbound.Dispose();
                        throw;
                    }

                    timeout.CancelAfter(Timeout.InfiniteTimeSpan);

                    await using var upstream = new Transport(outbound, true);
                    var channel = DataChannel.Start(transport, upstream, ct);
                    await channel.Transferring;
                    break;
                }
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or InvalidDataException or OperationCanceledException)
        {
            // bad/slow/dropped slave
        }
        finally
        {
            await transport.DisposeAsync();
        }
    }
}