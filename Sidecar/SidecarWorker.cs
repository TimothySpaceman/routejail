using System.Net.Sockets;
using Shared.Control;
using Shared.Frames;
using Shared.Transport;
using Sidecar.Control;

namespace Sidecar;

public sealed class SidecarWorker : BackgroundService
{
    private readonly ILogger<SidecarWorker> _log;
    private readonly string _socketPath;

    public SidecarWorker(IConfiguration config, ILogger<SidecarWorker> log)
    {
        _socketPath = config["Sidecar:SocketPath"] ?? "/tmp/router.sock";
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = TimeSpan.FromSeconds(1);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSessionAsync(stoppingToken);
                delay = TimeSpan.FromSeconds(1); // it worked, reset backoff
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Router connection failed, retrying in {Delay}", delay);
            }

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 30));
        }
    }

    private async Task RunSessionAsync(CancellationToken ct)
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        await socket.ConnectAsync(new UnixDomainSocketEndPoint(_socketPath), ct);

        await using var transport = new SocketTransport(socket, false);

        FrameCodec.Write(transport.Output, new Frame.Connect());
        await transport.Output.FlushAsync(ct);

        await using var control = ControlChannel<ISidecarControlApi, IRouterControlApi>.Start(
            transport,
            new SidecarControlApi()
        );

        _log.LogInformation("Connected to router");

        var n = await control.Remote.GetRandomNumberFromRouter(ct);
        _log.LogInformation("Router says {N}", n);

        await control.Completed.WaitAsync(ct);
    }
}