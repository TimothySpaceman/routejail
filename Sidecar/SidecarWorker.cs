using System.Net.Sockets;
using Shared.Channels;
using Shared.Control;
using Shared.Frames;
using Shared.Routing;
using Shared.Transport;
using Sidecar.Control;
using Sidecar.Dns;
using Sidecar.Routing;

namespace Sidecar;

public sealed class SidecarWorker : BackgroundService
{
    private readonly DnsServer _dns;
    private readonly ILogger<SidecarWorker> _log;
    private readonly string _socketPath;
    private Timer? _stats;

    public SidecarWorker(IConfiguration config, DnsServer dns, ILogger<SidecarWorker> log)
    {
        _socketPath = config["Sidecar:SocketPath"] ?? "/tmp/router.sock";
        _dns = dns;
        _log = log;
        
        var statsPath = config["Sidecar:StatsPath"];
        if (!string.IsNullOrEmpty(statsPath))
        {
            var proc = System.Diagnostics.Process.GetCurrentProcess();
            _stats = new Timer(_ =>
            {
                proc.Refresh();
                var t = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0).ToString("F3", System.Globalization.CultureInfo.InvariantCulture);
                File.AppendAllText(statsPath,
                    $"{{\"t\":{t},\"cpu_ns\":{(long)(proc.TotalProcessorTime.TotalMilliseconds * 1e6)},\"rss_kb\":{proc.WorkingSet64 / 1024},\"hwm_kb\":{proc.PeakWorkingSet64 / 1024},\"conns\":0}}\n");
            }, null, 0, 1000);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = TimeSpan.FromSeconds(1);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSessionAsync(stoppingToken);
                delay = TimeSpan.FromSeconds(1);
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
        await using var transport = new Transport(socket, false);

        FrameCodec.Write(transport.Output, new Frame.Connect());
        await transport.Output.FlushAsync(ct);

        var inbox = new RouteInbox(); // per session
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        await using var control = ControlChannel<ISidecarControlApi, IRouterControlApi>.Start(
            transport,
            new SidecarControlApi(
                inbox,
                (reservationId, host, port, protocol, rpcCt) => AcceptPtpChannelAsync(
                    reservationId, host, port, protocol,
                    CancellationTokenSource.CreateLinkedTokenSource(rpcCt, sessionCts.Token).Token
                )
            )
        );

        var reconciler = new DnsManager(inbox, _dns, OpenDataChannelAsync, _log);
        var reconciling = reconciler.RunAsync(sessionCts.Token);

        try
        {
            inbox.Submit(await control.Remote.GetRoutes());
            await Task.WhenAny(control.Completed, reconciling).WaitAsync(ct);
        }
        finally
        {
            await sessionCts.CancelAsync();
            await reconciling;
        }
    }

    private async Task<ITransport> OpenDataChannelAsync(Guid routeId, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(_socketPath), timeout.Token);
            timeout.CancelAfter(Timeout.InfiniteTimeSpan);

            var transport = new Transport(socket, true);
            FrameCodec.Write(transport.Output, new Frame.Open(routeId));
            await transport.Output.FlushAsync(ct);
            return transport;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task<ITransport> AcceptDataChannelAsync(Guid reservationId, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(_socketPath), timeout.Token);
            timeout.CancelAfter(Timeout.InfiniteTimeSpan);

            var transport = new Transport(socket, true);
            FrameCodec.Write(transport.Output, new Frame.Accept(reservationId));
            await transport.Output.FlushAsync(ct);
            return transport;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task AcceptPtpChannelAsync(
        Guid reservationId, string host, int port, RouteProtocol protocol,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));

        await using ITransport upstream = protocol == RouteProtocol.Udp
            ? await DatagramTransport.ConnectUdpAsync(host, port, DatagramTransport.DefaultIdle, ct)
            : await ConnectTcpAsync(host, port, timeout.Token);
        await using var downstream = await AcceptDataChannelAsync(reservationId, ct);

        await using var channel = DataChannel.Start(downstream, upstream, ct);
        await channel.Transferring.WaitAsync(ct);
    }

    private static async Task<ITransport> ConnectTcpAsync(string host, int port, CancellationToken ct)
    {
        var outbound = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await outbound.ConnectAsync(host, port, ct);
            outbound.NoDelay = true;
        }
        catch
        {
            outbound.Dispose();
            throw;
        }

        return new Transport(outbound, true);
    }
}