using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Shared.Channels;
using Shared.Routing;
using Shared.Transport;
using Sidecar.Routing;
using System.Threading.Channels;

namespace Sidecar.Dns;

record ActiveRoute(RouteSpec Spec, IPAddress Ip, CancellationTokenSource Cts, Task Running);

class ConnectionEntry
{
    public Task Running = Task.CompletedTask;
}

class UdpFlow(Channel<byte[]> inbox)
{
    public Channel<byte[]> Inbox { get; } = inbox;
    public Task Running = Task.CompletedTask;
}

class NameRecord(IPAddress ip)
{
    public IPAddress Ip { get; } = ip;
    public int Refs;
}

public class DnsManager(
    RouteInbox inbox,
    DnsServer dns,
    Func<Guid, CancellationToken, Task<ITransport>> openData,
    ILogger log
)
{
    private const int MaxUdpFlows = 256;
    private readonly LoopbackAllocator _alloc = new();
    private readonly ConcurrentDictionary<Guid, ActiveRoute> _active = new();
    private readonly ConcurrentDictionary<string, NameRecord> _names = new(StringComparer.OrdinalIgnoreCase);

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var snap in inbox.ReadAllAsync(ct)) await ApplyAsync(snap, ct);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            dns.Clear();
            foreach (var r in _active.Values) await StopAsync(r);
            _active.Clear();
        }
    }

    private async Task ApplyAsync(RouteSnapshot snap, CancellationToken ct)
    {
        var wanted = snap.Routes.ToDictionary(r => r.Id);

        foreach (var (id, active) in _active.ToList())
        {
            if (wanted.TryGetValue(id, out var spec) && spec == active.Spec) continue;
            var freedIp = ReleaseName(active.Spec.Name);
            await StopAsync(active);
            if (freedIp is not null) _alloc.Return(freedIp);
            _active.Remove(id, out _);
        }

        foreach (var spec in wanted.Values)
        {
            if (!_active.ContainsKey(spec.Id)) Add(spec, ct);
        }
    }

    private void Add(RouteSpec spec, CancellationToken ct)
    {
        var isNew = !_names.TryGetValue(spec.Name, out var record);
        var ip = isNew ? _alloc.Rent() : record!.Ip;

        var udp = spec.Protocol == RouteProtocol.Udp;
        var listener = new Socket(
            AddressFamily.InterNetwork,
            udp ? SocketType.Dgram : SocketType.Stream,
            udp ? ProtocolType.Udp : ProtocolType.Tcp
        );
        try
        {
            listener.Bind(new IPEndPoint(ip, spec.Port));
            if (!udp) listener.Listen(64);
        }
        catch (SocketException ex)
        {
            listener.Dispose();
            if (isNew) _alloc.Return(ip);
            log.LogError(ex, "Cannot listen for route {Route}", spec.Name);
            return;
        }

        if (isNew) _names[spec.Name] = record = new NameRecord(ip);
        record!.Refs++;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var running = udp
            ? RunUdpListenerAsync(spec, listener, cts.Token)
            : RunTcpListenerAsync(spec, listener, cts.Token);
        _active[spec.Id] = new ActiveRoute(spec, ip, cts, running);

        if (isNew) dns.Publish(spec.Name, ip);
    }

    private IPAddress? ReleaseName(string name)
    {
        var record = _names[name];
        if (--record.Refs > 0) return null;

        dns.Unpublish(name);
        _names.Remove(name, out _);
        return record.Ip;
    }

    private async Task StopAsync(ActiveRoute route)
    {
        await route.Cts.CancelAsync();
        await route.Running;
        route.Cts.Dispose();
    }

    // TODO: a listener that dies on its own should wake the manager instead of staying in _active
    private async Task RunTcpListenerAsync(RouteSpec spec, Socket listener, CancellationToken ct)
    {
        using var l = listener;
        var connections = new ConcurrentDictionary<Guid, ConnectionEntry>();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var app = await listener.AcceptAsync(ct);
                var id = Guid.NewGuid();
                var entry = new ConnectionEntry();
                connections[id] = entry;
                entry.Running = HandleAppConnectionAsync(spec, app, () => connections.TryRemove(id, out _), ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Listener for {Route} failed", spec.Name);
        }
        finally
        {
            await Task.WhenAll(connections.Values.Select(c => c.Running));
        }
    }

    private async Task RunUdpListenerAsync(RouteSpec spec, Socket listener, CancellationToken ct)
    {
        using var l = listener;
        var flows = new ConcurrentDictionary<IPEndPoint, UdpFlow>();
        var buffer = new byte[65535];
        var any = new IPEndPoint(IPAddress.Any, 0);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                SocketReceiveFromResult r;
                try
                {
                    r = await listener.ReceiveFromAsync(buffer, SocketFlags.None, any, ct);
                }
                catch (SocketException e) when (e.SocketErrorCode == SocketError.ConnectionReset)
                {
                    continue; // ICMP unreachable from a vanished app
                }

                var remote = (IPEndPoint)r.RemoteEndPoint;
                if (!flows.TryGetValue(remote, out var flow))
                {
                    if (flows.Count >= MaxUdpFlows) continue;

                    flow = new UdpFlow(Channel.CreateBounded<byte[]>(
                        new BoundedChannelOptions(64)
                        {
                            FullMode = BoundedChannelFullMode.DropWrite
                        }
                    ));
                    flows[remote] = flow;
                    flow.Running = RunUdpFlowAsync(spec, listener, remote, flow.Inbox,
                        () => flows.TryRemove(remote, out _), ct
                    );
                }

                flow.Inbox.Writer.TryWrite(buffer[..r.ReceivedBytes]);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            log.LogError(ex, "UDP listener for {Route} failed", spec.Name);
        }
        finally
        {
            await Task.WhenAll(flows.Values.Select(f => f.Running));
        }
    }

    private async Task RunUdpFlowAsync(
        RouteSpec spec, Socket listener, IPEndPoint remote,
        Channel<byte[]> flowInbox, Action unregister, CancellationToken ct
    )
    {
        try
        {
            await using var data = await openData(spec.Id, ct);
            await using var app = DatagramTransport.Start(
                c => flowInbox.Reader.ReadAsync(c),
                async (d, c) =>
                {
                    try
                    {
                        await listener.SendToAsync(d.AsMemory(), SocketFlags.None, remote, c);
                    }
                    catch (SocketException)
                    {
                    }
                },
                DatagramTransport.DefaultIdle, ct
            );
            await using var channel = DataChannel.Start(app, data, ct);
            await channel.Transferring.WaitAsync(ct);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            log.LogWarning(ex, "UDP flow {Remote} for {Route} failed", remote, spec.Name);
        }
        finally
        {
            unregister();
        }
    }

    private async Task HandleAppConnectionAsync(
        RouteSpec spec,
        Socket appSocket,
        Action unregister,
        CancellationToken ct
    )
    {
        var app = new Transport(appSocket, true);
        try
        {
            await using var data = await openData(spec.Id, ct);
            await using var channel = DataChannel.Start(app, data, ct);
            await channel.Transferring.WaitAsync(ct);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            log.LogWarning(ex, "Proxy for {Route} failed", spec.Name);
            try
            {
                appSocket.LingerState = new LingerOption(true, 0);
            }
            catch
            {
                // ignored
            }
        }
        finally
        {
            unregister();
            try
            {
                await app.DisposeAsync();
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Transport dispose failed");
            }
        }
    }
}