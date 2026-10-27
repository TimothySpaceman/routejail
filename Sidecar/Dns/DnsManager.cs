using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Shared.Channels;
using Shared.Routing;
using Shared.Transport;
using Sidecar.Routing;

namespace Sidecar.Dns;

record ActiveRoute(RouteSpec Spec, IPAddress Ip, CancellationTokenSource Cts, Task Running);

class ConnectionEntry
{
    public Task Running = Task.CompletedTask;
}

public class DnsManager(
    RouteInbox inbox,
    DnsServer dns,
    Func<Guid, CancellationToken, Task<ITransport>> openData,
    ILogger log
)
{
    private readonly LoopbackAllocator _alloc = new();
    private readonly ConcurrentDictionary<Guid, ActiveRoute> _active = new();

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
            dns.Unpublish(active.Spec.Name);
            await StopAsync(active);
            _active.Remove(id, out _);
        }

        foreach (var spec in wanted.Values)
        {
            if (!_active.ContainsKey(spec.Id)) Add(spec, ct);
        }
    }

    private void Add(RouteSpec spec, CancellationToken ct)
    {
        var ip = _alloc.Rent();
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            listener.Bind(new IPEndPoint(ip, spec.Port));
            listener.Listen(64);
        }
        catch (SocketException ex)
        {
            listener.Dispose();
            _alloc.Return(ip);
            log.LogError(ex, "Cannot listen for route {Route}", spec.Name);
            return;
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var running = RunListenerAsync(spec, listener, cts.Token);
        _active[spec.Id] = new ActiveRoute(spec, ip, cts, running);
        dns.Publish(spec.Name, ip);
    }

    private async Task StopAsync(ActiveRoute route)
    {
        await route.Cts.CancelAsync();
        await route.Running;
        route.Cts.Dispose();
        _alloc.Return(route.Ip);
    }

    // TODO: a listener that dies on its own should wake the manager instead of staying in _active
    private async Task RunListenerAsync(RouteSpec spec, Socket listener, CancellationToken ct)
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