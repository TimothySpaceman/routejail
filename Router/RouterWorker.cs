using System.Collections.Concurrent;
using System.Net.Sockets;
using Router.Connections;
using Router.Control;
using Router.Peers;
using Router.Routing;
using Shared.Channels;
using Shared.Control;
using Shared.Frames;
using Shared.Routing;
using Shared.Transport;

namespace Router;

record PeerListener(CancellationTokenSource Cts, Task Running, string Path);

public sealed class RouterWorker : BackgroundService, IPeerListenerManager
{
    private readonly ConcurrentDictionary<Guid, PeerListener> _listeners = new();
    private readonly PeerStore _peerStore;
    private readonly RouteStore _routeStore;
    private readonly ConnectionTable _connectionTable;
    private readonly ILogger<RouterWorker> _log;
    private CancellationTokenSource? _stoppingCts;

    public RouterWorker(
        PeerStore peerStore,
        RouteStore routeStore,
        ConnectionTable connectionTable,
        ILogger<RouterWorker> log
    )
    {
        _peerStore = peerStore;
        _routeStore = routeStore;
        _connectionTable = connectionTable;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        foreach (var peer in _peerStore.GetAll().Where(p => p.Enabled))
        {
            await StartListenerAsync(peer.Id, peer.SocketPath, stoppingToken);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }

        var stopTasks = _listeners.Keys.Select(StopListenerAsync);
        await Task.WhenAll(stopTasks);
    }

    public async Task StartListenerAsync(Guid peerId, string path, CancellationToken ct = default)
    {
        await StopListenerAsync(peerId);

        var effectiveToken = _stoppingCts != null
            ? CancellationTokenSource.CreateLinkedTokenSource(_stoppingCts.Token, ct).Token
            : ct;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(effectiveToken);

        try
        {
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            if (File.Exists(path)) File.Delete(path);

            var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            listener.Bind(new UnixDomainSocketEndPoint(path));
            listener.Listen(64);
            _log.LogInformation("Listening on {Path} for {Peer}", path, peerId);

            var runningTask = RunListenerLoopAsync(peerId, path, listener, cts);
            _listeners[peerId] = new PeerListener(cts, runningTask, path);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Failed to start listener on {Path} for peer {Peer}", path, peerId);
            cts.Dispose();
        }
    }

    public async Task StopListenerAsync(Guid peerId)
    {
        if (_listeners.TryRemove(peerId, out var listener))
        {
            await listener.Cts.CancelAsync();
            try
            {
                await listener.Running;
            }
            catch
            {
                // ignored
            }

            listener.Cts.Dispose();
        }

        if (_connectionTable.ControlChannels.TryRemove(peerId, out var controlEntry))
        {
            try
            {
                await controlEntry.Channel.DisposeAsync();
            }
            catch
            {
                // ignored
            }
        }

        foreach (var entry in _connectionTable.Connections.Values)
        {
            if (entry.PeerId == peerId || entry.DestinationPeerId == peerId)
            {
                entry.Kill();
            }
        }

        foreach (var pair in _connectionTable.PtpReservations)
        {
            if (pair.Value.DestinationPeerId == peerId || (
                    _connectionTable.Connections.TryGetValue(pair.Value.SourceConnectionId, out var src) &&
                    src.PeerId == peerId
                ))
            {
                _connectionTable.PtpReservations.TryRemove(pair.Key, out _);
                pair.Value.Tsc.TrySetCanceled();
            }
        }

        var peerConnections = _connectionTable.Connections.Values
            .Where(e => e.PeerId == peerId)
            .Select(e => e.Running)
            .ToList();

        if (peerConnections.Count > 0)
        {
            await Task.WhenAll(peerConnections);
        }
    }

    public bool IsListenerRunning(Guid peerId)
    {
        return _listeners.TryGetValue(peerId, out var listener) && !listener.Cts.IsCancellationRequested;
    }

    private async Task RunListenerLoopAsync(Guid peerId, string path, Socket listener, CancellationTokenSource cts)
    {
        var ct = cts.Token;
        try
        {
            using (listener)
            {
                while (!ct.IsCancellationRequested)
                {
                    var client = await listener.AcceptAsync(ct);
                    var id = Guid.NewGuid();
                    var entry = new ConnectionEntry(id, peerId);
                    _connectionTable.Connections[id] = entry;
                    entry.Running = HandleConnectionAsync(entry, client, ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Listener for {Peer} failed", peerId);
        }
        finally
        {
            try
            {
                File.Delete(path);
            }
            catch
            {
                // ignored
            }
        }
    }

    private async Task HandleConnectionAsync(ConnectionEntry entry, Socket socket, CancellationToken ct)
    {
        var peerId = entry.PeerId;
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
                    if (_connectionTable.ControlChannels.ContainsKey(peerId))
                    {
                        _log.LogWarning("Peer {Peer} already has a control connection established", peerId);
                        break;
                    }

                    await HandleControlConnectionAsync(peerId, transport, ct);
                    break;
                }

                case Frame.Open open:
                {
                    await HandleDataConnectionAsync(entry, open, transport, timeout, ct);
                    break;
                }

                case Frame.Accept accept:
                {
                    await HandlePtpConnectionAsync(entry, accept, transport, timeout, ct);
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Connection from {Peer} failed", peerId);
        }
        finally
        {
            _connectionTable.Connections.TryRemove(entry.Id, out _);
            try
            {
                await transport.DisposeAsync();
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Transport dispose failed");
            }
        }
    }

    private async Task HandleControlConnectionAsync(Guid peerId, ITransport transport, CancellationToken ct)
    {
        await using var control = ControlChannel<IRouterControlApi, ISidecarControlApi>.Start(
            transport,
            new RouterControlApi(peerId, _routeStore)
        );

        var entry = new ControlChannelEntry(control, DateTimeOffset.UtcNow);
        if (!_connectionTable.ControlChannels.TryAdd(peerId, entry)) return;
        try
        {
            await control.Completed.WaitAsync(ct);
        }
        finally
        {
            _connectionTable.ControlChannels.TryRemove(KeyValuePair.Create(peerId, entry));
        }
    }

    private async Task HandleDataConnectionAsync(
        ConnectionEntry entry,
        Frame.Open frame,
        ITransport transport,
        CancellationTokenSource timeout,
        CancellationToken ct
    )
    {
        if (!_connectionTable.TryGetControlChannel(entry.PeerId, out var controlEntry))
        {
            _log.LogError("Cannot establish Data Connection without Control Connection");
            return;
        }

        var control = controlEntry.Channel;

        if (!_routeStore.TryResolve(entry.PeerId, frame.RouteId, out var dest))
        {
            _log.LogWarning("Connection from {Peer} to {Route} is not allowed by current policy", entry.PeerId,
                frame.RouteId);
            return;
        }

        entry.RouteId = frame.RouteId;
        entry.RouteName = dest.Name;
        entry.Protocol = dest.Protocol;
        entry.Kind = dest.DestinationPeerId is not null ? ConnectionKind.Ptp : ConnectionKind.Direct;
        entry.DestinationPeerId = dest.DestinationPeerId;
        entry.TargetHost = dest.Host;
        entry.TargetPort = dest.TargetPort;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, control.Cts.Token, timeout.Token);
        entry.Cts = cts;

        if (dest.DestinationPeerId is not null)
        {
            timeout.CancelAfter(Timeout.InfiniteTimeSpan);
            if (!_connectionTable.TryGetControlChannel(dest.DestinationPeerId.Value, out var destControlEntry)) return;
            var destControl = destControlEntry.Channel;
            var id = Guid.NewGuid();
            var reservation = new PtpReservation(id, transport, entry.Id, dest.DestinationPeerId.Value);
            _connectionTable.PtpReservations[id] = reservation;
            try
            {
                if (!_routeStore.TryResolve(entry.PeerId, frame.RouteId, out var recheck1) || !IsMatch(dest, recheck1))
                {
                    _log.LogWarning("Route {Route} changed or deleted before PTP reservation", frame.RouteId);
                    return;
                }

                await destControl.Remote.AcceptDataConnection(id, dest.Host, dest.TargetPort, dest.Protocol, cts.Token);
                if (_connectionTable.PtpReservations.TryRemove(KeyValuePair.Create(id, reservation)))
                {
                    throw new IOException("Peer haven't accepted the data connection");
                }

                if (!_routeStore.TryResolve(entry.PeerId, frame.RouteId, out var recheck2) || !IsMatch(dest, recheck2))
                {
                    _log.LogWarning("Route {Route} changed or deleted before PTP transfer", frame.RouteId);
                    return;
                }

                await reservation.Tsc.Task.WaitAsync(cts.Token);
            }
            finally
            {
                _connectionTable.PtpReservations.TryRemove(KeyValuePair.Create(id, reservation));
            }
        }
        else if (dest.Protocol == RouteProtocol.Udp)
        {
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            await using var upstream = await DatagramTransport.ConnectUdpAsync(
                dest.Host,
                dest.TargetPort,
                DatagramTransport.DefaultIdle,
                cts.Token
            );
            timeout.CancelAfter(Timeout.InfiniteTimeSpan);

            if (!_routeStore.TryResolve(entry.PeerId, frame.RouteId, out var recheck) || !IsMatch(dest, recheck))
            {
                _log.LogWarning("Route {Route} changed or deleted before UDP transfer", frame.RouteId);
                return;
            }

            await using var channel = DataChannel.Start(transport, upstream, cts.Token);
            entry.Channel = channel;
            await channel.Transferring.WaitAsync(cts.Token);
        }
        else
        {
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            var outbound = new Socket(SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await outbound.ConnectAsync(dest.Host, dest.TargetPort, cts.Token);
            }
            catch
            {
                outbound.Dispose();
                throw;
            }

            timeout.CancelAfter(Timeout.InfiniteTimeSpan);
            await using var upstream = new Transport(outbound, true);

            if (!_routeStore.TryResolve(entry.PeerId, frame.RouteId, out var recheck) || !IsMatch(dest, recheck))
            {
                _log.LogWarning("Route {Route} changed or deleted before TCP transfer", frame.RouteId);
                return;
            }

            await using var channel = DataChannel.Start(transport, upstream, cts.Token);
            entry.Channel = channel;
            await channel.Transferring.WaitAsync(cts.Token);
            _log.LogInformation("Transfer task returned");
        }
    }

    private async Task HandlePtpConnectionAsync(
        ConnectionEntry entry,
        Frame.Accept frame,
        ITransport transport,
        CancellationTokenSource timeout,
        CancellationToken ct
    )
    {
        timeout.CancelAfter(Timeout.InfiniteTimeSpan);
        if (!_connectionTable.TryGetControlChannel(entry.PeerId, out var controlEntry))
        {
            _log.LogError("Cannot establish Data Connection without Control Connection");
            return;
        }

        var control = controlEntry.Channel;

        if (
            !_connectionTable.PtpReservations.TryGetValue(frame.ReservationId, out var reservation) ||
            entry.PeerId != reservation.DestinationPeerId
        )
        {
            _log.LogError("P2P reservation {Reservation} for peer {Peer} not found", frame.ReservationId, entry.PeerId);
            return;
        }

        if (!_connectionTable.PtpReservations.TryRemove(KeyValuePair.Create(frame.ReservationId, reservation))) return;

        if (!_connectionTable.Connections.TryGetValue(reservation.SourceConnectionId, out var sourceEntry))
        {
            _log.LogError("Source connection for reservation {Reservation} not found", frame.ReservationId);
            return;
        }

        entry.IsCounterpart = true;
        entry.Counterpart = sourceEntry;
        sourceEntry.Counterpart = entry;
        entry.RouteId = sourceEntry.RouteId;
        entry.RouteName = sourceEntry.RouteName;
        entry.Protocol = sourceEntry.Protocol;
        entry.Kind = ConnectionKind.Ptp;
        entry.DestinationPeerId = sourceEntry.DestinationPeerId;
        entry.TargetHost = sourceEntry.TargetHost;
        entry.TargetPort = sourceEntry.TargetPort;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, control.Cts.Token, timeout.Token);
        entry.Cts = cts;

        try
        {
            await using var channel = DataChannel.Start(reservation.SourceTransport, transport, cts.Token);
            entry.Channel = channel;
            sourceEntry.Channel = channel;

            await channel.Transferring.WaitAsync(cts.Token);
            reservation.Tsc.TrySetResult();
        }
        catch (OperationCanceledException)
        {
            reservation.Tsc.TrySetCanceled();
            throw;
        }
        catch (Exception ex)
        {
            reservation.Tsc.TrySetException(ex);
            throw;
        }
    }

    private static bool IsMatch(RouteRule a, RouteRule b) =>
        a.SourcePeerId == b.SourcePeerId &&
        a.Host == b.Host &&
        a.TargetPort == b.TargetPort &&
        a.Protocol == b.Protocol &&
        a.DestinationPeerId == b.DestinationPeerId;
}