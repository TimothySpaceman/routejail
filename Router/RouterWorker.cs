using System.Collections.Concurrent;
using System.Net.Sockets;
using Router.Control;
using Router.Policies;
using Shared.Channels;
using Shared.Control;
using Shared.Frames;
using Shared.Routing;
using Shared.Transport;

namespace Router;

record PeerListener(CancellationTokenSource Cts, Task Running);

record ConnectionEntry(Guid Id, Guid PeerId)
{
    public Task Running = Task.CompletedTask;
    public volatile DataChannel? Channel;
}

record PtpReservation(Guid Id, ITransport SourceTransport, Guid SourceConnectionId, Guid DestinationPeerId)
{
    public TaskCompletionSource Tsc { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class RouterWorker : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, PeerListener> _listeners = new();

    private readonly ConcurrentDictionary<Guid, ConnectionEntry> _connections = new();

    private readonly ConcurrentDictionary<Guid, ControlChannel<IRouterControlApi, ISidecarControlApi>>
        _controlChannels = new();

    private readonly ConcurrentDictionary<Guid, PtpReservation> _ptpReservations = new();

    private readonly ILogger<RouterWorker> _log;

    public RouterWorker(IConfiguration config, ILogger<RouterWorker> log)
    {
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        (string, Guid)[] knownPeers =
        [
            ("/run/rjail/a.sock", TempPolicy.PeerA),
            ("/run/rjail/b.sock", TempPolicy.PeerB)
        ];
        foreach (var (path, id) in knownPeers)
        {
            StartListener(id, path, stoppingToken);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }

        await Task.WhenAll(_listeners.Values.Select(l => l.Running));
    }

    private void StartListener(Guid peerId, string path, CancellationToken ct)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _listeners[peerId] = new PeerListener(cts, RunListenerAsync(peerId, path, cts));
    }

    private async Task StopListenerAsync(Guid peerId)
    {
        if (!_listeners.TryRemove(peerId, out var listener)) return;
        await listener.Cts.CancelAsync();
        await listener.Running;
        listener.Cts.Dispose();
    }

    private async Task RunListenerAsync(Guid peerId, string path, CancellationTokenSource cts)
    {
        var ct = cts.Token;
        try
        {
            if (File.Exists(path)) File.Delete(path);
            using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            listener.Bind(new UnixDomainSocketEndPoint(path));
            listener.Listen(64);
            _log.LogInformation("Listening on {Path} for {Peer}", path, peerId);

            while (!cts.Token.IsCancellationRequested)
            {
                var client = await listener.AcceptAsync(ct);
                var id = Guid.NewGuid();
                var entry = new ConnectionEntry(id, peerId);
                _connections[id] = entry;
                entry.Running = HandleConnectionAsync(entry, client, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            await cts.CancelAsync();
            _log.LogError(ex, "Listener for {Peer} failed", peerId);
        }
        finally
        {
            await Task.WhenAll(
                _connections.Values.Where(e => e.PeerId == peerId).Select(e => e.Running)
            );
            File.Delete(path);
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
                    if (_controlChannels.ContainsKey(peerId))
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
            _connections.TryRemove(entry.Id, out _);
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
            new RouterControlApi(peerId)
        );

        if (!_controlChannels.TryAdd(peerId, control)) return;
        try
        {
            await control.Completed.WaitAsync(ct);
        }
        finally
        {
            _controlChannels.TryRemove(KeyValuePair.Create(peerId, control));
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
        if (!_controlChannels.TryGetValue(entry.PeerId, out var control))
        {
            _log.LogError("Cannot establish Data Connection without Control Connection");
            return;
        }

        if (!TempPolicy.TryResolve(entry.PeerId, frame.RouteId, out var dest))
        {
            _log.LogWarning("Connection from {Peer} to {Route} is not allowed by current policy", entry.PeerId,
                frame.RouteId);
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, control.Cts.Token, timeout.Token);

        if (dest.DestinationPeerId is not null)
        {
            timeout.CancelAfter(Timeout.InfiniteTimeSpan); // Don't figure out yet how to do it properly
            if (!_controlChannels.TryGetValue(dest.DestinationPeerId.Value, out var destControl)) return;
            var id = Guid.NewGuid();
            var reservation = new PtpReservation(id, transport, entry.Id, dest.DestinationPeerId.Value);
            _ptpReservations[id] = reservation;
            try
            {
                await destControl.Remote.AcceptDataConnection(id, dest.Host, dest.TargetPort, dest.Protocol, cts.Token);
                if (_ptpReservations.TryRemove(KeyValuePair.Create(id, reservation)))
                {
                    throw new IOException("Peer haven't accepted the data connection");
                }

                await reservation.Tsc.Task.WaitAsync(cts.Token);
            }
            finally
            {
                _ptpReservations.TryRemove(KeyValuePair.Create(id, reservation));
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
            await using var channel = DataChannel.Start(transport, upstream, cts.Token);
            entry.Channel = channel;
            await channel.Transferring.WaitAsync(cts.Token);
        }
        else
        {
            timeout.CancelAfter(TimeSpan.FromSeconds(5)); // restart the clock for the outbound connect
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
        timeout.CancelAfter(Timeout.InfiniteTimeSpan); // Don't figure out yet how to do it properly
        if (!_controlChannels.TryGetValue(entry.PeerId, out var control))
        {
            _log.LogError("Cannot establish Data Connection without Control Connection");
            return;
        }

        if (
            !_ptpReservations.TryGetValue(frame.ReservationId, out var reservation) ||
            entry.PeerId != reservation.DestinationPeerId
        )
        {
            _log.LogError("P2P reservation {Reservation} for peer {Peer} not found", frame.ReservationId, entry.PeerId);
            return;
        }

        if (!_ptpReservations.TryRemove(KeyValuePair.Create(frame.ReservationId, reservation))) return;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct, control.Cts.Token, timeout.Token);
            await using var channel = DataChannel.Start(reservation.SourceTransport, transport, cts.Token);
            entry.Channel = channel;
            if (_connections.TryGetValue(reservation.SourceConnectionId, out var sourceEntry))
            {
                sourceEntry.Channel = channel;
            }

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
}