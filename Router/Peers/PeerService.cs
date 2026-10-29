using Router.Common;
using Router.Connections;
using Router.Routing;

namespace Router.Peers;

public interface IPeerService
{
    IReadOnlyList<PeerDto> GetAll();
    ServiceResult<PeerDto> GetById(Guid id);
    Task<ServiceResult<PeerDto>> CreateAsync(CreatePeerRequest request, CancellationToken ct = default);
    Task<ServiceResult<PeerDto>> UpdateAsync(Guid id, UpdatePeerRequest request, CancellationToken ct = default);
    Task<ServiceResult> DeleteAsync(Guid id, CancellationToken ct = default);
}

public sealed class PeerService : IPeerService
{
    private readonly PeerStore _peerStore;
    private readonly RouteStore _routeStore;
    private readonly ConnectionTable _connectionTable;
    private readonly IPeerListenerManager _listenerManager;

    public PeerService(
        PeerStore peerStore,
        RouteStore routeStore,
        ConnectionTable connectionTable,
        IPeerListenerManager listenerManager
    )
    {
        _peerStore = peerStore;
        _routeStore = routeStore;
        _connectionTable = connectionTable;
        _listenerManager = listenerManager;
    }

    public IReadOnlyList<PeerDto> GetAll()
    {
        return _peerStore.GetAll().Select(GetPeerDto).ToList();
    }

    public ServiceResult<PeerDto> GetById(Guid id)
    {
        if (!_peerStore.TryGet(id, out var peer))
        {
            return ServiceResult<PeerDto>.NotFound($"Peer {id} not found");
        }

        return ServiceResult<PeerDto>.Ok(GetPeerDto(peer));
    }

    public async Task<ServiceResult<PeerDto>> CreateAsync(CreatePeerRequest request, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            return ServiceResult<PeerDto>.Invalid("Peer name cannot be empty");
        }

        if (string.IsNullOrWhiteSpace(request.SocketPath))
        {
            return ServiceResult<PeerDto>.Invalid("Socket path cannot be empty");
        }

        var id = request.Id ?? Guid.NewGuid();
        if (_peerStore.TryGet(id, out _))
        {
            return ServiceResult<PeerDto>.Conflict($"Peer with id {id} already exists");
        }

        if (_peerStore.GetAll().Any(p => string.Equals(p.SocketPath, request.SocketPath, StringComparison.OrdinalIgnoreCase)))
        {
            return ServiceResult<PeerDto>.Conflict($"Peer with socket path '{request.SocketPath}' already exists");
        }

        var peer = new Peer(id, request.Name.Trim(), request.SocketPath.Trim(), request.Enabled ?? true);
        _peerStore.TryAdd(peer);

        if (peer.Enabled)
        {
            await _listenerManager.StartListenerAsync(peer.Id, peer.SocketPath, ct);
        }

        return ServiceResult<PeerDto>.Ok(GetPeerDto(peer));
    }

    public async Task<ServiceResult<PeerDto>> UpdateAsync(Guid id, UpdatePeerRequest request, CancellationToken ct = default)
    {
        if (!_peerStore.TryGet(id, out var existing))
        {
            return ServiceResult<PeerDto>.NotFound($"Peer {id} not found");
        }

        if (string.IsNullOrWhiteSpace(request.Name))
        {
            return ServiceResult<PeerDto>.Invalid("Peer name cannot be empty");
        }

        if (string.IsNullOrWhiteSpace(request.SocketPath))
        {
            return ServiceResult<PeerDto>.Invalid("Socket path cannot be empty");
        }

        if (_peerStore.GetAll().Any(p => p.Id != id && string.Equals(p.SocketPath, request.SocketPath, StringComparison.OrdinalIgnoreCase)))
        {
            return ServiceResult<PeerDto>.Conflict($"Peer with socket path '{request.SocketPath}' already exists");
        }

        var updated = new Peer(id, request.Name.Trim(), request.SocketPath.Trim(), request.Enabled);
        _peerStore.TryUpdate(updated);

        // Listener convergence:
        if (existing.Enabled && !updated.Enabled)
        {
            await _listenerManager.StopListenerAsync(id);
        }
        else if (!existing.Enabled && updated.Enabled)
        {
            await _listenerManager.StartListenerAsync(id, updated.SocketPath, ct);
        }
        else if (existing.Enabled && updated.Enabled)
        {
            if (!string.Equals(existing.SocketPath, updated.SocketPath, StringComparison.OrdinalIgnoreCase))
            {
                await _listenerManager.StopListenerAsync(id);
                await _listenerManager.StartListenerAsync(id, updated.SocketPath, ct);
            }
        }

        return ServiceResult<PeerDto>.Ok(GetPeerDto(updated));
    }

    public async Task<ServiceResult> DeleteAsync(Guid id, CancellationToken ct = default)
    {
        if (!_peerStore.TryGet(id, out var existing))
        {
            return ServiceResult.NotFound($"Peer {id} not found");
        }

        var referencingRules = _routeStore.GetAll()
            .Where(r => r.SourcePeerId == id || r.DestinationPeerId == id)
            .ToList();

        if (referencingRules.Count > 0)
        {
            return ServiceResult.Conflict($"Cannot delete peer {id} because it is referenced by {referencingRules.Count} route(s)");
        }

        _peerStore.TryRemove(id, out _);
        await _listenerManager.StopListenerAsync(id);

        return ServiceResult.Ok();
    }

    private PeerDto GetPeerDto(Peer peer)
    {
        var listenerUp = _listenerManager.IsListenerRunning(peer.Id);
        DateTimeOffset? controlConnectedSince = null;
        if (_connectionTable.TryGetControlChannel(peer.Id, out var controlEntry))
        {
            controlConnectedSince = controlEntry.ConnectedAt;
        }

        var routeSnapshotVersion = _routeStore.Version;

        var liveConnections = _connectionTable.Connections.Values
            .Count(e => e.PeerId == peer.Id && e.RouteId != null && !e.IsCounterpart);

        var status = new PeerStatusDto(
            listenerUp,
            controlConnectedSince,
            routeSnapshotVersion,
            liveConnections
        );

        return new PeerDto(
            peer.Id,
            peer.Name,
            peer.SocketPath,
            peer.Enabled,
            status
        );
    }
}
