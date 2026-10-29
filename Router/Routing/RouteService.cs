using Router.Common;
using Router.Connections;
using Router.Peers;
using Shared.Routing;

namespace Router.Routing;

public interface IRouteService
{
    IReadOnlyList<RouteRuleDto> GetAll(Guid? peerId = null);
    ServiceResult<RouteRuleDto> GetById(Guid id);
    Task<ServiceResult<RouteRuleDto>> CreateAsync(CreateRouteRuleRequest request, CancellationToken ct = default);
    Task<ServiceResult<RouteRuleDto>> UpdateAsync(Guid id, UpdateRouteRuleRequest request, CancellationToken ct = default);
    Task<ServiceResult> DeleteAsync(Guid id, CancellationToken ct = default);
}

public sealed class RouteService : IRouteService
{
    private readonly RouteStore _routeStore;
    private readonly PeerStore _peerStore;
    private readonly ConnectionTable _connectionTable;
    private readonly IConnectionService _connectionService;
    private readonly ILogger<RouteService> _log;

    public RouteService(
        RouteStore routeStore,
        PeerStore peerStore,
        ConnectionTable connectionTable,
        IConnectionService connectionService,
        ILogger<RouteService> log
    )
    {
        _routeStore = routeStore;
        _peerStore = peerStore;
        _connectionTable = connectionTable;
        _connectionService = connectionService;
        _log = log;
    }

    public IReadOnlyList<RouteRuleDto> GetAll(Guid? peerId = null)
    {
        var rules = peerId.HasValue ? _routeStore.GetByPeer(peerId.Value) : _routeStore.GetAll();
        return rules.Select(ToDto).ToList();
    }

    public ServiceResult<RouteRuleDto> GetById(Guid id)
    {
        if (!_routeStore.TryGet(id, out var rule))
        {
            return ServiceResult<RouteRuleDto>.NotFound($"Route {id} not found");
        }

        return ServiceResult<RouteRuleDto>.Ok(ToDto(rule));
    }

    public async Task<ServiceResult<RouteRuleDto>> CreateAsync(CreateRouteRuleRequest request, CancellationToken ct = default)
    {
        var id = request.Id ?? Guid.NewGuid();

        if (_routeStore.TryGet(id, out _))
        {
            return ServiceResult<RouteRuleDto>.Conflict($"Route with id {id} already exists");
        }

        var validationError = ValidateRuleInput(request.SourcePeerId, request.Name, request.Port, request.Host, request.TargetPort, request.DestinationPeerId);
        if (validationError != null)
        {
            return ServiceResult<RouteRuleDto>.Invalid(validationError);
        }

        if (IsDuplicateKey(id, request.SourcePeerId, request.Name, request.Port, request.Protocol))
        {
            return ServiceResult<RouteRuleDto>.Conflict("A route with the same source peer, name, port, and protocol already exists");
        }

        var rule = new RouteRule(
            id,
            request.SourcePeerId,
            request.Name.Trim(),
            request.Port,
            request.Host.Trim(),
            request.TargetPort,
            request.DestinationPeerId,
            request.Protocol
        );

        _routeStore.Add(rule);

        DropDisallowedConnections();
        await PushSnapshotAsync(rule.SourcePeerId, ct);

        return ServiceResult<RouteRuleDto>.Ok(ToDto(rule));
    }

    public async Task<ServiceResult<RouteRuleDto>> UpdateAsync(Guid id, UpdateRouteRuleRequest request, CancellationToken ct = default)
    {
        if (!_routeStore.TryGet(id, out var existing))
        {
            return ServiceResult<RouteRuleDto>.NotFound($"Route {id} not found");
        }

        var validationError = ValidateRuleInput(request.SourcePeerId, request.Name, request.Port, request.Host, request.TargetPort, request.DestinationPeerId);
        if (validationError != null)
        {
            return ServiceResult<RouteRuleDto>.Invalid(validationError);
        }

        if (IsDuplicateKey(id, request.SourcePeerId, request.Name, request.Port, request.Protocol))
        {
            return ServiceResult<RouteRuleDto>.Conflict("A route with the same source peer, name, port, and protocol already exists");
        }

        var updated = new RouteRule(
            id,
            request.SourcePeerId,
            request.Name.Trim(),
            request.Port,
            request.Host.Trim(),
            request.TargetPort,
            request.DestinationPeerId,
            request.Protocol
        );

        _routeStore.TryUpdate(updated);

        DropDisallowedConnections();

        await PushSnapshotAsync(existing.SourcePeerId, ct);
        if (existing.SourcePeerId != updated.SourcePeerId)
        {
            await PushSnapshotAsync(updated.SourcePeerId, ct);
        }

        return ServiceResult<RouteRuleDto>.Ok(ToDto(updated));
    }

    public async Task<ServiceResult> DeleteAsync(Guid id, CancellationToken ct = default)
    {
        if (!_routeStore.TryRemove(id, out var removed))
        {
            return ServiceResult.NotFound($"Route {id} not found");
        }

        DropDisallowedConnections();
        await PushSnapshotAsync(removed.SourcePeerId, ct);

        return ServiceResult.Ok();
    }

    private void DropDisallowedConnections()
    {
        foreach (var conn in _connectionTable.Connections.Values)
        {
            if (conn.RouteId == null || conn.IsCounterpart) continue;

            if (!_routeStore.TryResolve(conn.PeerId, conn.RouteId.Value, out var currentRule))
            {
                _connectionService.Kill(conn.Id);
                continue;
            }

            var allowed =
                conn.PeerId == currentRule.SourcePeerId &&
                string.Equals(conn.TargetHost, currentRule.Host, StringComparison.OrdinalIgnoreCase) &&
                conn.TargetPort == currentRule.TargetPort &&
                conn.Protocol == currentRule.Protocol &&
                conn.DestinationPeerId == currentRule.DestinationPeerId;

            if (!allowed)
            {
                _connectionService.Kill(conn.Id);
            }
        }
    }

    private async Task PushSnapshotAsync(Guid peerId, CancellationToken ct)
    {
        if (_connectionTable.TryGetControlChannel(peerId, out var controlEntry))
        {
            var snapshot = _routeStore.SnapshotFor(peerId);
            try
            {
                await controlEntry.Channel.Remote.ApplyRoutes(snapshot, ct);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Failed to push route snapshot to peer {PeerId}", peerId);
            }
        }
    }

    private string? ValidateRuleInput(
        Guid sourcePeerId,
        string name,
        int port,
        string host,
        int targetPort,
        Guid? destinationPeerId
    )
    {
        if (string.IsNullOrWhiteSpace(name)) return "Route name cannot be empty";
        if (string.IsNullOrWhiteSpace(host)) return "Route host cannot be empty";
        if (port <= 0 || port > 65535) return "Port must be between 1 and 65535";
        if (targetPort <= 0 || targetPort > 65535) return "TargetPort must be between 1 and 65535";
        if (!_peerStore.TryGet(sourcePeerId, out _)) return $"Source peer {sourcePeerId} does not exist";
        if (destinationPeerId.HasValue && !_peerStore.TryGet(destinationPeerId.Value, out _))
            return $"Destination peer {destinationPeerId.Value} does not exist";

        return null;
    }

    private bool IsDuplicateKey(Guid routeId, Guid sourcePeerId, string name, int port, RouteProtocol protocol)
    {
        return _routeStore.GetAll().Any(r =>
            r.Id != routeId &&
            r.SourcePeerId == sourcePeerId &&
            string.Equals(r.Name, name.Trim(), StringComparison.OrdinalIgnoreCase) &&
            r.Port == port &&
            r.Protocol == protocol);
    }

    private static RouteRuleDto ToDto(RouteRule r) => new(
        r.Id,
        r.SourcePeerId,
        r.Name,
        r.Port,
        r.Host,
        r.TargetPort,
        r.DestinationPeerId,
        r.Protocol
    );
}
