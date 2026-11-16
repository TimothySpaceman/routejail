using Router.Common;
using Router.Routing;
using Shared.Routing;

namespace Router.Connections;

public interface IConnectionService
{
    IReadOnlyList<ConnectionDto> GetAll();
    ServiceResult<ConnectionDto> GetById(Guid id);
    ServiceResult Kill(Guid id);
}

public sealed class ConnectionService : IConnectionService
{
    private readonly ConnectionTable _connectionTable;
    private readonly RouteStore _routeStore;

    public ConnectionService(ConnectionTable connectionTable, RouteStore routeStore)
    {
        _connectionTable = connectionTable;
        _routeStore = routeStore;
    }

    public IReadOnlyList<ConnectionDto> GetAll()
    {
        var result = new List<ConnectionDto>();
        foreach (var entry in _connectionTable.Connections.Values)
        {
            if (entry.RouteId == null || entry.IsCounterpart) continue;
            result.Add(ToDto(entry));
        }
        return result;
    }

    public ServiceResult<ConnectionDto> GetById(Guid id)
    {
        if (!_connectionTable.TryGetConnection(id, out var entry) || entry.RouteId == null)
        {
            return ServiceResult<ConnectionDto>.NotFound($"Connection {id} not found");
        }

        if (entry.IsCounterpart && entry.Counterpart != null)
        {
            return ServiceResult<ConnectionDto>.Ok(ToDto(entry.Counterpart));
        }

        return ServiceResult<ConnectionDto>.Ok(ToDto(entry));
    }

    public ServiceResult Kill(Guid id)
    {
        if (!_connectionTable.TryGetConnection(id, out var entry) || entry.RouteId == null)
        {
            return ServiceResult.NotFound($"Connection {id} not found");
        }

        entry.Kill();
        return ServiceResult.Ok();
    }

    private ConnectionDto ToDto(ConnectionEntry entry)
    {
        var routeName = entry.RouteName;
        if (routeName == null && entry.RouteId != null && _routeStore.TryResolve(entry.PeerId, entry.RouteId.Value, out var r))
        {
            routeName = r.Name;
        }

        var channel = entry.Channel;
        var bytesIn = channel?.BytesIn ?? 0;
        var bytesOut = channel?.BytesOut ?? 0;
        var lastActivity = channel?.LastActivity;

        return new ConnectionDto(
            entry.Id,
            entry.PeerId,
            entry.RouteId,
            routeName,
            entry.Protocol ?? RouteProtocol.Tcp,
            entry.Kind == ConnectionKind.Ptp ? "ptp" : "direct",
            entry.DestinationPeerId,
            entry.StartedAt,
            bytesIn,
            bytesOut,
            lastActivity
        );
    }
}
