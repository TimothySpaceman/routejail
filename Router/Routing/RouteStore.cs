using System.Diagnostics.CodeAnalysis;
using Shared.Routing;

namespace Router.Routing;

public sealed class RouteStore
{
    private readonly object _lock = new();
    private readonly Dictionary<Guid, RouteRule> _byId = new();
    private readonly Dictionary<(Guid PeerId, Guid RouteId), RouteRule> _byPeerAndRoute = new();
    private long _version = 1;

    public long Version
    {
        get
        {
            lock (_lock)
            {
                return _version;
            }
        }
    }

    public void Add(RouteRule rule)
    {
        lock (_lock)
        {
            _byId[rule.Id] = rule;
            _byPeerAndRoute[(rule.SourcePeerId, rule.Id)] = rule;
            _version++;
        }
    }

    public bool TryAdd(RouteRule rule)
    {
        lock (_lock)
        {
            if (_byId.ContainsKey(rule.Id)) return false;
            _byId[rule.Id] = rule;
            _byPeerAndRoute[(rule.SourcePeerId, rule.Id)] = rule;
            _version++;
            return true;
        }
    }

    public bool TryUpdate(RouteRule rule)
    {
        lock (_lock)
        {
            if (!_byId.TryGetValue(rule.Id, out var old)) return false;
            _byPeerAndRoute.Remove((old.SourcePeerId, old.Id));
            _byId[rule.Id] = rule;
            _byPeerAndRoute[(rule.SourcePeerId, rule.Id)] = rule;
            _version++;
            return true;
        }
    }

    public bool TryRemove(Guid id, [NotNullWhen(true)] out RouteRule? rule)
    {
        lock (_lock)
        {
            if (_byId.Remove(id, out rule))
            {
                _byPeerAndRoute.Remove((rule.SourcePeerId, rule.Id));
                _version++;
                return true;
            }
            rule = null;
            return false;
        }
    }

    public bool TryGet(Guid id, [NotNullWhen(true)] out RouteRule? rule)
    {
        lock (_lock)
        {
            return _byId.TryGetValue(id, out rule);
        }
    }

    public bool TryResolve(Guid peerId, Guid routeId, [NotNullWhen(true)] out RouteRule? rule)
    {
        lock (_lock)
        {
            return _byPeerAndRoute.TryGetValue((peerId, routeId), out rule);
        }
    }

    public RouteSnapshot SnapshotFor(Guid peerId)
    {
        lock (_lock)
        {
            var routes = _byId.Values
                .Where(r => r.SourcePeerId == peerId)
                .Select(r => new RouteSpec(r.Id, r.Name, r.Port, r.Protocol))
                .ToList();
            return new RouteSnapshot(_version, routes);
        }
    }

    public IReadOnlyList<RouteRule> GetAll()
    {
        lock (_lock)
        {
            return _byId.Values.ToList();
        }
    }

    public IReadOnlyList<RouteRule> GetByPeer(Guid peerId)
    {
        lock (_lock)
        {
            return _byId.Values.Where(r => r.SourcePeerId == peerId).ToList();
        }
    }
}
