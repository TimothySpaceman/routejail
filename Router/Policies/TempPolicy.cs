using Shared.Routing;

namespace Router.Policies;

public sealed class TempPolicy
{
    public static readonly Guid PeerA = Guid.Parse("00000000-0000-0000-0000-00000000000a");

    private static readonly IReadOnlyList<RouteRule> _rules =
    [
        new(Guid.Parse("00000000-0000-0000-0000-0000000000c1"), PeerA, "test.com", 9000, "127.0.0.1", 9000),
    ];
    
    public static RouteSnapshot SnapshotFor(Guid peerId)
    {
        return new RouteSnapshot(
            1,
            _rules.Where(r => r.SourcePeer == peerId).Select(
                r => new RouteSpec(r.Id, r.Name, r.Port)
            ).ToList()
        );
    }

    public static bool TryResolve(Guid peerId, Guid routeId, out RouteRule rule)
    {
        rule = _rules.FirstOrDefault(r => r.Id == routeId && r.SourcePeer == peerId)!;
        return rule is not null;
    }
}