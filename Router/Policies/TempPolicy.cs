using Shared.Routing;

namespace Router.Policies;

public sealed class TempPolicy
{
    public static readonly Guid PeerA = Guid.Parse("00000000-0000-0000-0000-00000000000a");
    public static readonly Guid PeerB = Guid.Parse("00000000-0000-0000-0000-00000000000b");

    private static readonly IReadOnlyList<RouteRule> _rules =
    [
        new(Guid.Parse("00000000-0000-0000-0000-0000000000c1"), PeerA, "google.com", 80, "142.250.130.100", 80),
        new(Guid.Parse("00000000-0000-0000-0000-0000000000c2"), PeerA, "sc.b.peer", 9000, "127.0.0.1", 5000, PeerB),
    ];
    
    public static RouteSnapshot SnapshotFor(Guid peerId)
    {
        return new RouteSnapshot(
            1,
            _rules.Where(r => r.SourcePeerId == peerId).Select(
                r => new RouteSpec(r.Id, r.Name, r.Port)
            ).ToList()
        );
    }

    public static bool TryResolve(Guid peerId, Guid routeId, out RouteRule rule)
    {
        rule = _rules.FirstOrDefault(r => r.Id == routeId && r.SourcePeerId == peerId)!;
        return rule is not null;
    }
}