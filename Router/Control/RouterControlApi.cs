using Router.Policies;
using Shared.Control;
using Shared.Routing;

namespace Router.Control;

public class RouterControlApi(Guid peerId) : IRouterControlApi
{
    public Task<RouteSnapshot> GetRoutes() => Task.FromResult(TempPolicy.SnapshotFor(peerId));
}