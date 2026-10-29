using Router.Routing;
using Shared.Control;
using Shared.Routing;

namespace Router.Control;

public class RouterControlApi(Guid peerId, RouteStore routeStore) : IRouterControlApi
{
    public Task<RouteSnapshot> GetRoutes() => Task.FromResult(routeStore.SnapshotFor(peerId));
}
