using Shared.Routing;

namespace Shared.Control;

public interface IRouterControlApi
{
    Task<RouteSnapshot> GetRoutes();
}