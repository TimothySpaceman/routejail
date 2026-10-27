using Shared.Routing;

namespace Shared.Control;

public interface ISidecarControlApi
{
    Task ApplyRoutes(RouteSnapshot snapshot);
}