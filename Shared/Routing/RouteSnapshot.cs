namespace Shared.Routing;

public sealed record RouteSnapshot(long Version, IReadOnlyList<RouteSpec> Routes);