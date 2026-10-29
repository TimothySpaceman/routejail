using Router.Common;

namespace Router.Connections;

public static class ConnectionEndpoints
{
    public static void MapConnectionEndpoints(this IEndpointRouteBuilder app)
    {
        var conns = app.MapGroup("/connections").WithTags("Connections");
        conns.MapGet("", (IConnectionService svc) => Results.Ok(svc.GetAll()));
        conns.MapGet("/{id:guid}", (Guid id, IConnectionService svc) =>
            HttpResults.ToHttpResult(svc.GetById(id)));
        conns.MapDelete("/{id:guid}", (Guid id, IConnectionService svc) =>
            HttpResults.ToHttpResult(svc.Kill(id)));
    }
}
