using Router.Common;

namespace Router.Routing;

public static class RouteEndpoints
{
    public static void MapRouteEndpoints(this IEndpointRouteBuilder app)
    {
        var routes = app.MapGroup("/routes").WithTags("Routes");
        routes.MapGet("", (Guid? peer, IRouteService svc) => Results.Ok(svc.GetAll(peer)));
        routes.MapGet("/{id:guid}", (Guid id, IRouteService svc) =>
            HttpResults.ToHttpResult(svc.GetById(id)));
        routes.MapPost("", async (CreateRouteRuleRequest req, IRouteService svc, CancellationToken ct) =>
        {
            var res = await svc.CreateAsync(req, ct);
            return HttpResults.ToHttpResult(res, r => Results.Created($"/routes/{r.Id}", r));
        });
        routes.MapPut("/{id:guid}", async (Guid id, UpdateRouteRuleRequest req, IRouteService svc, CancellationToken ct) =>
            HttpResults.ToHttpResult(await svc.UpdateAsync(id, req, ct)));
        routes.MapDelete("/{id:guid}", async (Guid id, IRouteService svc, CancellationToken ct) =>
            HttpResults.ToHttpResult(await svc.DeleteAsync(id, ct)));
    }
}
