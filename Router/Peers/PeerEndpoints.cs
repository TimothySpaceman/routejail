using Router.Common;

namespace Router.Peers;

public static class PeerEndpoints
{
    public static void MapPeerEndpoints(this IEndpointRouteBuilder app)
    {
        var peers = app.MapGroup("/peers").WithTags("Peers");
        peers.MapGet("", (IPeerService svc) => Results.Ok(svc.GetAll()));
        peers.MapGet("/{id:guid}", (Guid id, IPeerService svc) =>
            HttpResults.ToHttpResult(svc.GetById(id)));
        peers.MapPost("", async (CreatePeerRequest req, IPeerService svc, CancellationToken ct) =>
        {
            var res = await svc.CreateAsync(req, ct);
            return HttpResults.ToHttpResult(res, p => Results.Created($"/peers/{p.Id}", p));
        });
        peers.MapPut("/{id:guid}", async (Guid id, UpdatePeerRequest req, IPeerService svc, CancellationToken ct) =>
            HttpResults.ToHttpResult(await svc.UpdateAsync(id, req, ct)));
        peers.MapDelete("/{id:guid}", async (Guid id, IPeerService svc, CancellationToken ct) =>
            HttpResults.ToHttpResult(await svc.DeleteAsync(id, ct)));
    }
}
