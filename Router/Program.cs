using System.Text.Json;
using System.Text.Json.Serialization;
using Router;
using Router.Connections;
using Router.Peers;
using Router.Routing;

var builder = WebApplication.CreateBuilder(args);

var urls = builder.Configuration["ASPNETCORE_URLS"] ?? builder.Configuration["urls"];
if (string.IsNullOrEmpty(urls))
{
    var host = builder.Configuration["Api:Host"] ?? "0.0.0.0";
    var port = builder.Configuration.GetValue<int?>("Api:Port") ?? 5000;
    builder.WebHost.UseUrls($"http://{host}:{port}");
}

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: true));
});

builder.Services.AddOpenApi();

builder.Services.AddSingleton<PeerStore>();
builder.Services.AddSingleton<RouteStore>();
builder.Services.AddSingleton<ConnectionTable>();

builder.Services.AddSingleton<RouterWorker>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RouterWorker>());
builder.Services.AddSingleton<IPeerListenerManager>(sp => sp.GetRequiredService<RouterWorker>());

builder.Services.AddSingleton<IConnectionService, ConnectionService>();
builder.Services.AddSingleton<IPeerService, PeerService>();
builder.Services.AddSingleton<IRouteService, RouteService>();

var app = builder.Build();

app.MapOpenApi();
app.MapGet("/openapi.json", () => Results.Redirect("/openapi/v1.json")).ExcludeFromDescription();

app.MapPeerEndpoints();
app.MapRouteEndpoints();
app.MapConnectionEndpoints();

app.Run();
