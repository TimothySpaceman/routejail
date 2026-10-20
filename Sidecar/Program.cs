using Sidecar;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<SidecarWorker>();

var host = builder.Build();
host.Run();