using Sidecar;
using Sidecar.Dns;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<DnsServer>(); 
builder.Services.AddHostedService(sp => sp.GetRequiredService<DnsServer>());
builder.Services.AddHostedService<SidecarWorker>();

var host = builder.Build();
host.Run();