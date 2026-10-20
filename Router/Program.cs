using Router;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<RouterWorker>();

var host = builder.Build();
host.Run();