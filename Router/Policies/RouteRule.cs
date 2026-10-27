namespace Router.Policies;

public sealed record RouteRule(Guid Id, Guid SourcePeer, string Name, int Port, string Host, int TargetPort);
