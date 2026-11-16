namespace Router.Peers;

public sealed record Peer(
    Guid Id,
    string Name,
    string SocketPath,
    bool Enabled = true
);
