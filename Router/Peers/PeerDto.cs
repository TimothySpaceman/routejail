namespace Router.Peers;

public record PeerStatusDto(
    bool ListenerUp,
    DateTimeOffset? ControlConnectedSince,
    long RouteSnapshotVersion,
    int LiveConnections
);

public record PeerDto(
    Guid Id,
    string Name,
    string SocketPath,
    bool Enabled,
    PeerStatusDto Status
)
{
    public bool ListenerUp => Status.ListenerUp;
    public DateTimeOffset? ControlConnectedSince => Status.ControlConnectedSince;
    public long RouteSnapshotVersion => Status.RouteSnapshotVersion;
    public int LiveConnections => Status.LiveConnections;
}

public record CreatePeerRequest(
    Guid? Id,
    string Name,
    string SocketPath,
    bool? Enabled = true
);

public record UpdatePeerRequest(
    string Name,
    string SocketPath,
    bool Enabled = true
);
