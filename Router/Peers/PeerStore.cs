using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Router.Peers;

public sealed class PeerStore
{
    private readonly ConcurrentDictionary<Guid, Peer> _peers = new();

    public bool TryAdd(Peer peer) => _peers.TryAdd(peer.Id, peer);

    public void Upsert(Peer peer) => _peers[peer.Id] = peer;

    public bool TryUpdate(Peer peer)
    {
        if (!_peers.ContainsKey(peer.Id)) return false;
        _peers[peer.Id] = peer;
        return true;
    }

    public bool TryRemove(Guid id, [NotNullWhen(true)] out Peer? peer) => _peers.TryRemove(id, out peer);

    public bool TryGet(Guid id, [NotNullWhen(true)] out Peer? peer) => _peers.TryGetValue(id, out peer);

    public IReadOnlyList<Peer> GetAll() => _peers.Values.ToList();
}
