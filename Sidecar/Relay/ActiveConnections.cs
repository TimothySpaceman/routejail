namespace Sidecar.Relay;

public static class ActiveConnections
{
    private static int _count;

    public static int Count => Volatile.Read(ref _count);

    public static void Enter() => Interlocked.Increment(ref _count);

    public static void Exit() => Interlocked.Decrement(ref _count);
}