# RouteJail
### A network isolation and traffic control utility for container sandboxes

## Build
```shell
dotnet publish Router  -c Release -r linux-x64 --self-contained -p:InvariantGlobalization=true -o ./dist/router
```
```shell
dotnet publish Sidecar -c Release -r linux-x64 --self-contained -p:PublishSingleFile=true \
  -p:IncludeNativeLibrariesForSelfExtract=true -p:InvariantGlobalization=true -o ./dist/sidecar
```

## Environment
```
DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0
DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1
```