# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY Sidecar/Sidecar.csproj Sidecar/
RUN dotnet restore Sidecar/Sidecar.csproj

COPY Shared/ Shared/
COPY Sidecar/ Sidecar/
RUN dotnet publish Sidecar/Sidecar.csproj \
    -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

RUN mkdir -p /run/rjail && chown app:app /run/rjail
USER app