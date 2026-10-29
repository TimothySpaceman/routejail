FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Shared/Shared.csproj Shared/
COPY Router/Router.csproj Router/
RUN dotnet restore Router/Router.csproj

COPY Shared/ Shared/
COPY Router/ Router/
RUN dotnet publish Router/Router.csproj \
    -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

RUN mkdir -p /run/rjail && chown app:app /run/rjail
USER app