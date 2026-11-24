#!/bin/sh
# Starts the .NET driver built by `dotnet build -c Release conformance/drivers/dotnet`. Only this process gets the
# variant's environment (an agent's profiler variables), not the build.
set -e
D=$(cd "$(dirname "$0")" && pwd)
DLL=$(ls "$D"/bin/Release/net*/TrailbeamKitDriver.dll 2>/dev/null | head -1)
if [ -z "$DLL" ]; then
  echo "conformance/drivers/dotnet is not built: dotnet build -c Release conformance/drivers/dotnet" >&2
  exit 2
fi
DOTNET=dotnet
[ -n "$DOTNET_ROOT" ] && [ -x "$DOTNET_ROOT/dotnet" ] && DOTNET="$DOTNET_ROOT/dotnet"
exec "$DOTNET" "$DLL"
