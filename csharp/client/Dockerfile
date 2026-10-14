# Builds the Dh_NetClientTests test suite. Built and run by the testCsharpClient task in
# build.gradle, which stages the build context (sources under csharp/client/, backplane
# .proto files under proto/) and provides the deephaven/dotnet* base images
# (see docker/registry/dotnet and docker/registry/dotnet-runtime-8).
FROM deephaven/dotnet:local-build
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1
ENV DOTNET_NOLOGO=1
# The SDK image only carries the net10.0 runtime; graft in the net8.0 runtime from the
# (digest-pinned) official runtime image so that both target frameworks of the test
# project can execute.
COPY --from=deephaven/dotnet-runtime-8:local-build /usr/share/dotnet/shared/Microsoft.NETCore.App /usr/share/dotnet/shared/Microsoft.NETCore.App/
WORKDIR /project
COPY . /project/
RUN set -eux; \
    mkdir /out; \
    dotnet restore /project/csharp/client/Dh_NetClientTests/Dh_NetClientTests.csproj; \
    dotnet build /project/csharp/client/Dh_NetClientTests/Dh_NetClientTests.csproj \
      -c Release --no-restore
