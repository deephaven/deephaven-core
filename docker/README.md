### Bind mounts

This `docker/` directory serves the local docker development process by being a convenient place
for [bind mounts](https://docs.docker.com/storage/bind-mounts/). Developers may structure data
however they see fit in any subdirectories.

By convention though, the main [docker-compose.yml](../docker-compose.yml) file will use the
`docker/core/data/` directory as the bind mount for the `/data` volume and the
`docker/core/cache/` directory as the bind mount for the `/cache` volume with respect to the
`grpc-api` service.
