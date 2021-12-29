# docker

## docker-server-slim

The `docker-server-slim` project produces the image `deephaven/server-slim:local-build`.

## docker-server

The `docker-server` project produces the images:
* `deephaven/server:local-build`
* `deephaven/server-nltk:local-build`
* `deephaven/server-pytorch:local-build`
* `deephaven/server-sklearn:local-build`
* `deephaven/server-tensorflow:local-build`

When the base images for `docker-server` are updated, the requirements.txt files may need to be updated.
This can currently be done manually via:

```shell
docker run --rm ghcr.io/deephaven/server-base:latest pip freeze > docker/server/src/main/server/requirements.txt
docker run --rm ghcr.io/deephaven/nltk-base:latest pip freeze > docker/server/src/main/server-nltk/requirements.txt
docker run --rm ghcr.io/deephaven/pytorch-base:latest pip freeze > docker/server/src/main/server-pytorch/requirements.txt
docker run --rm ghcr.io/deephaven/sklearn-base:latest pip freeze > docker/server/src/main/server-sklearn/requirements.txt
docker run --rm ghcr.io/deephaven/tensorflow-base:latest pip freeze > docker/server/src/main/server-tensorflow/requirements.txt
```

## runtime-base

The `docker-runtime-base` project produces the image `deephaven/runtime-base:local-build`.

## Docker Registry

In an effort to use human-friendly names that express intent, and the desire to have more reproducible builds, the
docker registry contains an explicit mapping from human-friendly docker image names to docker repository digest IDs for
external images that the build depends on.

The [registry](registry/) contains a list of folders where each folder represents an external image that the build
depends on. Each folder is automatically registered as a gradle project with the name `docker-<folderName>`.

Each registry project will contain a `gradle.properties` file, and expresses the mapping via the properties
`deephaven.registry.imageName` (the human-friendly docker image name) and `deephaven.registry.imageId`
(the docker repository digest ID).

To see all of the build-relevant tasks for all docker registries, you may run:

```shell
./gradlew help -q --task tagLocalBuild
```

The `tagLocalBuild` tasks are meant to be depended on via `Docker.registryTask` or `Docker.registryFiles`.

To see all of the docker registry tasks associated with a specific project, you may run:

```shell
./gradlew docker-<folderName>:tasks -q --group "Docker Registry"
```

Note: this set of tasks contains "release management" tasks that aren't meant to be run during the normal build procedure.

### Release Management

The "release management" tasks are meant to aid in keeping the mappings up-to-date. At a minimum, they should be run
at the beginning of a new release cycle; but may be run as often as desired to stay more up-to-date with the repository
versions. Most developers will not need to run these tasks.

To compare the images against the what can be pulled from the repository, the following can be run:

```shell
./gradlew --continue pullImage compareImage
```

When the repository version does not match the source-controlled version, there will be a helpful error message
describing the steps to take to update the versions.

There is a [nightly image check workflow](/.github/workflows/nightly-image-check.yml) that will run and notify when the
repository version and source-controlled versions differ. To silence known out-of-date images from triggering every
workflow run, the `deephaven.registry.ignoreOutOfDate` gradle property may be set to `true`.

When updating an external image, it may be best practice to only update one external image at a time per PR.
This can usually be done automatically via:

```shell
./gradlew docker-<folderName>:bumpImage
```

### Caveats

When updating versions (either just the digest, or the human-friendly name and the digest), the developer should do
their due-diligence to understand what has changed, and to read the release notes, even when everything in CI passes.

The "release management" tasks, even though run automatically, aren't a fool-proof solution to keeping up-to-date. It
will still be up to the developers to manage the human-friendly names; and to choose an appropriately scoped
human-friendly names. Most docker library images follow the pattern where they publish against `<name>:<major>`,
`<name>:<major>.<minor>`, and `<name>:<major>.<minor>.<patch>`; or some variant on `<name>:latest`. When specifying the
full semver for example, the automatically checks may never trigger if the publisher treats them as "immutable". An
example of this is envoy - they publish "immutable", full semver tags without providing partial semver tags. Ie, they
publish `envoyproxy/envoy:v1.17.1` but not `envoyproxy/envoy:v1.17`, meaning it's on the developers to manually update
the mapping details when necessary.
