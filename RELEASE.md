# Release

This document is primarily targeted towards Deephaven release managers.
It is meant to contain the necessary instructions for executing a Deephaven release.

Please be sure to read and understand this document in full before proceeding with a release.
If you have any questions or concerns about any of the commands, please reach out.

## Artifacts

### deephaven-core jars
The deephaven-core jars are the most important artifacts.
They serve as the basis for the Deephaven server application and downstream consumers.
They are released to [Maven Central](https://repo1.maven.org/maven2/io/deephaven/).

### deephaven-core wheel
The deephaven-core wheel is the most important artifact for Python integration purposes.
It is released to [PyPi](https://pypi.org/project/deephaven-core/) and [GitHub releases](https://github.com/deephaven/deephaven-core/releases).

### deephaven-server wheel
The deephaven-server wheel is used to start the Deephaven server application from the context of python.
It is released to [PyPi](https://pypi.org/project/deephaven-server/).
Note: the deephaven-server wheel is large (roughly, the same size as the Deephaven server application), so it is _not_ re-attached to GitHub releases.

### Deephaven server application
The Deephaven server application is an important artifact for running the Deephaven server with the default classpath and configuration points.
It also serves as the basis for Deephaven server images.
It is released to [GitHub releases](https://github.com/deephaven/deephaven-core/releases) in both tar and zip formats.

### Deephaven server images
The Deephaven server images are important artifacts for easily running the Deephaven server in docker.
The images are released to the GitHub container registry `ghcr.io/`.
The image building logic is separated into the [deephaven/deephaven-server-docker](https://github.com/deephaven/deephaven-server-docker) repository.

The regular images are the `server` (includes Python environment) and `server-slim` (does not include Python environment).
* [ghcr.io/deephaven/server](https://github.com/deephaven/deephaven-core/pkgs/container/server)
* [ghcr.io/deephaven/server-slim](https://github.com/deephaven/deephaven-core/pkgs/container/server-slim)

The extended images are all extensions to the `server` image and include additional Python dependencies.
* [ghcr.io/deephaven/server-all-ai](https://github.com/deephaven/deephaven-core/pkgs/container/server-all-ai)
* [ghcr.io/deephaven/server-nltk](https://github.com/deephaven/deephaven-core/pkgs/container/server-nltk)
* [ghcr.io/deephaven/server-pytorch](https://github.com/deephaven/deephaven-core/pkgs/container/server-pytorch)
* [ghcr.io/deephaven/server-sklearn](https://github.com/deephaven/deephaven-core/pkgs/container/server-sklearn)
* [ghcr.io/deephaven/server-tensorflow](https://github.com/deephaven/deephaven-core/pkgs/container/server-tensorflow)

### Deephaven server SBOM
The Deephaven server software bill-of-materials is an experimental artifact that some consumers may want.
It may also be useful for release management purposes to ensure that no unexpected dependencies are change from release to release.
It is released to [GitHub releases](https://github.com/deephaven/deephaven-core/releases) in the [syft](https://github.com/anchore/syft) json format.

### Deephaven python client
The Deephaven python client wheels are released on PyPi as [`pydeephaven`](https://pypi.org/project/pydeephaven/) and
[`pydeephaven-ticking`](https://pypi.org/project/pydeephaven-ticking/).

### Deephaven go client
The Deephaven go client is released as a [Go package](https://pkg.go.dev/github.com/deephaven/deephaven-core/go).

### Deephaven API docs
API documentation is generated for Java, Python and C++ implemetations for Deephaven integration.
The artifacts are released to [GitHub releases](https://github.com/deephaven/deephaven-core/releases)
and are published as the following:
* [Java Client/Server API](https://deephaven.io/core/javadoc/)
* [Python Integration API](https://deephaven.io/core/pydoc/)
* [Python Client API](https://deephaven.io/core/client-api/python/)
* [C++ Client API](https://deephaven.io/core/client-api/cpp/)
* [C++ Examples](https://deephaven.io/core/client-api/cpp-examples/)
* [R Client API](https://deephaven.io/core/client-api/r/)
* [JavaScript/TypeScript Client API](https://deephaven.io/core/client-api/javascript/)

### Deephaven JS API types
The npm package `@deephaven/jsapi-types` allow TypeScript clients to use their IDE's autocomplete and compiler's typechecks.
It is released to [npm](https://www.npmjs.com/package/@deephaven/jsapi-types).

## Release process

The majority of the release procedure is controlled through the [publish-ci.yml workflow](./.github/workflows/publish-ci.yml).
It is kicked off by a push to a branch name that matches `release/v*`.
Please familiarize yourself with the steps in that workflow.

### 0. Poll Deephaven team

Ensure you are proceeding with a known release, and there aren't any blockers.

### 1. Repository prerequisites

These release notes assume that the Deephaven repository `git@github.com:deephaven/deephaven-core.git` is referenced as the remote named `upstream`.
Please ensure your local repository is setup as such, or that you replace any commands with the appropriately named remote:
```shell
$ git remote get-url upstream
git@github.com:deephaven/deephaven-core.git
```

### 2. Create a local release branch

Ensure you are up-to-date with `upstream/main`, or at the commit that you want to start a new release from.
If you are unsure what commit to start from, please ask.
Please double-check you are on the version you expect to be releasing.
The releases have so far proceeded with `release/vX.Y.Z`, where `X.Y.Z` is the version number (this isn't a technical requirement), please replace `X.Y.Z` with the appropriate version.
We also separate out the release branch from `upstream/main` with an empty commit (this isn't a technical requirement).

```shell
$ git fetch upstream
$ git checkout upstream/main
$ ./gradlew printVersion -PdeephavenBaseQualifier= -q
$ git checkout -b release/vX.Y.Z
$ git commit --allow-empty -m "Cut for X.Y.Z"
```

#### Procedure for patch releases

For patch releases, typically the branch will be based off of the previous release tag, and not `upstream/main`, and the necessary patch fixes can be cherry-picked from the `upstream/main` branch.
The patch release manager is also responsible for bumping the patch version numbers as appropriate (note comment block on the list of commands
below).

Here is an example going from `X.Y.0` to `X.Y.1`:

```shell
$ git fetch upstream
$ git checkout vX.Y.0
$ git checkout -b release/vX.Y.1
$ git cherry-pick <...>
#
# Edit files, updating from X.Y.0 to X.Y.1, and git add them.
#
# Look in the last section "Version bump in preparation of next release" for a list of
# files to update to the right version you are producing.
#
# See https://github.com/deephaven/deephaven-core/issues/3466 for future improvements to this process.
$ ...
$ git commit -m "Bump to X.Y.1"
$ git --no-pager log --oneline vX.Y.0..release/vX.Y.1
#
# Compare output to expected PR list for missing or extraneous PRs
```

It's also best practice to ensure that the cherry-picks compile, as there can sometimes be changes that cherry-pick cleanly, but don't compile.
`./gradlew quick` is recommended as a quick validation that things look ok.

### 3. Push to upstream

Triple-check things look correct, the release is a "GO", and then start the release process by pushing the release branch to upstream:

```shell
$ git show release/vX.Y.Z
$ git push -u upstream release/vX.Y.Z
```

Note: release branches are _not_ typically merged back into `main`.

### 4. Monitor release

The release will proceed with [GitHub Actions](https://github.com/deephaven/deephaven-core/actions/workflows/publish-ci.yml).
The specific action can be found based off of the name of the release branch: [?query=branch%3Arelease%2FvX.Y.Z](https://github.com/deephaven/deephaven-core/actions/workflows/publish-ci.yml?query=branch%3Arelease%2FvX.Y.Z).

The "Publish" step creates the artifacts and publishes the jars to a [Maven Central staging repository](https://s01.oss.sonatype.org).

The "Upload Artifacts" step uploads the Deephaven server application, the deephaven-core wheel, and the deephaven-server wheel as *temporary* GitHub action artifacts.

The "Publish deephaven-core to PyPi" uploads the deephaven-core wheel to [PyPi](https://pypi.org/project/deephaven-core/).
If this step fails, the deephaven-core wheel from the "Upload Artifacts" step can be uploaded manually.

The "Publish deephaven-server to PyPi" uploads the deephaven-server wheel to [PyPi](https://pypi.org/project/deephaven-server/).
If this step fails, the deephaven-server wheel from the "Upload Artifacts" step can be uploaded manually.

The "Publish pydeephaven to PyPi" uploads the pydeephaven wheel to [PyPi](https://pypi.org/project/pydeephaven/).
If this step fails, the pydeephaven wheel from the "Upload Artifacts" step can be uploaded manually.

The "Publish pydeephaven-ticking to PyPi" uploads the pydeephaven-ticking wheels to [PyPi](https://pypi.org/project/pydeephaven-ticking/).
If this step fails, the pydeephaven-ticking wheel from the "Upload Artifacts" step can be uploaded manually.

The "Publish @deephaven/jsapi-types to npmjs" uploads the TypeScript tarball to [NPM](https://www.npmjs.com/package/@deephaven/jsapi-types).
If this step fails, the deephaven-jsapi-types tarball from the "Upload Artifacts" step can be uploaded manually.

Once the workflow job is done, ensure all publication sources have the new artifacts.

### 5. Download artifacts

Once the full publish-ci.yml worflow is done, the release artifacts can be downloaded from the GitHub Artifacts (located in the "Summary" tab of the action).
Similarly, release artifacts can be downloaded from the docs-ci.yml workflow.
These are currently manual steps taken from the browser. (The artifacts will be uploaded in Step #9)

There is potential in the future for QA-ing these artifacts above and beyond the integration testing that CI provides, as the release is not set in stone yet.

### 6. Create SBOM

This step is optional, but encouraged.
If you are unable to use these tools, it is possible for somebody else to do this part.

The following tools are used: [syft](https://github.com/anchore/syft), [cyclonedx](https://github.com/CycloneDX/cyclonedx-cli).

```shell
syft server-jetty-X.Y.Z.tar -o json > server-jetty-X.Y.Z.tar.syft.json
```

Compare differences:
```shell
syft convert server-jetty-A.B.C.tar.syft.json -o cyclonedx-json=/tmp/A.B.C.cyclonedx.json
syft convert server-jetty-X.Y.Z.tar.syft.json -o cyclonedx-json=/tmp/X.Y.Z.cyclonedx.json
cyclonedx diff /tmp/A.B.C.cyclonedx.json /tmp/X.Y.Z.cyclonedx.json --component-versions
```

Please post the difference to the Deephaven team to ensure there are no unexpected new dependencies, removed dependencies, or updated dependencies.

### 7. Maven Central jars

The jars are put into a [Maven Central Repository Manager](https://s01.oss.sonatype.org) staging repository.
You'll need your own username and password to sign in (to ensure auditability).

Arguably, the Maven Central jars are the most important artifacts - once they are officially released from the staging repository, they are released "forever".
This is in contrast with PyPi where build numbers _could_ be incremented and docker where tags can always be re-written.

If any late-breaking issues are found during the release process, but the Maven Central jars have not been released from staging, the release process can theoretically be re-done.

When ready, the staging repository will need to be "Closed" and then "Released".
Once the staging repository has been "Released", there is no going back.

The jars will be visible after release at [https://repo1.maven.org/maven2/io/deephaven/](https://repo1.maven.org/maven2/io/deephaven/).
Sometimes it takes a little bit of time for the jars to appear.

### 8. Tag upstream

The `vX.Y.Z` tag is primarily meant for an immutable reference point in the future.
It does not kick off any additional jobs.
The release should only be tagged _after_ the Maven Central staging repository has been "Released".

```shell
$ git tag -a -m "[Release] X.Y.Z" vX.Y.Z release/vX.Y.Z
$ git show vX.Y.Z
$ git push upstream vX.Y.Z
```

### 9. GitHub release

Create a new [GitHub release](https://github.com/deephaven/deephaven-core/releases/new) and use the `vX.Y.Z` tag as reference.

The convention is to have the Release title of the form `vX.Y.Z` and to autogenerate the release notes in comparison to the previous release tag. Question: should we always generate release notes based off of the previous minor release, instead of patch? Our git release workflow suggests we may want to do it always minor to minor.

Upload the Deephaven server application, deephaven-core wheel, pydeephaven wheel, pydeephaven-ticking wheels, @deephaven/jsapi-types tarball, and SBOM artifacts. Also, upload the C++, Java, Python, R and TypeScript docs artifacts.
(These are the artifacts downloaded in Step #5)

Hit the GitHub "Publish release" button.

### 10. Deephaven go client

The go client release consists of simply tagging and pushing to upstream:

```shell
$ git tag -a -m "[Release] Deephaven Go Client X.Y.Z" go/vX.Y.Z release/vX.Y.Z 
$ git show go/vX.Y.Z
$ git push upstream go/vX.Y.Z
```

### 11. Deephaven.io release

Verify that [Reference API Docs](https://deephaven.io/core/docs/#reference-guides) point to the latest version and that
version X.Y.Z is present. _(In the case of a patch on an old release, this may not be the version just built.)_
- ex. https://deephaven.io/core/release/vX.Y.Z/javadoc
- ex. https://deephaven.io/core/javadoc

The (non-public) [deephaven.io](https://github.com/deephaven/deephaven.io) `next` branch needs to be merged into `main`.  Ping Margaret.

### 12. Deephaven images

Follow the release process as described at [deephaven-server-docker/RELEASE.md](https://github.com/deephaven/deephaven-server-docker/blob/main/RELEASE.md).

### 13. Let everybody know

Ping team, ping community, ping friends - the latest Deephaven has been released!

### 14. Clean-up

The release branches serve a purpose for kicking off CI jobs, but aren't special in other ways.
Sometime after a release, old release branches can be safely deleted.

### 15. Version bump in preparation of the next release.

Say we just did release `0.31.0`. The next expected release is `0.32.0`  We update the repository with a bump to all files that
mention the version explicitly. These files are listed below:

```
#
# Edit files for version change
#
gradle.properties
R/rdeephaven/DESCRIPTION
cpp-client/deephaven/CMakeLists.txt
```

This leaves the files "ready" for the next regular release, and also ensures any build done from
a developer for testing of latest is not confused with the code just released.

In the case of a patch release these would need to be updated to a different version, like from `0.31.0` to `0.31.1`.

## External dependencies

There are a few external dependencies that Deephaven manages, but they have separate release lifecycles that don't directly impact the release of the other artifacts mentioned earlier.
* [deephaven/barrage](https://github.com/deephaven/barrage)
* [deephaven/deephaven-csv](https://github.com/deephaven/deephaven-csv)
* [deephaven/web-client-ui](https://github.com/deephaven/web-client-ui)
* [jpy-consortium/jpy](https://github.com/jpy-consortium/jpy)

