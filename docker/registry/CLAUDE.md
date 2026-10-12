# docker/registry

Each folder here pins an external image by digest; see the docker README (imported below) for how the registry works
and how to bump images.

When bumping registry images:

- Bump in logical groups (for example, all of the kafka images together), one branch per group, so a breakage can be
  attributed to a specific image.
- Don't run `./gradlew pullImage bumpImage` (or `scripts/update-requirements.sh`, which runs it) across every image at
  once; scope it to the group, e.g. `./gradlew docker-kafka:pullImage docker-kafka:bumpImage`.
- After `bumpImage`, restore the comments it removed from `gradle.properties`, and make the follow-up changes listed for
  that image. Then run the tasks listed for that image, with `--rerun` on any `*Run` task whose image changed.
- If `bumpImage` / `compareImage` misbehave (for example, "multiple digests"), get the index digest with
  `docker buildx imagetools inspect <imageName> --format '{{.Manifest.Digest}}'` and edit `gradle.properties` by hand.
- Prefer `imagetools inspect` over pulling just to check for updates; Docker Hub rate-limits anonymous pulls.

@../README.md
