# The Deephaven Core devcontainer

One configuration, used on macOS (Docker Desktop) and on Linux with rootful or rootless Docker.
Open it with VS Code's Dev Containers extension ("Reopen in Container"), the `devcontainer` CLI,
or `devc`. Nothing in here is host-specific; the Features detect what they need at build time.

## What is in this folder

| File | Purpose |
| --- | --- |
| `devcontainer.json` | The configuration. Base image plus Features for Java, Node, Python, git-lfs, coding-agent CLIs, git identity, and nested containers via podman. |
| `devcontainer-lock.json` | Pins every Feature to a digest, so a rebuild gets the same Features until someone updates it deliberately. |
| `initialize-command.sh` | Runs on the **host** before the container is created. Copies your git `user.name`/`user.email` into `~/.config/devc/gitconfig-identity`, which is bind-mounted read-only for the identity Feature. |
| `post-create.sh` | Runs once per container as the remote user: creates the Python venv that `remoteEnv` points at, and fixes ownership of the Gradle cache volume. |
| `post-start.sh` | Runs on every start: builds the Deephaven Python wheels (inside a nested container, via podman) and installs them into the venv, so `./gradlew server-jetty-app:run` works with no manual step. Idempotent; steady-state cost is one `pip show`. |
| `seccomp-podman.json` | The seccomp profile that lets podman run inside the devcontainer without granting it any capability. See below. |

## Nested containers: `docker` inside the devcontainer

This build runs several steps in containers: protobuf code generation, the web IDE build, the
Python wheels, and the client integration tests that start a server container. Inside the
devcontainer, `docker` is podman (the `podman-as-docker` Feature), and those containers are
**children of the devcontainer**, not siblings on your host daemon. Nothing here mounts your
host's Docker socket.

The Feature grants the devcontainer **no capability**. Two things make that possible, and both
are in this folder or the Feature:

- **`seccomp-podman.json`**, referenced from `runArgs`. Docker's default seccomp filter blocks the
  syscalls that create user namespaces and mount filesystems unless the container holds
  `CAP_SYS_ADMIN`. Podman needs exactly those, so this profile is Docker's own default profile,
  complete and unmodified, with **one rule added**: an unconditional allow for
  `unshare setns clone clone3 mount umount2 pivot_root mount_setattr open_tree open_tree_attr
  move_mount fsopen fsconfig fsmount fspick sethostname setdomainname keyctl`.
  The kernel still enforces its own rules on these: with no capability they only work inside a
  user namespace the process created itself, which is how rootless podman works on any ordinary
  Linux desktop. Everything else Docker's default blocks stays blocked.
- **`systempaths=unconfined`** (declared by the Feature, not here): without it the nested
  container runtime cannot mount its own `/proc`. Without `CAP_SYS_ADMIN` this mostly exposes
  read-only kernel information; the kernel's permission checks on `/proc/sys` still apply.

The file is long because a seccomp profile cannot say "the defaults plus X"; it has to list
everything. **Do not hand-edit it.** To regenerate: take the current upstream default from
`https://github.com/moby/profiles/blob/main/seccomp/default.json` (Apache-2.0) and prepend that
single rule. The canonical copy ships with the Feature at `features/podman-as-docker/seccomp-podman.json`
in the `devc-tools` repo. It has to live in this repo because the Docker CLI reads it on the
host when the container is created; a Feature cannot supply it.

`--device=/dev/net/tun` in `runArgs` gives nested containers their own private networks with
name resolution between them, which the `deephaven-in-docker` Gradle tests rely on.

## Who you are inside the container

| Host | `id` inside | Why |
| --- | --- | --- |
| macOS Docker Desktop | `uid=1000(vscode)` | Ordinary user, as always. |
| Linux, rootful Docker | `uid=<your uid>(vscode)` | The devcontainer CLI's usual uid alignment. |
| Linux, **rootless** Docker | `uid=0(vscode)`, `HOME=/home/vscode` | See below. |

On a rootless daemon the workspace bind mount is owned by container uid 0 (the daemon maps your
host uid to container root), so a normal user inside cannot write a single project file. The
`rootless-remap` Feature detects that at build time and makes `vscode` uid 0 while keeping its
name and home. `sudo` becomes a no-op and tools that refuse to run as root will complain, but
nothing on the host changes: container root there *is* your own unprivileged user, so files
come out owned by you. On every other host the Feature does nothing.

## What this opens, in plain terms

Compared to a plain devcontainer, code running in here can additionally create user namespaces
and use mount syscalls inside them, which is what any unprivileged user on a stock Linux desktop
can do. That is a somewhat larger kernel attack surface than a default devcontainer, because
unprivileged user namespaces have been the entry point for kernel privilege-escalation bugs in
the past. It is not a capability grant: escaping the container still requires a kernel bug, not
a known technique. The blast radius of any escape is unchanged by this folder: your user account
on rootless Linux, the Docker Desktop VM (your shared folders and other containers) on macOS.

Two alternatives were rejected for being worse: `docker-in-docker` needs `--privileged`, and
`docker-outside-of-docker` hands the devcontainer control of your host's Docker daemon with no
escape needed.

For the measurements behind all of this, see the `devc-tools` repo: `features/podman-as-docker/README.md`
and `docs/manual-verification.md` § 13.9.

## Troubleshooting

- **`podman run` fails with `cannot clone: Operation not permitted` or `cannot re-exec process`**
  — the seccomp profile did not reach the container. Check that `runArgs` still references
  `seccomp-podman.json` and that the file exists at that path on the host. `post-start.sh` prints
  a diagnosis for this on every start.
- **Nested containers cannot resolve each other by name** — `/dev/net/tun` missing from
  `runArgs`, or the Feature's `rootlessNetworkCmd` is not `slirp4netns`.
- **`./gradlew` dies with `Could not create parent directory for lock file`** — the Gradle cache
  volume is root-owned; `post-create.sh` should have fixed this. Rebuild the container.
- **Wheel build failed at start** — see `~/.cache/deephaven/devcontainer-post-start.log`. The
  wheel container runs `mypy` and `ruff` over `py/server`, so formatting or typing drift there
  fails this build.
- **On rootless Linux, files you create show up on the host owned by someone else** — the remap
  did not take. `id` inside should print `uid=0(vscode)`; if it prints 1000, rebuild without
  cache and check the build log for the `rootless-remap` lines.
