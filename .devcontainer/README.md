# The Deephaven Core devcontainer

Configuration for running a development environment in a devcontainer. This provides:

- Tool installation for developing Deephaven Core
- A sandbox environment for allowing AI agents more autonomy to accomplish tasks without constant approval prompts

> IMPORTANT: it is not recommended to use Linux rootful Docker setups with this config due to the seccomp filter settings required to run nested podman without `--privileged`.

## Git worktrees

Running this config in a git worktree branch requires that the git repo be configured using `worktree.useRelativePaths`. This is due to worktree branches `.git` file being a pointer to the main checkout which sits outside of the folder mount for the project.

In git 2.48+ you can configure relative paths in git repos via:

```sh
git config --global worktree.useRelativePaths true   # new worktrees
git worktree repair                                  # existing ones, from the main checkout
```

VS Code will automatically determine the common mount directory so that git works in the devcontainer. If you are using the devcontainer CLI, you will need to explicitly include the `--mount-git-worktree-common-dir` flag.

```sh
devcontainer up --mount-git-worktree-common-dir
devcontainer exec --mount-git-worktree-common-dir bash
```

## What is in this folder

| File                     | Purpose                                                                                                                                                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `devcontainer.json`      | The configuration. Base image plus Features for Java, Node, Python, git-lfs, coding-agent CLIs, git identity, and nested containers via podman.                                                                                                  |
| `devcontainer-lock.json` | Pins every Feature to a digest, so a rebuild gets the same Features until someone updates it deliberately.                                                                                                                                       |
| `initialize-command.sh`  | Runs on the **host** before the container is created. Copies your git `user.name`/`user.email` into `~/.config/devc/gitconfig-identity`, which is bind-mounted read-only for the identity Feature.                                               |
| `post-create.sh`         | Runs once per container as the remote user: creates the Python venv that `remoteEnv` points at, and fixes ownership of the Gradle cache and `node_modules` volumes.                                                                              |
| `post-start.sh`          | Runs on every start: builds the Deephaven Python wheels (inside a nested container, via podman) and installs them into the venv, so `./gradlew server-jetty-app:run` works with no manual step. Idempotent; steady-state cost is one `pip show`. |
| `seccomp-podman.json`    | The seccomp profile that lets podman run inside the devcontainer without granting it any capability. See below.                                                                                                                                  |

## Agent permissions

An agent in here can read and write anything mounted into the container and reach anything on the
network. It cannot see the rest of your filesystem, your host's processes, or its Docker daemon.
There is no egress filtering: whatever an agent can read, it can send somewhere.

### What is mounted

- **This repo** — and, in a worktree, the main checkout's `.git`. That includes `.git/hooks`,
  shared with the main checkout, so a hook written from inside runs on the **host** the next time
  you commit or push there. Inherent to bind-mounting a repo you also use outside a container.
- **`.devcontainer/`**, as part of the repo. `initialize-command.sh` runs on the **host** at every
  rebuild.
- **Agent config** — `~/.claude`, `~/.copilot`, `~/.pi` mounted under `~/.config/devc/` on host. The intent is that any devcontainer can use the same mounts and share agent config + login credentials and that configuration persists across container rebuilds. By default the CLIs run in container will populate with their default config. e.g. Claude CLI defaults to `auto` mode by default. Users can modify on host according to their preferences.
  > Note: agent configs can contain hooks, and this is writable inside the devcontainer. The `~/.config/devc` host folder is not intended to be shared with host agents, so it's not directly a risk to the host, but an agent modifying a hook in devcontainer would be shared with any other devcontainers that share the same bind mount. Mitigation here is to not share the bind mount across devcontainers you don't want in the same blast radius.
- **Your git name and email**, read-only. Nothing else from your host git config.
- **Caches** — Gradle and `node_modules` are volumes, not host directories.

> Note: this config deliberately does not mount ssh keys, registry tokens (`~/.npmrc`, `~/.pypirc`), cloud CLI sessions,
> and your host's Docker socket, but some editors may automatically include these in certain scenarios. See VS Code Specific Settings below.

### VS Code Specific Settings

- **VS Code Dev Containers**
  - **ssh-agent** ssh credentials are forwarded to devcontainer if `ssh-agent` is running. Disable by not running `ssh-agent` on host.
  - **git credential helper** installed by default proxying to your host's credential manager. Opt out by setting
    `dev.containers.gitCredentialHelperConfigLocation: "none"` in VS Code host settings.
- **Copilot Chat in VS Code** configured in `devcontainer.json` to run without per-command approval via `"chat.tools.global.autoApprove": true`

## Nested containers: `docker` inside the devcontainer

This repo runs several steps in containers: protobuf code generation, the web IDE build, the
Python wheels, and the client integration tests that start a server container. Inside the
devcontainer, `docker` is podman (the `podman-as-docker` Feature), and those containers are
**children of the devcontainer**, not siblings on your host daemon. Nothing here mounts your
host's Docker socket.

The Feature adds **no capabilities** beyond Docker's default set — in particular not
`CAP_SYS_ADMIN`. Two things make that possible, and both are in this folder or the Feature:

- **`seccomp-podman.json`**, referenced from `runArgs`. Docker's default seccomp filter blocks the
  syscalls that create user namespaces and mount filesystems unless the container holds
  `CAP_SYS_ADMIN`. Podman needs exactly those, so this profile is Docker's own default profile,
  complete and unmodified, with **one rule added**: an unconditional allow for
  `unshare setns clone clone3 mount umount2 pivot_root mount_setattr open_tree open_tree_attr
move_mount fsopen fsconfig fsmount fspick sethostname setdomainname keyctl`.
  The kernel still enforces its own rules on these: without `CAP_SYS_ADMIN` they only work inside
  a user namespace the process created itself, which is how rootless podman works on any ordinary
  Linux desktop. Everything else Docker's default blocks stays blocked.
- **`systempaths=unconfined`** (declared by the Feature, not here): without it the nested
  container runtime cannot mount its own `/proc`. Without `CAP_SYS_ADMIN` this mostly exposes
  read-only kernel information; the kernel's permission checks on `/proc/sys` still apply.

The file is long because a seccomp profile cannot say "the defaults plus X". **Do not hand-edit
it** — regenerate by prepending that one rule to the upstream default
(`https://github.com/moby/profiles/blob/main/seccomp/default.json`, Apache-2.0). The canonical
copy is `https://github.com/devc-tools/devc-tools/blob/main/features/podman-as-docker/seccomp-podman.json`; it is duplicated here
because the Docker CLI reads it on the host at creation time, where a Feature cannot reach.

`--device=/dev/net/tun` in `runArgs` gives nested containers their own private networks with
name resolution between them, which the `deephaven-in-docker` Gradle tests rely on.

## Who you are inside the container

On macOS you are an ordinary `uid=1000(vscode)`. On a rootless Linux daemon you are
`uid=0(vscode)`, because the daemon maps your host uid to container root and so the workspace
bind mount is unwritable by anyone else — the `rootless-remap` Feature detects that at build time
and makes `vscode` uid 0, keeping its name and home. `sudo` becomes a no-op and tools that refuse
to run as root will complain, but nothing on the host changes: container root there _is_ your own
unprivileged user, so files come out owned by you.

## What this opens, in plain terms

Code in here can create user namespaces and mount inside them — what any unprivileged user on a
stock Linux desktop can do. That is a larger kernel attack surface than a default devcontainer,
since unprivileged user namespaces have been an entry point for privilege-escalation bugs; it is
not a capability grant, and escaping still takes a kernel bug rather than a known technique. The
blast radius is unchanged by anything in this folder: your own account on rootless Linux, the
Docker Desktop VM on macOS.

Two alternatives were rejected for being worse: `docker-in-docker` needs `--privileged`, and
`docker-outside-of-docker` hands the devcontainer control of your host's Docker daemon with no
escape needed.

For the measurements behind all of this, see the [devc-tools repo](https://github.com/devc-tools/devc-tools): `features/podman-as-docker/README.md`
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
- **`npm ci` fails with `EACCES`** — same root-owned-volume problem, for a `node_modules` mount.
  Rerun `bash .devcontainer/post-create.sh`; if you added a mount, add it to that script's list.
- **Wheel build failed at start** — see `~/.cache/deephaven/devcontainer-post-start.log`. The
  wheel container runs `mypy` and `ruff` over `py/server`, so drift there fails the build.
  Nothing is installed, so the next start just retries.
- **You edited `py/server` and the venv still runs the old code** — use the build-and-install
  command from `AGENTS.md`; `pip` in here is already the venv's, so it applies unchanged.
  `post-start.sh` runs it for you on first start only. Java changes need none of this — only the
  Python layer is a copy.
- **On rootless Linux, files you create show up on the host owned by someone else** — the remap
  did not take. `id` inside should print `uid=0(vscode)`; if it prints 1000, rebuild without
  cache and check the build log for the `rootless-remap` lines.

## Reference: why terminal auto-approve rules are not used

`chat.tools.terminal.autoApprove` cannot express "approve everything in this container". VS Code
parses each command and always asks about two constructs whatever the rules say: a variable
assignment (`LOG=/tmp/x`, `export FOO=bar`) and a redirect to a destination outside the workspace
or containing `$ ( ) { } ~`. Against ~4,900 recorded agent commands from this repo, a fully
loosened rule set still stopped on 8.7% of them — about one prompt every 12 commands, 98% of them
variable assignments. `chat.tools.global.autoApprove` is evaluated before those rules, so it
clears them too.
