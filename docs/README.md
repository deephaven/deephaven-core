# Core Docs

This directory contains the Deephaven Core documentation for Python and Groovy. All scripts used require Docker running locally.

## TL;DR

```sh
./docs/start # Start docs server on port 3001. -p <port> to run on a different port.
./docs/format # Format all docs using dprint.
./docs/updateSnapshots # Update changed/added code block snapshots in the docs.
./docs/updateSnapshots -t local # Update changed/added code block snapshots using a local build of Deephaven Core.
```

## GitHub LFS

The repository uses [GitHub large file support](https://git-lfs.github.com/). Follow the directions in the link to install. If you are using Mac, you need to:

```
brew install git-lfs
git lfs install
```

Note that if you have installed lfs _after_ you cloned the repo, you will need to manually fetch the lfs files using:

```
git lfs fetch
```

On some systems, you may also need to run:

```
git lfs pull
```

The current files stored in LFS can be seen in [.gitattributes](.gitattributes). To add a new file type to LFS, use the `lfs track` command.  
Here is an example for `*.mp4` files.

```
git lfs track "*.mp4"
```

## Local Preview

Use the following script to run the docs with a local preview on [http://localhost:3001](http://localhost:3001):

```
./docs/start
./docs/start -p 4000 # to run on port 4000
```

## Editing

Documents are created and stored in the folder for each language as `.md` markdown files. You can use whatever folder hierarchy is appropriate for your content. All file and folder names should be in _kebab-case_.

Once a file is created, it must be added to the appropriate `./sidebar.json` to have it appear in the sidebar.

Image assets are stored in `<language>/assets` and can be linked using the relative path from your document.

An editor on the docs team should approve all changes before being merged.

Docs must be formatted using `./docs/format`, which uses `dprint` inside a Docker container. The config is stored in `dprint.json`. If you prefer to run `dprint` locally, you can follow the [dprint install instructions](https://dprint.dev/install/).

The following extensions can be used to format on save in your IDE:

- VSCode [dprint extension](https://marketplace.visualstudio.com/items?itemName=dprint.dprint). Follow instructions to init `dprint` and set as your default formatter.
- Jetbrains [dprint plugin](https://plugins.jetbrains.com/plugin/18192-dprint). Follow instructions to init `dprint` and set up format on save.

In some cases, such as YAML code blocks, you may want to preserve indentation and tell dprint not to format the code. First, consider if this is really necessary and not that you prefer different formatting. If necessary, use a `<!-- dprint-ignore -->` comment to skip the following line. This works if put directly above a code block. You can also use `<!-- dprint-ignore-start -->` and `<!-- dprint-ignore-end -->` to ignore a code block.

## Code Block Snapshots

All `python` and `groovy` code blocks in the documentation are run, and the output is saved as a snapshot. This allows us to ensure the code examples are up-to-date and working correctly. See the [snapshotter README](./snapshotter/README.md) for more info. The documentation is also available [here](https://github.com/deephaven/salmon/tree/main/tools/snapshotter#snapshotter-tool).

You can run the snapshotter tool against the latest published Deephaven Core release, a Docker tag, or a local build of Deephaven Core. If you are documenting a new feature, you must run against the local build.

To run the snapshotter tool, use the following command:

```
./docs/updateSnapshots
./docs/updateSnapshots -t local # to use a local build of Deephaven Core
```

> [!NOTE]
> Snapshots of tables will be limited to the first 100 rows and plots to an equally spaced 1000 points (based on index) to ensure the files are not too large.

Some meta tags can be used to control the behavior of the snapshotter tool. These tags are added to the code block after the language. Details about the tags are in the [snapshotter tool documentation](./snapshotter/README.md).

### IDE and AI Tools

We recommend [Windsurf](https://windsurf.com/) as the IDE for docs work. It integrates well with [Claude](https://www.anthropic.com/claude), which is our preferred AI model for documentation tasks.

**Windsurf Setup:**

1. Install Windsurf from [windsurf.com](https://windsurf.com/).
2. Open the Cascade panel (the AI assistant) and select Claude as your model.
3. Use `Cmd+L` to open the Cascade chat for general questions or `Cmd+I` for inline edits.

**Tips:**

- Select text and use `Cmd+I` to get inline suggestions for improving specific sections.
- A simple prompt like "improve" or "make this clearer" often works well.
- For larger tasks, describe what you want in the Cascade panel and let it make edits across files.
- Claude handles technical documentation and code examples well, making it a good fit for Deephaven docs.

**Alternative: VS Code with GitHub Copilot**

If you prefer [Visual Studio Code](https://code.visualstudio.com/), you can use [GitHub Copilot](https://copilot.github.com/):

- When using Copilot, manually choose the `GPT 4o` model. It has performed well in our experience. Do not use the `o1` model.
- Select the text you want AI to work on and type `Cmd+I` to bring up the Copilot chat window.
- A simple prompt of "improve" is often sufficient to get an acceptable result.

Both workflows let you work directly with .md files without copying between your IDE and a browser-based AI chat.

**Style Guide:**

Documentation style standards are defined in `.windsurf/rules`. When using Windsurf, Cascade automatically applies these rules to this repo.

**Slash Commands:**

Custom workflows are available in `.windsurf/workflows/`. Type the command in Cascade chat:

- `/format-docs` — Format, validate, and update snapshots for documentation.
- `/create-groovy-version` — Create a Groovy version of a Python documentation page.
- `/add-to-sidebar` — Add a new documentation page to the sidebar.
- `/accuracy-check` — Review documentation for technical accuracy, style, and missing links.

## File Locations

- **Python docs:** `docs/python/`
- **Groovy docs:** `docs/groovy/`
- **Conceptual guides:** `docs/{python,groovy}/conceptual/`
- **Reference docs:** `docs/{python,groovy}/reference/`
- **How-to guides:** `docs/{python,groovy}/how-to-guides/`

## Git Workflow

This is a forking workflow:

- `origin`: your-username/deephaven-core (your fork)
- `upstream`: deephaven/deephaven-core (main repo)
- Push branches to `origin`, not `upstream`
- PRs go from `your-username:branch` to `deephaven:main`

## Indexing

The site search re-indexes nightly. If you publish a new page or change files significantly mid-day, this may affect search results. Ask in the docs channel for someone to trigger this job in Inkeep manually.

## Deployment

Changes are automatically deployed live when the branch is merged to main. Continuous deployment is handled by a github action. See the .github/workflows for details.

The action uses rsync to sync the docs directory with the server (currently AWS S3).

## Code Of Conduct

This project has adopted the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
For more information see the [Code of Conduct](./CODE_OF_CONDUCT.md) or contact [opencode@deephaven.io](mailto:opencode@deephaven.io)
with any additional questions or comments.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details.
