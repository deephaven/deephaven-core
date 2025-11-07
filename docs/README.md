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

### AI Tools

Our current recommendations for AI tools are:

- Use [Visual Studio Code](https://code.visualstudio.com/).
- Use [GitHub Copilot](https://copilot.github.com/).
    - When using Copilot, manually choose the `GPT 4o` model. It has performed well in our experience, so choose this unless you have a reason to try something else. Do not use the `o1` model - it does a poor job.
    - Select the text you want AI to work on and type `cmd+i` to bring up the Copilot chat window.
    - A simple prompt of `improve` is often sufficient to get an acceptable result.
    - If you write the entire body of the doc, you can go to the top of the doc and hit cmd+I and try more complex prompts like `write an introduction to this document`. Sometimes AI will do well with this; other times it will not.

The above workflow allows you to work directly with .md files without having to copy/paste things back and forth between your IDE and the AI chat panel in your browser. This is a big time saver.

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
