---
title: Create and organize notebooks
---

You can use Deephaven notebooks to develop query components, stage draft queries, or store notes. Think of a notebook as a sandbox where you develop and refine queries, then run them in the [Console](./navigating-the-ui.md#console-pane) with one click. A query on ticking data keeps updating after you run it, so you can watch its results change as new data arrives.

The File Explorer is a tab in the [right-hand pane](./navigating-the-ui.md#right-hand-pane) of the Deephaven GUI. It lists the notebooks and folders saved on the server and lets you create, open, copy, rename, move, and delete them.

## Create notebooks and folders

Use the two buttons at the top of the File Explorer to create new notebooks and folders.

![The New notebook and New folder buttons highlighted at the top of the File Explorer](../../assets/how-to/notebooks/notebook1.png)

Click the **New notebook** button on the left, and a blank notebook opens below the [Console](./navigating-the-ui.md#console-pane) panel.

![A new, empty notebook](../../assets/how-to/notebooks/notebook3.png)

The filename extension matches the programming language of your current session, so a new notebook in a Groovy session gets a name like `Untitled-1.groovy`. The screenshot above comes from a Python session and shows a `.py` name.

Click the **New folder** button on the right to create a folder. A **Create New Folder** dialog opens, where you name the folder and choose the directory to create it in.

![A new folder panel appears in which you can name the new folder and choose what directory to create it in](../../assets/how-to/notebooks/notebooks5.png)

## Run and save a notebook

The notebook toolbar has three buttons and a **More Actions** menu.

![The Run, Run Selected, and Save buttons at the top of a notebook](../../assets/how-to/notebooks/notebook6.png)

- **Run**: Runs the notebook's entire content in the [Console](./navigating-the-ui.md#console-pane).
- **Run Selected**: Runs the selected lines in the [Console](./navigating-the-ui.md#console-pane). A line you highlight only part of runs in full. If nothing is highlighted, it runs the line the cursor is on.
- **Save**: Saves the notebook as a file on the server, where it appears in the File Explorer. The first time, Deephaven asks for a filename and directory and renames the tab to match. After that, **Save** overwrites the file. See [Where notebooks are stored](#where-notebooks-are-stored).
- **More Actions**: Opens a menu of other notebook actions, including **Find**, **Copy File**, **Rename File**, **Delete File**, **Show Minimap**, and **Word Wrap**.

**Run** and **Run Selected** are available only when the notebook's language matches your session's, so a `.py` notebook can't run in a Groovy session.

A blue circle on a notebook's tab means the notebook has unsaved changes.

A notebook whose name ends in `.md` opens as rendered Markdown instead of in the code editor, and it doesn't have the notebook toolbar. Instead, each code block in it has its own **Run code** button. The **Run Selected Code** button at the top runs the highlighted code block and then highlights the next one, so you can step through the notebook one block at a time.

## Use the notebook context menu

Right-clicking within the body of a notebook opens a context menu with the following options.

![The notebook's right-click options panel](../../assets/how-to/notebooks/notebook7.png)

- **Find**: Opens a search bar for that notebook.
  ![A search bar appears at the top of the notebook](../../assets/how-to/notebooks/notebook8.png)
- **Run** and **Run Selected**: Run the whole notebook or the selected lines, like the [toolbar buttons](#run-and-save-a-notebook) of the same names.
- **Change All Occurrences**: Selects every occurrence of the selected text in the notebook. When you type, all of them change at once.
- **Cut**: Removes the highlighted content and copies it to the clipboard.
- **Copy**: Copies the highlighted content to the clipboard.
- **Paste**: Inserts the clipboard's content at the cursor.
- **Command Palette**: Opens the Command Palette, which lists the editor's commands and the keyboard shortcuts for common ones.
  ![The Command Palette menu](../../assets/how-to/notebooks/notebook9.png)

## Organize notebooks

The File Explorer lists your saved notebooks and folders. Use it to open, copy, rename, delete, and arrange them:

- Click a notebook to preview it. A preview tab shows one notebook at a time, and the next notebook you click replaces it.
- Double-click a notebook to open it in its own tab. If it's already open, Deephaven switches to its tab.
- Click a folder to expand or collapse it.
- Right-click a notebook to copy, rename, or delete it, or right-click a folder to rename or delete it.
- The same right-click menu has **New File** and **New Folder**, which create a notebook or folder.
- To delete several items at once, Shift-click or Ctrl-click (Cmd-click on macOS) to select them, then right-click and choose **Delete**.
- Drag any item into or out of a folder to organize your files.

## Where notebooks are stored

The server stores notebooks as files in the `storage/notebooks` directory under its data directory:

- **Docker:** The [Deephaven Docker images](../../getting-started/docker-install.md) set the data directory to `/data`, so notebooks are in `/data/storage/notebooks`.
- **Other installations:** The data directory is [OS-dependent](../configuration/configuration-properties.md#os-dependent-directories). The server prints it at startup as `deephaven.dataDir` unless bootstrap logging is turned off.

To change the data directory, see [Bootstrap configuration](../configuration/configuration-properties.md#bootstrap-configuration). To move only the storage root, set the [`storage.path`](../configuration/configuration-properties.md#server-configuration) property.

> [!WARNING]
> If you run Deephaven in Docker without mounting your own volume at `/data`, the server stores notebooks in an anonymous Docker volume. A container you start later with `docker run` gets a new, empty volume and doesn't see them. Mount a persistent Docker volume or a host directory at `/data` to keep your notebooks. See [Access your file system with Docker data volumes](../../conceptual/docker-data-volumes.md) for instructions on persisting your notebooks across container restarts and removals.

## Related documentation

- [Access your file system with Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Install and run Deephaven with Docker](../../getting-started/docker-install.md)
- [Navigate the GUI](./navigating-the-ui.md)
- [Configuration properties reference](../configuration/configuration-properties.md)
