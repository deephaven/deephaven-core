---
title: Create and manage Markdown widgets
---

This guide shows you how to add a Markdown widget to a dashboard, style its content with Markdown, and reopen or delete widgets you have closed. A dashboard is the arrangement of tables, plots, and other panels in the Deephaven web interface.

The Markdown widget is a useful tool for including text or images alongside your data. Use it for notes such as descriptions of the tables and plots in the dashboard, code snippets and explanations, instructions, contact info, and reminders.

## Create a Markdown widget

To create a Markdown widget, open the [**Controls** menu](./navigating-the-ui.md#top-bar) and select **Markdown Widget**. You can also drag **Markdown Widget** from the menu to the spot in the dashboard where you want the widget.

![The Controls menu button highlighted in the Deephaven web interface](../../assets/how-to/controls.png)

![The Markdown Widget button highlighted in the Controls drop-down](../../assets/how-to/mw1.png)

What opens depends on whether this dashboard has any [closed Markdown widgets that you edited](#manage-markdown-widgets):

- If it doesn't, a new widget opens with placeholder text.
- If it does, the widget opens on a start page. Click **New Markdown Note** to create a widget, or click a name under **Recently Closed from Dashboard** to reopen that widget.

![The start page, with a New Markdown Note link and a Recently Closed from Dashboard list](../../assets/how-to/mw3.png)

Double-click inside the widget to edit its content. A widget you create with **New Markdown Note** is already open for editing. There's no Save button. When you click outside the widget, Deephaven saves your edits and shows the formatted content:

<LoopedVideo src='../../assets/how-to/markdown-widget.mp4' />

New widgets are named **Note**, **Note-1**, and so on. To give a widget a name you'll recognize in the **Recently Closed from Dashboard** list, right-click its tab and choose **Rename**.

## Markdown syntax

Style the widget's content with Markdown syntax. The following table shows common examples:

| Markdown                                       | Result                                                  |
| ---------------------------------------------- | ------------------------------------------------------- |
| `# Heading 1 Text`                             | Level-1 heading                                         |
| `## Heading 2 Text`                            | Level-2 heading                                         |
| `### Heading 3 Text`                           | Level-3 heading                                         |
| `**bold text**`                                | Bold text                                               |
| `*italic text*`                                | Italic text                                             |
| `> This is a block quote`                      | Block quote                                             |
| `` `inline code` ``                            | Inline code                                             |
| `![Test image](https://example.com/image.png)` | The image at the URL, with `Test image` as its alt text |

The widget also renders code blocks, links, lists, and horizontal rules. For math, write LaTeX between single `$` signs for inline math. For a block, put `$$` on its own line before and after the LaTeX.

The widget doesn't render HTML tags such as `<img>`. It shows them as plain text.

> [!NOTE]
> For more syntax, see the [Markdown Guide's basic syntax](https://www.markdownguide.org/basic-syntax/) page.

## Manage Markdown widgets

When you close a Markdown widget that you have edited, Deephaven adds it to the dashboard's **Recently Closed from Dashboard** list. A new widget you never edited, or a widget you left empty, isn't added.

Your browser stores your Markdown widgets with the rest of your layout. They don't appear in other browsers or for other users, and clearing your browser's site data deletes them.

To reopen a widget, select **Markdown Widget** from the [**Controls** menu](./navigating-the-ui.md#top-bar), then click its name in the **Recently Closed from Dashboard** list on the start page.

To permanently delete a closed widget:

1. Select **Markdown Widget** from the [**Controls** menu](./navigating-the-ui.md#top-bar) to open the start page.
2. Hover over the widget's name in the **Recently Closed from Dashboard** list.
3. Click the trash can icon that appears.
4. Click **Delete** to confirm. You can't undo the deletion.

![The widget's trash can icon, highlighted](../../assets/how-to/mw4.png)

## Related documentation

- [Navigate the GUI](./navigating-the-ui.md)
