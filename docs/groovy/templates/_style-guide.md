---
title: Documentation style guide
---

## Organization

We've adopted Diátaxis Framework for documentation. All new content should fit into one of these four quadrants:

- **Tutorial** - A learning resource / lesson that guides a reader step-by-step to a specific outcome. All users will achieve the same outcome.
- **How-to** - A "follow along" resource that also guides a reader step-by-step. A getting started guide for new users to show how to solve a specific problem. Similar to a recipe: when following along with a how-to guide, all users will learn the same information, but they can use their own ingredients, so outcomes may differ. As opposed to a reference guide, here advice or general recommendations on how to use a feature / method may be offered.
- **Reference** - A resource to study. Purely informative; purpose is to describe. No extraneous information - very pointed, without advice or general tips.
- **Conceptual Guide** - A resource to study. Intended to broaden understanding of higher concepts, to explain how or why, with contextual explanations.

## Code Formatting and Conventions

- By convention, column names should always start with capital letters; e.g, "NewColumn" , "StringColumn"
- Write out "column" for consistency - e.g., `"columnToMatch"`; `"sourceColumn"`
- "parameter" or "argument" is used to define an argument to a function. "method" or "function" is used to define a function.
- If a parameter is a vararg, use an ellipsis: e.g., `String...`
- A true/false condition as type will be written as `boolean`
- Use whitespace for readability in code blocks, both by including line spaces and examples as `"A = 4"`
- Note that variables should be formatted in backticks (code); `A = 4`
- Avoid the full import when possible: `from deephaven import *`; use the specific module, class, or function
- Null should be written as "will not include null values" within a paragraph / narrative, written as `NULL` in parameter descriptions, and written using the appropriate programming language null value in code.

## Writing Conventions

- No contractions in reference material
- Tone of tutorials and how-tos can be conversational, friendlier. Reference material is dry and formal - no "you".
- Headings: "sentence case"; no capitals other than first word of proper nouns; e.g., "How to join tables"
- Links: when a method is mentioned in the documentation outside of its primary article, link to the appropriate reference page for that method. This excludes code snippets / blocks.
- Use present, active tense - e.g., "The thing does X" instead of "The thing will do X".

## Examples

- Shorter is better.
- OSS reference examples should be self-contained - can be run without importing data.
- Try to use the same or nearly the same tables / example sets in a given topic to highlight differences in methods.

## Reference Section

- One example for each syntax version presented
- Users should be able to perform a single cut and paste to run an example
- Only use method name once in description (or it's confusing and circular)
- Variables for example tables should be `source` and `result` consistently
- Explanation should be minimal and limited to code comments. A one line sentence to precede example when necessary, particularly if there is more than one. "**The following query** uses a single ternary-if operator" and "**The following query** chains two operators together", etc.
- Tables should be self-contained - in other words, users does not have to import data and can run the example without special handling.
- Column names and formulas should be in code: `Z = X + Y`
- Links to JavaDocs/PyDocs should say [JavaDocs] unless there are multiple: e.g., [TableTools PyDocs] and [QueryScope PyDocs].

## How-To Guides

Examples can be rolled out in parts, as we expect the reader to follow along, and we'll walk through the queries with more explanation.

## Screenshots

- Do not capture the console and code in the shot; just the table or plot being referenced. Each captured table should be associated with only one tab.
- Gifs should be very small files; must be 2mb or less
- See [Editing](./README.MD/editing) for more information on screenshot conventions.

## Markdown

You can write content using [GitHub-flavored Markdown syntax](https://github.github.com/gfm/).

### Syntax

This is an example page for styling markdown based Docusaurus sites.

### Code Switcher

```groovy skip-test
// I am groovy code
```

### Expandable section

Requires extra newlines around content to be formatted as embeded markdown.

<details>
<summary>Title Goes here</summary>

I am an example collapsing section.

</details>

### Headers

# H1 - Create the best documentation

## H2 - Create the best documentation

### H3 - Create the best documentation

#### H4 - Create the best documentation

##### H5 - Create the best documentation

###### H6 - Create the best documentation

---

### Emphasis

Emphasis, aka italics, with _asterisks_ or _underscores_.

Strong emphasis, aka bold, with **asterisks** or **underscores**.

Combined emphasis with **asterisks and _underscores_**.

Strikethrough uses two tildes. ~~Scratch this.~~

---

### Lists

1. First ordered list item
1. Another item
   - Unordered sub-list.
1. Actual numbers don't matter, just that it's a number
   1. Ordered sub-list
1. And another item.

- Unordered list can use asterisks

* Or minuses

- Or pluses

---

### Links

- When query methods are referenced within the text, link to the reference article, "you can use naturalJoin[link] to do x, y, z". As opposed to, "Check out our tutorial "Creating your first table"[tutorial link]."
- No links in headers.

[I'm an inline-style link](https://www.google.com/)

[I am a relative link in the same directory.](./README.md)

[I am a relative link in another directory.] (../reference/table-operations/filter/where.md)

Tip: don't forget the leading slash in your internal link.

Tip: do **not** include a slash before an anchor link:

[Error Bar Plotting](./error-bars.md#category)

[I'm an inline-style link with title](https://www.google.com/ "Google's Homepage")

[I'm a reference-style link][arbitrary case-insensitive reference text]

[You can use numbers for reference-style link definitions][1]

Or leave it empty and use the [link text itself].

URLs will automatically get turned into links. http://www.example.com/ and sometimes example.com (but not on GitHub, for example).

Some text to show that the reference links can follow later.

[arbitrary case-insensitive reference text]: https://www.mozilla.org/
[1]: http://slashdot.org/
[link text itself]: http://www.reddit.com/

---

### Images

Images from any folder can be used by providing path to file. Path should be relative to markdown file.

![img](../../../static/img/logo.svg)

### Video

Autoplay Looped Video instead of GIF, for large files.

<LoopedVideo src='../assets/interfaces/classic/pw25.mp4' />

Here's a YouTube embed:

<Youtube id="dQw4w9WgXcQ" />

### Code

```javascript
var s = "JavaScript syntax highlighting";
alert(s);
```

```
No language indicated, so no syntax highlighting.
But let's throw in a <b>tag</b>.
```

```js {2}
function highlightMe() {
  console.log("This line can be highlighted!");
}
```

---

### Tables

Colons can be used to align columns.

| Tables        |      Are      |   Cool |
| ------------- | :-----------: | -----: |
| col 3 is      | right-aligned | \$1600 |
| col 2 is      |   centered    |   \$12 |
| zebra stripes |   are neat    |    \$1 |

There must be at least 3 dashes separating each header cell. The outer pipes (|) are optional, and you don't need to make the raw Markdown line up prettily. You can also use inline Markdown.

| Markdown | Less      | Pretty     |
| -------- | --------- | ---------- |
| _Still_  | `renders` | **nicely** |
| 1        | 2         | 3          |

Markup for special parameters table used by api reference pages

<ParamTable>
<Param name="SampleParameter1" type="String">

I am a sample Parameter description. To use **markdown** I require a blank line before and after.

</Param>
<Param name="SampleParameter2" type="String">I am an inline param, no markdown support.</Param>
<Param name="TableSample" type="Table">

- List
- item
- here

</Param>
</ParamTable>

---

### Blockquotes

> Blockquotes are very handy in email to emulate reply text. This line is part of the same quote.

Quote break.

> This is a very long line that will still be quoted properly when it wraps. Oh boy let's keep writing to make sure this is long enough to actually wrap for everyone. Oh, you can _put_ **Markdown** into a blockquote.

---

### Inline HTML

<dl>
  <dt>Definition list</dt>
  <dd>Is something people use sometimes.</dd>

<dt>Markdown in HTML</dt>
  <dd>Does *not* work **very** well. Use HTML <em>tags</em>.</dd>
</dl>

---

### Line Breaks

Here's a line for us to start with.

This line is separated from the one above by two newlines, so it will be a _separate paragraph_.

This line is also a separate paragraph, but... This line is only separated by a single newline, so it's a separate line in the _same paragraph_.

---

### Admonitions

> [!NOTE]
> This is a note

> [!TIP]
> This is a tip

> [!IMPORTANT]
> This is important

> [!CAUTION]
> This is a caution

> [!WARNING]
> This is a warning

> [!NOTE]
> Good to know:
> You can add a custom title to any of these bases.
