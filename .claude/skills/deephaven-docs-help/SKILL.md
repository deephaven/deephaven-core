---
name: deephaven-docs-help
description: >
  Front door for vague deephaven-core (Community) documentation requests. It does no review or writing itself: it asks one short question and hands off to the right skill. **Use this skill when:** someone asks for a documentation "review," "check," "look," or "feedback" without saying how deep ("give me a review," "can you check my docs," "look at this page"), or asks for help writing documentation, whether or not they name a kind ("help me write docs for this," "document this feature," "I need docs for X," "write something up," "help me write a how-to for X"). Saying how much changed ("I only changed one sentence") does not name a depth, so use this skill and recommend the matching option. **Do NOT use when** a review request already names the depth or dimension: "spot-check," "quick check," "full review," "fact-check," "check the style," "is the organization right" (go straight to the matching skill), when the request is to address PR review comments (use deephaven-docs-address-review-comments), to find out what needs documenting for a release (use deephaven-documentation-needs-check), or for Enterprise/deephaven-ent docs.
allowed-tools: Read, Grep, Glob, Skill, Bash(git diff *), Bash(git status *)
---

# Deephaven docs help: pick the right workflow

This skill is a router. Its only job is to turn a vague request into the right skill with one
short exchange. Do not start reviewing or drafting here.

Look at what the person gave you before asking anything. A file, diff, or PR tells you a lot:
if it answers a question below, don't ask that question.

## Request is a review ("review this," "check my docs")

1. **Find the target.** A target is a docs page or a diff that touches docs pages (files under
   `docs/`). If the request names none and the working tree (`git diff`, `git status`) has no
   docs changes, ask which page or change to look at, even when the tree has other changes
   such as code or skill edits. Stop there until you have one.
2. **Size it.** Use the target to pick a recommendation:
   - One isolated change (one sentence, one snippet, one parameter name, or a paragraph that
     makes a single claim): recommend a **spot check** (`deephaven-core-accuracy-spot-check`).
     The spot check excludes edits that touch several sections or make several independent
     claims, so don't recommend it for those.
   - Several independent claims or several sections changed, and the person only cares whether
     it is true: recommend an **accuracy check** (`deephaven-core-accuracy-check`).
   - A new page, a rewritten page, a PR touching several sections, or "is this ready to
     publish": recommend a **full review** (`deephaven-docs-review-full`: accuracy, then
     structure, then style).
   - Unclear: recommend the full review, since it is a safe fallback.
3. **Offer the pick list**, with your recommendation first and each option's cost in plain words:
   - **Quick spot check**: verifies the changed lines and the sentences directly around them
     against source, for one isolated change. Fastest.
   - **Full review**: accuracy, structure, and style on the whole page. Slowest, most thorough.
   - **One dimension**: accuracy only (`deephaven-core-accuracy-check`), structure only
     (`deephaven-doc-structure-review`), or style only (`deephaven-writing-style`).
4. On their answer, invoke that skill with the target. If they say "whatever you think," use
   your recommendation.

## Request is to write docs ("help me write docs for this")

Docs here come in four categories, defined in `ref-deephaven-doc-categories` (read it for the
current directory and tone rules; don't restate them from memory). Work out which one the
person needs, asking only what the request and the code don't already answer. Ask at most
three questions, one message at a time:

1. **What is being documented?** A feature, an API, a workflow, or a design idea? If it is a
   code change, read the diff first so you can describe it back instead of asking.
2. **Who is the reader and what are they trying to do?**
   - Following a fixed path from zero to a known result: **tutorial**. In this repo that means
     the Crash Course only; if that is not what they want, it is not a tutorial.
   - Getting a specific job done with their own data: **how-to guide**.
   - Understanding why or how something works: **concept guide**.
   - Looking up exact names, parameters, and return values: **reference guide**.
3. **Is there existing coverage?** Search `docs/` for the feature so you update a page instead
   of adding a duplicate.

Features usually need more than one doc. Say so when it applies, for example a reference entry
for each new method plus a how-to for the main workflow, and let the person choose which to
write now. Present the result as a short plan: the category, the target directory and filename,
and the pages to update (Python and Groovy pairs, sidebar entries, Related documentation).
Then confirm before writing.

When they agree, write following `deephaven-writing-style`, and offer the review pick list above
once the draft exists.

## Rules

- Ask at most one question per message, and never re-ask something the request answered.
- Recommend; don't survey. Lead with your pick and one line on why.
- Do not run any review or draft anything before the person has chosen.
- If a review request turns out to be specific after all (names a depth or dimension), skip
  this skill and invoke the matching review skill directly.
- For a writing request, naming a category ("help me write a how-to for X") answers question 2
  only. There is no per-category drafting skill, so still do the existing-coverage search and
  present the plan for confirmation before drafting.
