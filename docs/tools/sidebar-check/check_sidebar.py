"""
Sidebar consistency check

Checks docs/python/sidebar.json and docs/groovy/sidebar.json for:
    - pages whose file does not exist
    - groups that hold fewer than two items
    - labels in Title Case instead of sentence case
    - capitalized Deephaven terms (for example "Execution Context") written in another case

Intentional exceptions live in allowlist.json next to this script. The check
also fails on allowlist entries that no longer match anything, so the list
stays current.

Usage:
    python3 docs/tools/sidebar-check/check_sidebar.py

Exits with status 1 if it finds a problem. In GitHub Actions, each problem is
also reported as an error annotation on the file it concerns, so it shows in
the pull request's checks summary.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.dirname(os.path.dirname(HERE))
LANGUAGES = ["python", "groovy"]

# A word that is capitalized but not all caps (APIs and acronyms like CSV pass).
# Identifiers such as format_columns, deephaven.ui, and InputTable never match.
CAPITALIZED_WORD = re.compile(r"^[A-Z][a-z]+$")
# Keeps dotted and underscored identifiers together as one token, and splits
# hyphenated words so each part is checked ("Title-Case" -> "Title", "Case").
WORD = re.compile(r"[A-Za-z][\w']*(?:\.[A-Za-z_]\w*)*")


def phrase_pattern(phrase):
    """Matches a capitalized phrase or its plural, in any case."""
    return re.compile(r"\b" + r"\s+".join(map(re.escape, phrase.split())) + r"(?:e?s)?\b", re.IGNORECASE)


def title_case_words(label, allow, used):
    """Returns the words after the first that should be lowercase in sentence case."""
    for phrase in allow["capitalized_phrases"]:
        # Lowercase rather than delete the phrase, so the label's first word stays first.
        label = label.replace(phrase, phrase.lower())
    words = WORD.findall(label)
    found = []
    for w in words[1:]:
        if CAPITALIZED_WORD.match(w):
            if w in allow["proper_nouns"]:
                used.add(("noun", w))
            else:
                found.append(w)
    return found


def miscased_phrases(label, allow, used):
    """Returns uses of a capitalized phrase (or its plural) written in a different case."""
    found = []
    for phrase in allow["capitalized_phrases"]:
        for m in phrase_pattern(phrase).finditer(label):
            used.add(("phrase", phrase))
            # The phrase keeps its capitals; a plural suffix is lowercase.
            expected = phrase + m.group(0)[len(phrase):].lower()
            if m.group(0) != expected:
                found.append(f"'{m.group(0)}' should be '{expected}'")
    return found


def in_subtree(where, subtree):
    """True if the trail is the subtree itself or one of its descendants."""
    return where == subtree or where.startswith(subtree + " > ")


def check_language(lang, allow, used):
    errors = []
    sidebar = json.load(open(os.path.join(DOCS, lang, "sidebar.json")))
    single_ok = set(allow["single_page_groups"].get(lang, []))
    case_exempt_paths = set(allow["case_exempt_paths"].get(lang, []))
    exempt_subtrees = allow["case_exempt_subtrees"]

    def walk(items, trail):
        for item in items:
            label = item["label"]
            where = " > ".join(trail + [label])
            exempt = (
                not trail  # top-level sections keep their names
                or any(in_subtree(where, s) for s in exempt_subtrees)
                or item.get("path") in case_exempt_paths
            )
            if not exempt:
                words = title_case_words(label, allow, used)
                if words:
                    errors.append(f"{lang}: '{where}' is not sentence case ({', '.join(words)})")
            # Proper nouns keep their capitals everywhere, even in labels exempt from sentence case.
            errors.extend(f"{lang}: '{where}': {m}" for m in miscased_phrases(label, allow, used))
            for s in exempt_subtrees:
                if in_subtree(where, s):
                    used.add(("subtree", s))
            if item.get("path") in case_exempt_paths:
                used.add(("path", lang, item["path"]))

            if "items" in item:
                if len(item["items"]) == 1 and where in single_ok:
                    used.add(("single", lang, where))
                elif len(item["items"]) < 2:
                    errors.append(f"{lang}: group '{where}' holds {len(item['items'])} item(s); groups need at least two")
                walk(item["items"], trail + [label])
            elif "path" in item:
                if not os.path.isfile(os.path.join(DOCS, lang, item["path"])):
                    errors.append(f"{lang}: '{where}' points to missing file {item['path']}")

    walk(sidebar["sidebars"]["main"], [])
    return errors


def annotate(error):
    """Prints a GitHub Actions error annotation for one problem."""
    source = error.split(":", 1)[0]
    file = ("docs/tools/sidebar-check/allowlist.json" if source == "allowlist"
            else f"docs/{source}/sidebar.json")
    message = error.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error file={file},title=Sidebar check::{message}")


def main():
    allow = json.load(open(os.path.join(HERE, "allowlist.json")))
    used = set()
    errors = []
    for lang in LANGUAGES:
        errors += check_language(lang, allow, used)

    for lang, groups in allow["single_page_groups"].items():
        errors += [f"allowlist: single-page group '{g}' ({lang}) no longer exists or has more items"
                   for g in groups if ("single", lang, g) not in used]
    for lang, paths in allow["case_exempt_paths"].items():
        errors += [f"allowlist: case-exempt path '{p}' ({lang}) is not in the sidebar"
                   for p in paths if ("path", lang, p) not in used]
    errors += [f"allowlist: case-exempt subtree '{s}' is not in any sidebar"
               for s in allow["case_exempt_subtrees"] if ("subtree", s) not in used]
    errors += [f"allowlist: capitalized phrase '{p}' is not in any checked label"
               for p in allow["capitalized_phrases"] if ("phrase", p) not in used]
    errors += [f"allowlist: proper noun '{n}' is not in any checked label"
               for n in allow["proper_nouns"] if ("noun", n) not in used]

    in_github_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    for e in errors:
        print(e)
        if in_github_actions:
            annotate(e)
    if errors:
        print(f"\n{len(errors)} sidebar problem(s). Fix the sidebar, or add an intentional "
              "exception to docs/tools/sidebar-check/allowlist.json.")
        return 1
    print("Sidebars OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
