"""
Sidebar consistency check

Checks docs/python/sidebar.json and docs/groovy/sidebar.json for:
    - pages whose file does not exist
    - groups that hold only one item
    - labels in Title Case instead of sentence case

Intentional exceptions live in allowlist.json next to this script. The check
also fails on allowlist entries that no longer match anything, so the list
stays current.

Usage:
    python3 docs/tools/sidebar-check/check_sidebar.py

Exits with status 1 if it finds a problem.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.dirname(os.path.dirname(HERE))
LANGUAGES = ["python", "groovy"]

# A word that is capitalized but not all caps (APIs and acronyms like CSV pass).
CAPITALIZED_WORD = re.compile(r"^[A-Z][a-z]+$")
WORD = re.compile(r"[A-Za-z][\w'-]*")


def looks_like_code(label):
    """Labels that name an API element (format_columns, formatColumns, InputTable, ...)."""
    return bool(re.search(r"[_().]|^[a-z]+[A-Z]|^[A-Z][a-z]+[A-Z]", label))


def title_case_words(label, allow):
    """Returns the words after the first that should be lowercase in sentence case."""
    for phrase in allow["capitalized_phrases"]:
        label = label.replace(phrase, "")
    words = WORD.findall(label)
    return [w for w in words[1:] if CAPITALIZED_WORD.match(w) and w not in allow["proper_nouns"]]


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
                or any(where.startswith(s) for s in exempt_subtrees)
                or item.get("path") in case_exempt_paths
                or looks_like_code(label)
            )
            if not exempt:
                words = title_case_words(label, allow)
                if words:
                    errors.append(f"{lang}: '{where}' is not sentence case ({', '.join(words)})")
            for s in exempt_subtrees:
                if where.startswith(s):
                    used.add(("subtree", s))
            if item.get("path") in case_exempt_paths:
                used.add(("path", lang, item["path"]))

            if "items" in item:
                if len(item["items"]) == 1:
                    if where in single_ok:
                        used.add(("single", lang, where))
                    else:
                        errors.append(f"{lang}: group '{where}' holds only one item")
                walk(item["items"], trail + [label])
            elif "path" in item:
                if not os.path.isfile(os.path.join(DOCS, lang, item["path"])):
                    errors.append(f"{lang}: '{where}' points to missing file {item['path']}")

    walk(sidebar["sidebars"]["main"], [])
    return errors


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

    for e in errors:
        print(e)
    if errors:
        print(f"\n{len(errors)} sidebar problem(s). Fix the sidebar, or add an intentional "
              "exception to docs/tools/sidebar-check/allowlist.json.")
        return 1
    print("Sidebars OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
