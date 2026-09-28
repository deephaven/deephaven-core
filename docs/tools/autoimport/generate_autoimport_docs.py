"""
Auto-import documentation generator

This script generates documentation for Deephaven's auto-imported functions.
It must be run inside a Deephaven session.

Usage:
    1. Start a Deephaven server
    2. Copy this script to the data volume
    3. Run it in the Deephaven IDE or via script execution
    4. Copy the generated files from /data/autoimport_output/ to the docs directory

Output:
    - Generates separate markdown files for each category
    - Files are written to /data/autoimport_output/
"""

import os

os.system("pip install beautifulsoup4 requests")

from deephaven.pandas import to_pandas
from deephaven import empty_table
from deephaven import merge

from datetime import timedelta
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Optional
import functools
import requests
import jpy
import re

# Configuration: Category filters
# Maps category names to package/class prefixes
CATEGORY_FILTERS = {
    "java": [
        "java.lang.",
        "java.util.",
    ],
    "constants": [
        "io.deephaven.util.QueryConstants",
    ],
    "basic": [
        "io.deephaven.function.Basic",
    ],
    "data-types": [
        "io.deephaven.function.Cast",
        "io.deephaven.vector.VectorConversions",
        "io.deephaven.util.type.TypeUtils",
        "io.deephaven.util.type.ArrayTypeUtils",
        "io.deephaven.base.string.cache.CompressedString",
    ],
    "parse": [
        "io.deephaven.function.Parse",
    ],
    "time": [
        "java.time.",
        "io.deephaven.time.",
    ],
    "math": [
        "io.deephaven.function.Random",
        "io.deephaven.function.Numeric",
    ],
    "logic": [
        "io.deephaven.function.Logic",
    ],
    "search": [
        "io.deephaven.function.BinSearch",
    ],
    "sort": [
        "io.deephaven.function.Sort",
    ],
    "gui": [
        "io.deephaven.gui.",
        "io.deephaven.engine.util.ColorUtilImpl",
    ],
}

# Category metadata for generating page headers
CATEGORY_METADATA = {
    "basic": {
        "title": "Basic functions",
        "description": "Array manipulation, counting, null handling, and utility functions.",
        "source_class": "io.deephaven.function.Basic",
    },
    "math": {
        "title": "Math functions",
        "description": "Mathematical operations including abs, sum, avg, min, max, trigonometry, and statistics.",
        "source_class": "io.deephaven.function.Numeric",
    },
    "time": {
        "title": "Time functions",
        "description": "Date and time utilities for parsing, formatting, arithmetic, and time zone handling.",
        "source_class": "io.deephaven.time.DateTimeUtils",
    },
    "logic": {
        "title": "Logic functions",
        "description": "Boolean operations: and, or, not.",
        "source_class": "io.deephaven.function.Logic",
    },
    "parse": {
        "title": "Parse functions",
        "description": "String-to-primitive parsing functions.",
        "source_class": "io.deephaven.function.Parse",
    },
    "sort": {
        "title": "Sort functions",
        "description": "Sorting functions for arrays and vectors.",
        "source_class": "io.deephaven.function.Sort",
    },
    "search": {
        "title": "Search functions",
        "description": "Binary search and string matching utilities.",
        "source_class": "io.deephaven.function.BinSearch",
    },
    "gui": {
        "title": "GUI functions",
        "description": "Color utilities and constants for table formatting.",
        "source_class": "io.deephaven.gui.color.Color",
    },
    "constants": {
        "title": "Constants",
        "description": "Null values, infinity, and numeric limits.",
        "source_class": "io.deephaven.util.QueryConstants",
    },
    "java": {
        "title": "Java classes",
        "description": "Java standard library classes (String, Integer, List, Map, etc.).",
        "source_class": "java.lang",
    },
    "data-types": {
        "title": "Data types",
        "description": "Type casting and data structure utilities.",
        "source_class": "io.deephaven.function.Cast",
    },
}

OUTPUT_DIR = "/data/autoimport_output"

# Excluded sources
HashSet = jpy.get_type("java.util.HashSet")
exclude_sources = HashSet()
exclude_sources.add("java.util.Enum")
exclude_sources.add("java.util.Formattables")
exclude_sources.add("java.lang.LoggerFinder")
exclude_sources.add("io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils")


def remap_source(source: Optional[str], name: Optional[str], signature: Optional[str]) -> Optional[str]:
    """Remap Deephaven's BinSearchAlgo.valueOf enum."""
    if source == "io.deephaven.function.BinSearchAlgo" and name == "valueOf" and signature == "Enum(Class,String)":
        return "java.lang.Enum"
    return source


def categorize(s: str) -> str:
    """Categorize a class or interface."""
    for n, fs in CATEGORY_FILTERS.items():
        if any([s == f for f in fs]):
            return n

    for n, fs in CATEGORY_FILTERS.items():
        if any([s.startswith(f) for f in fs]):
            return n

    return "uncategorized"


@functools.cache
def get_url_data(url: str) -> str:
    """Download HTML from a URL."""
    print(f"Downloading: {url}")
    r = requests.get(url)
    return r.text


@functools.cache
def get_bs(url: str) -> BeautifulSoup:
    """Return BeautifulSoup of HTML."""
    data = get_url_data(url)
    return BeautifulSoup(data, "html.parser")


def package_url(package_name: str) -> str:
    """Get the URL of a particular Java package."""
    if package_name.startswith("java."):
        return f"https://docs.oracle.com/en/java/javase/17/docs/api/java.base/{package_name.replace('.', '/')}/package-summary.html"
    elif package_name.startswith("io.deephaven."):
        return f"https://deephaven.io/core/javadoc/{package_name.replace('.', '/')}/package-summary.html"
    else:
        raise ValueError(f"Unsupported package name: {package_name}")


def find_all_classes(package_name: str) -> list:
    """Get a list of all classes in a package."""
    url = package_url(package_name)
    bs = get_bs(url)
    bs1 = bs.find("div", {"id": "class-summary.tabpanel"})

    if not bs1:
        return []

    bs2 = bs1.find_all("a")
    rst = []

    for x in bs2:
        t = x.getText()
        if " " in t or "." in t or t[0].islower():
            continue
        rst.append(f"{package_name}.{t}")

    return rst


def doc_url(class_name: str) -> str:
    """Get the URL of a particular class."""
    if class_name.startswith("java."):
        url = f"https://docs.oracle.com/en/java/javase/17/docs/api/java.base/{class_name.replace('.', '/')}.html"
    elif class_name.startswith("io.deephaven."):
        url = f"https://deephaven.io/core/javadoc/{class_name.replace('.', '/')}.html"
    else:
        url = None
    return url.replace("$", ".") if url else None


def method_signature(method) -> str:
    """Obtain the signature of a method."""
    rt = method.getReturnType().getSimpleName()
    args = ",".join([t.getSimpleName() for t in method.getParameterTypes()])

    if method.isVarArgs():
        args = "...".join(args.rsplit("[]", 1))

    return f"{rt}({args})"


def long_signature(method) -> str:
    """Obtain the long signature of a method."""
    rst = method.toString()

    if method.isVarArgs():
        rst = "...".join(rst.rsplit("[]", 1))

    rst = rst.replace("java.lang.Object", "T")
    rst = rst.replace("Object ", "T ")
    rst = rst.replace("java.lang.Comparable", "T")
    rst = rst.replace("Comparable", "T")

    return rst


def fix_descriptions(doc: Optional[str], desc: Optional[str], typ: Optional[str]) -> str:
    """Limits descriptions to 100 characters."""
    if not desc:
        if "Color" in str(doc) and "ColorUtilImpl" not in str(doc) and typ == "CONSTANT":
            desc = "A color."
        else:
            return "No description."
    if len(desc) < 98:
        if not desc.endswith("."):
            desc += "."
    else:
        desc = desc[:97] + "..."
    return " ".join(desc.split())


def remove_html_tags(text: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)


def get_docs(docs: Optional[str]) -> Optional[str]:
    """Get docs for a method."""
    if not docs:
        return None

    if "StaticCalendarMethods" in docs:
        if "calendarDayOfWeek" in docs:
            docs = docs.replace("calendarDayOfWeek", "dayOfWeek")
        elif "calendarTimeZone" in docs:
            docs = docs.replace("calendarTimeZone", "timeZone")
        newdocs = get_docs(docs.replace("StaticCalendarMethods", "Calendar"))
        if not newdocs:
            newdocs = get_docs(docs.replace("StaticCalendarMethods", "BusinessCalendar"))
        return newdocs

    d = docs.split("#")
    url = d[0]
    id = d[1] if len(d) == 2 else None
    bs = get_bs(url)

    if id:
        s = bs.find("section", {"id": id})
        if not s:
            return None
        s = s.find("div", {"class": "block"})
        if not s:
            return None
        return remove_html_tags(str(s))

    s = bs.find("section", {"id": "class-description"})
    if not s:
        return None
    s = s.find("div", {"class": "block"})
    if not s:
        return None
    return remove_html_tags(str(s))


def better_docs(docs: Optional[str], signature: Optional[str]) -> Optional[str]:
    """Improve the docs for any class, constant, or function."""
    if signature:
        if "(" not in signature:
            suffix = signature.split(".")[-1]
        else:
            i1 = signature.index("(")
            i2 = signature.rindex(".", 0, i1)
            suffix = signature[i2 + 1 :]
        return f"{docs}#{suffix}"
    return docs


def sep_signature(sig: Optional[str]) -> Optional[str]:
    """Separate a signature (add a space after any comma)."""
    if sig:
        return sig.replace(",", ", ")
    return sig


def escape_link_text(text: Optional[str]) -> Optional[str]:
    """Escape link text for markdown."""
    if text:
        return text.replace("(", r"\(").replace(")", r"\)").replace("[", r"\[").replace("]", r"\]")
    return text


def generate_page_header(category: str, language: str = "python") -> str:
    """Generate the markdown header for a category page."""
    meta = CATEGORY_METADATA.get(category, {})
    title = meta.get("title", category.replace("-", " ").title())
    description = meta.get("description", "")

    header = f"""---
title: {title}
sidebar_label: {title}
---

{description}

"""
    return header


def generate_page_footer(category: str) -> str:
    """Generate the markdown footer for a category page."""
    return """
## Related documentation

- [Auto-imported functions](./index.md)
- [Query language functions](../../../../how-to-guides/built-in-functions.md)
"""


def write_category_file(category: str, df_rst, output_dir: str):
    """Write a single category file."""
    filename = f"{category}.md"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        f.write(generate_page_header(category))

        n_items = len(df_rst)
        if n_items == 0:
            f.write("No items in this category.\n")
            f.write(generate_page_footer(category))
            return

        types = df_rst["Type"].astype(str).tolist()
        names = df_rst["Name"].astype(str).tolist()
        signatures = df_rst["Signature"].fillna("None").astype(str).tolist()
        docs = df_rst["Docs"].astype(str).tolist()
        descs = df_rst["Description"].astype(str).tolist()

        siglinks = [f"[{escape_link_text(signatures[idx])}](<{docs[idx]}>)" for idx in range(n_items)]

        # Write table header
        f.write("| Type | Name | Signature | Description |\n")
        f.write("| ---- | ---- | --------- | ----------- |\n")

        # Write rows
        for idx in range(n_items):
            f.write(f"| {types[idx]} | {names[idx]} | {siglinks[idx]} | {descs[idx]} |\n")

        f.write(generate_page_footer(category))

    print(f"Wrote: {filepath}")


def main():
    """Main function to generate all documentation."""
    print("Starting auto-import documentation generation...")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Get query library imports
    QLID = jpy.get_type("io.deephaven.engine.table.lang.impl.QueryLibraryImportsDefaults")
    qlid = QLID()
    qlid_packages = [p.getName() for p in qlid.packages().toArray()]
    qlid_classes = [c.getName() for c in qlid.classes().toArray()]
    qlid_statics = [s.getName() for s in qlid.statics().toArray()]

    print(f"Found {len(qlid_packages)} packages, {len(qlid_classes)} classes, {len(qlid_statics)} statics")

    # Expand packages to classes
    for p in qlid_packages:
        cc = find_all_classes(p)
        qlid_classes.extend(cc)

    class_imports = [[categorize(x), x] for x in qlid_classes]
    static_imports = [[categorize(x), x] for x in qlid_statics]

    # Build raw table
    raw = None

    print("Processing class imports...")
    for category, class_import in class_imports:
        doc = doc_url(class_import)
        name = class_import.split(".")[-1]

        c = empty_table(1).update(
            [
                f"Category=`{category}`",
                "Type=`CLASS`",
                f"Source=`{class_import}`",
                f"Docs=`{doc}`",
                f"Name=`{name}`",
                "Signature=(String)null",
                "LongSignature=(String)null",
            ]
        )

        raw = merge([raw, c]) if raw else c

    print("Processing static imports...")
    for category, static_import in static_imports:
        doc = doc_url(static_import)

        c = (
            empty_table(1)
            .update(
                [
                    "Type=`CONSTANT`",
                    f"Category=`{category}`",
                    f"Source=`{static_import}`",
                    f"Docs=`{doc}`",
                    f"Field={static_import}.class.getFields()",
                ]
            )
            .ungroup()
            .where(
                [
                    "java.lang.reflect.Modifier.isStatic(Field.getModifiers())",
                    "java.lang.reflect.Modifier.isPublic(Field.getModifiers())",
                ]
            )
            .update(
                [
                    "Name=Field.getName()",
                    "Signature=Field.getType().getSimpleName()",
                    "LongSignature=Field.toString()",
                ]
            )
            .drop_columns("Field")
        )

        func = (
            empty_table(1)
            .update(
                [
                    "Type=`FUNCTION`",
                    f"Category=`{category}`",
                    f"Source=`{static_import}`",
                    f"Docs=`{doc}`",
                    f"Method={static_import}.class.getMethods()",
                ]
            )
            .ungroup()
            .where(
                [
                    "java.lang.reflect.Modifier.isStatic(Method.getModifiers())",
                    "java.lang.reflect.Modifier.isPublic(Method.getModifiers())",
                ]
            )
            .update(
                [
                    "Name=Method.getName()",
                    "Signature=method_signature(Method)",
                    "LongSignature=long_signature(Method)",
                ]
            )
            .drop_columns("Method")
        )

        raw = merge([raw, c, func]) if raw else merge([c, func])

    print("Processing results...")
    rst = (
        raw.where("!exclude_sources.contains(Source)")
        .update(["Source = remap_source(Source, Name, Signature)"])
        .select()
    )

    rst = (
        rst.last_by(["Category", "Type", "Name", "Signature"])
        .sort(["Category", "Type", "Name", "Signature"])
        .update(
            [
                "Docs=better_docs(Docs,LongSignature)",
                "Description=get_docs(Docs)",
                "Signature = sep_signature(Signature)",
            ]
        )
        .view(["Category", "Type", "Name", "Signature", "Source", "Docs", "Description"])
        .where(["!Name.matches(`valueOf`)", "!Name.matches(`values`)"])
    )

    rst = rst.update(["Description = fix_descriptions(Docs, Description, Type)"])

    print("Writing category files...")
    unique_categories = to_pandas(rst.select_distinct(["Category"]))["Category"].tolist()

    for category in unique_categories:
        df_category = to_pandas(rst.where([f"Category == `{category}`"]))
        write_category_file(category, df_category, OUTPUT_DIR)

    print(f"\nDONE! Files written to {OUTPUT_DIR}")
    print(f"Categories generated: {unique_categories}")


# Run main
main()
