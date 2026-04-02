#!/usr/bin/env python3
"""Compare Python and Groovy documentation files line-by-line in Deephaven Core.

This version uses minimal filtering - only excludes:
1. Language-specific implementation details (Python vs Groovy syntax)
2. Git commit hash differences in URLs

Overall approach:
    1. Discover all .md files under docs/python/ and docs/groovy/, pair them by
       relative path, and identify files that exist only on one side.
    2. For each paired file, collapse code blocks (``` ... ```) into single-line
       placeholders so that expected code differences don't produce noise.
    3. Normalize every non-code line through a shared pipeline (`normalize_line`)
       that erases known language-specific variations (naming conventions, link
       targets, terminology, etc.).
    4. Run `difflib.Differ` on the normalized content to find structural
       differences, then map each diff back to original line numbers/content.
    5. Apply a second filter (`is_language_specific_difference`) on each changed
       line pair, plus skip predicates for lines that are allowed to differ
       (Pydoc links, markdown tables, etc.).
    6. Write surviving differences to a markdown report.
"""

import re
import sys
from pathlib import Path
import difflib
from docs_utils import load_exclusion_list


# ---------------------------------------------------------------------------
# Equivalent API mappings (data-driven)
# ---------------------------------------------------------------------------

# Maps (python_name, groovy_name) pairs. Variants with/without the `agg.`
# prefix and with underscores removed are generated automatically — only list
# the canonical form here.
_EQUIVALENT_API_PAIRS = [
    # (python_name,          groovy_name)
    ("ring_table",           "RingTableTools.of"),
    ("consume",              "consumeToTable"),
    # Aggregation specs
    ("agg.abs_sum",          "AggAbsSum"),
    ("agg.avg",              "AggAvg"),
    ("agg.count",            "AggCount"),
    ("agg.count_",           "AggCount"),
    ("agg.count_distinct",   "AggCountDistinct"),
    ("agg.count_where",      "AggCountWhere"),
    ("agg.distinct",         "AggDistinct"),
    # NOTE: "agg.ditinct" covers a known typo in existing docs
    ("agg.ditinct",          "AggDistinct"),
    ("agg.first",            "AggFirst"),
    ("agg.formula",          "AggFormula"),
    ("agg.group",            "AggGroup"),
    ("agg.Group",            "AggGroup"),
    ("agg.last",             "AggLast"),
    ("agg.max_",             "AggMax"),
    ("agg.max",              "AggMax"),
    ("agg.median",           "AggMed"),
    ("agg.min_",             "AggMin"),
    ("agg.pct",              "AggPct"),
    ("agg.sorted_first",     "AggSortedFirst"),
    ("agg.sorted_last",      "AggSortedLast"),
    ("agg.std",              "AggStd"),
    ("agg.sum_",             "AggSum"),
    ("agg.unique",           "AggUnique"),
    ("agg.var",              "AggVar"),
    ("agg.weighted_avg",     "AggWAvg"),
    ("agg.weighted_sum",     "AggWSum"),
    # Short-form aggregation names (without agg. prefix)
    ("abs_sum",              "AggAbsSum"),
    ("count_",               "AggCount"),
    ("count",                "AggCount"),
    ("count_distinct",       "AggCountDistinct"),
    ("count_where",          "AggCountWhere"),
    ("formula",              "AggFormula"),
    ("agg_all_by",           "AggAllBy"),
    ("avg_by",               "avgBy"),
]


def _build_equivalent_api_patterns():
    """Build compiled regex replacement list from _EQUIVALENT_API_PAIRS.

    For each pair, generates patterns matching backticked versions of:
    - The exact name
    - The camelCase variant (underscores removed, following chars uppercased)
    """
    patterns = []

    def _snake_to_camel(name):
        parts = name.split("_")
        if len(parts) <= 1:
            return None
        return parts[0] + "".join(w.capitalize() for w in parts[1:] if w)

    # For each pair, expand into all plausible surface forms so that e.g.
    # ("agg.weighted_avg", "AggWAvg") generates patterns for:
    #   `agg.weighted_avg`, `agg.weightedAvg`, `weighted_avg`, `weightedAvg`, `AggWAvg`
    for py_name, gr_name in _EQUIVALENT_API_PAIRS:
        names = {py_name, gr_name}
        # Add camelCase variant of the Python name
        camel = _snake_to_camel(py_name)
        if camel:
            names.add(camel)
        # Strip agg. prefix and add that variant too
        for n in list(names):
            if n.startswith("agg."):
                names.add(n[4:])

        # Each pattern matches the name wrapped in backticks, e.g. `AggWAvg`
        for name in names:
            escaped = re.escape(name)
            patterns.append(re.compile(rf"`{escaped}`"))

    return patterns


# Pre-compiled at import time so we don't rebuild on every normalize_line() call.
_EQUIVALENT_API_COMPILED = _build_equivalent_api_patterns()

# All equivalent API backtick spans get replaced with this single token.
PLACEHOLDER_API = "`EQUIVALENT_API`"


# ---------------------------------------------------------------------------
# Equivalent markdown-link mappings (data-driven)
# ---------------------------------------------------------------------------

# Each entry: (text_variants, canonical_text, path_variants)
#   text_variants  - alternate backtick-link text forms, e.g. ["empty_table", "emptyTable"]
#   canonical_text - the replacement link text (without backticks)
#   path_variants  - if not None, the link target must contain one of these
#                    before ".md"; if None, any link target matches.
_EQUIVALENT_LINK_SPECS: list[tuple[list[str], str, list[str] | None]] = [
    (["empty_table", "emptyTable"],                        "emptyTable",              None),
    (["new_table", "newTable"],                             "newTable",                None),
    (["multi_join", "multiJoin", "MultiJoin.of"],           "MultiJoin",               ["multi-join", "multiJoin", "multijoin"]),
    (["range_join", "rangeJoin"],                           "rangeJoin",               ["range-join", "rangeJoin"]),
    (["keyed_transpose", "keyedTranspose"],                 "keyedTranspose",          ["keyed-transpose", "keyedTranspose"]),
    (["partitioned_transform", "partitionedTransform"],     "partitionedTransform",    ["partitioned-transform", "partitionedTransform"]),
    (["one_click", "oneClick"],                             "oneClick",                ["one-click", "oneClick"]),
    (["has_data_index", "hasDataIndex"],                    "hasDataIndex",            ["has-data-index", "hasDataIndex"]),
    (["meta_table", "metaTable", "meta"],                   "meta",                    ["meta_table", "metaTable", "meta"]),
]


def _build_equivalent_link_patterns():
    """Build compiled (regex, replacement) pairs from _EQUIVALENT_LINK_SPECS.

    When path_variants is None the pattern matches any markdown link target.
    When path_variants is set the target URL must contain one of the variants
    immediately before ``.md``.
    """
    compiled = []
    for text_variants, canonical, path_variants in _EQUIVALENT_LINK_SPECS:
        text_alt = "|".join(re.escape(v) for v in text_variants)
        if path_variants is None:
            pattern = re.compile(rf"\[`(?:{text_alt})`\]\([^\)]+\)")
        else:
            path_alt = "|".join(re.escape(p) for p in path_variants)
            pattern = re.compile(
                rf"\[`(?:{text_alt})`\]\([^\)]*(?:{path_alt})\.md[^\)]*\)"
            )
        replacement = f"[`{canonical}`](LINK_TARGET)"
        compiled.append((pattern, replacement))
    return compiled


# Pre-compiled at import time alongside the API patterns.
_EQUIVALENT_LINK_COMPILED = _build_equivalent_link_patterns()


# ---------------------------------------------------------------------------
# Language-specific difference patterns
# ---------------------------------------------------------------------------

# Pre-compiled patterns for fallback heuristics in is_language_specific_difference.
# These detect Python-specific and Groovy-specific constructs.
_PYTHON_SPECIFIC_PATTERNS = [
    re.compile(r"Python\s+\[`with`\]", re.IGNORECASE),
    re.compile(r"`with`\s+statement", re.IGNORECASE),
    re.compile(r"pandas\s+DataFrames?", re.IGNORECASE),
    re.compile(r"pip[-\s]install", re.IGNORECASE),
    re.compile(r"\bpip\b", re.IGNORECASE),
    re.compile(r"\.py\b", re.IGNORECASE),
]

_GROOVY_SPECIFIC_PATTERNS = [
    re.compile(r"try-with-resources", re.IGNORECASE),
    re.compile(r"SafeCloseable", re.IGNORECASE),
    re.compile(r"DataFrames?\b(?!\s*\()", re.IGNORECASE),
    re.compile(r"\.groovy\b", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Reference-doc helpers
# ---------------------------------------------------------------------------

def normalize_reference_param_tables(content):
    """Normalize <ParamTable> blocks in reference docs.

    Parameter sections are expected to differ between Python and Groovy reference docs.
    To avoid spurious diffs while preserving line numbers, replace every line inside
    <ParamTable>...</ParamTable> with a stable placeholder line.
    """
    lines = content.splitlines(keepends=False)
    result = []
    in_param_table = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("<ParamTable"):
            in_param_table = True
            result.append("<ParamTable OMITTED/>")
            continue

        if in_param_table:
            result.append("<ParamTable OMITTED/>")
            if stripped.startswith("</ParamTable"):
                in_param_table = False
            continue

        result.append(line)

    return "\n".join(result)


def is_reference_doc(file_path):
    """Return True if the given path is a reference doc under docs/{python,groovy}/reference/."""
    return "reference" in Path(file_path).parts


def should_exclude_reference_path(rel_path_str: str) -> bool:
    """Return True if this relative path should be excluded due to being in /reference.

    Exclude everything under reference/, except reference/community-questions/.
    """
    if not rel_path_str.startswith("reference/"):
        return False
    return not rel_path_str.startswith("reference/community-questions/")


# ---------------------------------------------------------------------------
# Line-level skip predicates
# ---------------------------------------------------------------------------

def _is_pydoc_link_line(line: str) -> bool:
    """Return True if line is a Pydoc link bullet (allowed only in Python docs)."""
    stripped = line.strip()
    if not stripped.startswith("- "):
        return False
    return bool(re.search(r"\[[^\]]*\bPydoc\b[^\]]*\]\([^\)]+\)", stripped))


def _is_groovy_parquet_instructions_line(line: str) -> bool:
    """Return True if line is a Groovy-only Parquet instructions related-doc link."""
    stripped = line.strip()
    if not stripped.startswith("- "):
        return False
    return bool(re.search(r"^-\s*\[Parquet instructions\]\([^\)]+\)\s*$", stripped))


def is_markdown_table_line(line: str) -> bool:
    return line.strip().startswith("|")


def is_omitted_paramtable_line(line: str) -> bool:
    return line.strip() == "<ParamTable OMITTED/>"


def should_skip_diff_line(line: str, side: str) -> bool:
    """Return True if a lone added/removed diff line should be silently skipped.

    Some line types are expected to appear on only one side (e.g. Pydoc links
    only in Python, Parquet instructions only in Groovy).  Markdown table rows
    and omitted param-table placeholders are always skipped because they are
    handled by broader normalization elsewhere.

    Args:
        line: The content of the diff line.
        side: ``'python'`` or ``'groovy'``.
    """
    if is_markdown_table_line(line):
        return True
    if is_omitted_paramtable_line(line):
        return True
    if side == "python" and _is_pydoc_link_line(line):
        return True
    if side == "groovy" and _is_groovy_parquet_instructions_line(line):
        return True
    return False


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _canonicalize_backticked_identifiers(text: str) -> str:
    """Canonicalize identifier style inside backticks.

    Treats snake_case and camelCase identifiers as equivalent by removing
    underscores and lowercasing.  Limited to backticked spans to avoid
    rewriting prose.

    Examples: `group_by` == `groupBy`, `agg.sorted_last` == `agg.sortedLast`
    """
    def _canonicalize_token(token: str) -> str:
        # Guard: only transform tokens that look like identifiers (letters,
        # digits, underscores, dots).  Strings like "col1 + col2" are left as-is.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\.]*", token):
            return token
        # Handle dotted paths like "agg.sorted_last" segment by segment.
        parts = token.split(".")
        return ".".join(p.replace("_", "").lower() for p in parts)

    def _repl(match):
        return f"`{_canonicalize_token(match.group(1))}`"

    return re.sub(r"`([^`]+)`", _repl, text)


def _normalize_snake_case_outside_backticks(text: str) -> str:
    """Convert snake_case words to camelCase outside backticked spans.

    This is used to normalize prose table-name references like ``my_table``
    vs ``myTable``.
    """
    def _snake_to_camel(match):
        name = match.group(0)
        parts = name.split("_")
        if len(parts) > 1:
            return parts[0] + "".join(w.capitalize() for w in parts[1:])
        return name

    # Split on backtick spans so that even-indexed elements are prose and
    # odd-indexed elements are backticked code.  Only transform the prose.
    parts = re.split(r"(`[^`]*`)", text)
    for i in range(0, len(parts), 2):
        parts[i] = re.sub(r"\b[a-z]+_[a-z_]+\b", _snake_to_camel, parts[i])
    return "".join(parts)


def _normalize_link_anchors(text: str) -> str:
    """Canonicalize markdown link anchors by lowercasing and stripping underscores.

    Converts fragments like ``#new_table`` or ``#newTable`` into a canonical
    form so that equivalent anchors compare equal.
    """
    def _canon_anchor(match):
        prefix = match.group(1)
        anchor = match.group(2)
        return prefix + anchor.replace("_", "").lower()

    return re.sub(r"(#)([A-Za-z_][A-Za-z0-9_]*)", _canon_anchor, text)


def _normalize_git_commit_hashes(text: str) -> str:
    """Replace git commit hashes and line numbers in GitHub URLs with placeholders."""
    return re.sub(
        r"(github\.com/[^/]+/[^/]+/blob/)([0-9a-f]{40})(/.+?\.java)(#L\d+)?",
        r"\1COMMIT_HASH\3#LNUM",
        text,
    )


def _normalize_equivalent_markdown_links(text: str) -> str:
    """Normalize markdown links for functionally equivalent API names.

    Backtick-link pairs (e.g. [`empty_table`](...) vs [`emptyTable`](...)) are
    handled by the data-driven ``_EQUIVALENT_LINK_COMPILED`` patterns.  Prose
    link variants and the Agg catch-all are handled by explicit regexes below.
    """
    # Data-driven backtick-link normalization
    for pattern, replacement in _EQUIVALENT_LINK_COMPILED:
        text = pattern.sub(replacement, text)

    # --- Prose link variants (not backticked, can't be data-driven easily) ---

    # "Create a new table" / "Create an empty table" / "Create new tables"
    text = re.sub(
        r"\[Create (?:a new|an empty|new) tables?\]\([^\)]*new-and-empty-table\.md[^\)]*\)",
        "[Create a new table](LINK_TARGET)",
        text,
    )

    # Python/Groovy variables in query strings
    text = re.sub(
        r"\[(?:Python|Groovy) variables in query strings\]\([^\)]+(?:python-variables|groovy-variables)\.md[^\)]*\)",
        "[LANGUAGE variables in query strings](LINK_TARGET)",
        text,
    )
    # Python functions / Groovy closures in query strings
    text = re.sub(
        r"\[(?:Python functions|Groovy closures) in query strings\]\([^\)]+(?:python-functions|groovy-closures)\.md[^\)]*\)",
        "[LANGUAGE callables in query strings](LINK_TARGET)",
        text,
    )
    # Python/Groovy classes in query strings
    text = re.sub(
        r"\[(?:Python|Groovy) classes in query strings\]\([^\)]+(?:python-classes|groovy-classes)\.md[^\)]*\)",
        "[LANGUAGE classes in query strings](LINK_TARGET)",
        text,
    )

    # Aggregation spec reference links (Agg*.md) catch-all
    text = re.sub(
        r"\[`[^`]+`\]\([^\)]*(Agg[A-Za-z0-9]+)\.md[^\)]*\)",
        r"[`\1`](LINK_TARGET)",
        text,
    )

    return text


def normalize_line(text: str) -> str:
    """Apply the full normalization pipeline to a single line of text.

    This is the **single source of truth** for all normalizations.  It is called
    both by ``normalize_code_blocks`` (to prepare content for structural diffing
    via ``difflib.Differ``) and by ``is_language_specific_difference`` (to decide
    whether a detected diff is actually language-specific noise).

    Ordering matters:
        1. Formatting fixes (bold-backtick, whitespace) first so later regexes
           see cleaner input.
        2. Markdown-link normalization before identifier canonicalization,
           because the link regexes match on the original casing/underscore
           style (e.g. ``empty_table``).
        3. Equivalent-API replacement before backtick canonicalization, because
           canonicalization lowercases everything and would prevent the
           case-sensitive API patterns (e.g. ``AggWAvg``) from matching.
        4. Everything else (anchors, assets, language names, terminology) is
           order-independent.
    """
    # --- 1. Formatting fixes ---

    # Bold-backtick formatting: **`foo`** -> **foo**
    text = re.sub(r"\*\*`([^`]+)`\*\*", r"**\1**", text)

    # Missing whitespace after backticked API identifiers with a dot
    # e.g. "`agg.avg`returns" -> "`agg.avg` returns"
    text = re.sub(r"(`[^`]*\.[^`]*`)([A-Za-z])", r"\1 \2", text)

    # Collapse multi-space runs in markdown table rows so alignment padding
    # doesn't cause false diffs.
    if text.lstrip().startswith("|"):
        text = re.sub(r" +", " ", text)

    # --- 2. Markdown link normalization (case-sensitive, runs first) ---
    text = _normalize_equivalent_markdown_links(text)

    # --- 3. Equivalent API names (case-sensitive, before canonicalization) ---
    for pattern in _EQUIVALENT_API_COMPILED:
        text = pattern.sub(PLACEHOLDER_API, text)

    # --- 4. Identifier canonicalization (destructive: lowercases + strips _) ---
    text = _canonicalize_backticked_identifiers(text)

    # Snake-case outside backticks (prose table names, etc.)
    text = _normalize_snake_case_outside_backticks(text)

    # Link anchors
    text = _normalize_link_anchors(text)

    # Git commit hashes
    text = _normalize_git_commit_hashes(text)

    # Asset paths
    text = re.sub(r"\.\./assets/[^/\)]+/", r"../assets/FOLDER/", text)

    # Image markdown: normalize to alt-text only
    text = re.sub(r"!\[([^\]]+)\]\([^\)]+\)", r"![IMAGE:\1]", text)

    # Language names
    text = re.sub(r"\bpython\b", "LANGUAGE", text, flags=re.IGNORECASE)
    text = re.sub(r"\bgroovy\b", "LANGUAGE", text, flags=re.IGNORECASE)

    # vector/array terminology
    text = re.sub(r"\bvectors?\b", "GROUPED_VALUES", text, flags=re.IGNORECASE)
    text = re.sub(r"\barrays?\b", "GROUPED_VALUES", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\b(?:a|an)\s+GROUPED_VALUES\b", "GROUPED_VALUES", text, flags=re.IGNORECASE
    )

    # Pydoc / Javadoc
    text = re.sub(r"\bPydoc\b", "APIDOC", text)
    text = re.sub(r"\bJavadoc\b", "APIDOC", text)
    text = re.sub(r"/core/pydoc/[^\")\s]+", "/core/APIDOC/PATH", text)
    text = re.sub(r"/core/javadoc/[^\")\s]+", "/core/APIDOC/PATH", text)

    return text


# ---------------------------------------------------------------------------
# Language-specific line comparison
# ---------------------------------------------------------------------------

def is_language_specific_difference(line1: str, line2: str) -> bool:
    """Return True if two lines differ only due to Python-vs-Groovy specifics.

    First tries normalization (the primary filter). If normalized forms still
    differ, falls back to pattern-based heuristics: if line1 contains a
    Python-specific construct *and* line2 contains a Groovy-specific construct,
    the pair is assumed to be a language-specific rewrite of the same idea.
    """
    norm1 = normalize_line(line1)
    norm2 = normalize_line(line2)

    if norm1 == norm2:
        return True

    # Fallback heuristic: both sides must match language-specific patterns.
    has_python = any(p.search(line1) for p in _PYTHON_SPECIFIC_PATTERNS)
    has_groovy = any(p.search(line2) for p in _GROOVY_SPECIFIC_PATTERNS)

    return has_python and has_groovy


# ---------------------------------------------------------------------------
# Code-block collapsing
# ---------------------------------------------------------------------------

def normalize_code_blocks(content: str):
    """Replace fenced code blocks with numbered placeholders.

    Code examples are expected to differ between Python and Groovy docs, so we
    collapse each entire ``` ... ``` block into a single line like
    ``CODE_BLOCK_3``.  Lines outside code blocks are run through
    ``normalize_line`` so the diff sees only the normalized prose.

    Returns:
        tuple: (normalized_content, line_map) where ``line_map[i]`` gives the
        original 1-based line number for normalized line ``i`` (0-indexed list).
        This is needed later to report diff results using original line numbers.
    """
    lines = content.split("\n")
    result = []
    line_map: list[int] = []
    in_code_block = False
    code_block_counter = 0

    for orig_idx, line in enumerate(lines):
        orig_line_num = orig_idx + 1

        if line.strip().startswith("```"):
            if not in_code_block:
                in_code_block = True
                code_block_counter += 1
                placeholder = f"CODE_BLOCK_{code_block_counter}"
                result.append(placeholder)
                line_map.append(orig_line_num)
            else:
                in_code_block = False
        elif in_code_block:
            pass
        else:
            result.append(normalize_line(line))
            line_map.append(orig_line_num)

    return "\n".join(result), line_map


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

def get_line_differences(file1_path, file2_path):
    """Get detailed line-by-line differences between two files.

    Returns a list of diff dicts (or None if the files are equivalent).  Each
    dict has a ``type`` key ("changed", "removed", or "added") plus the
    relevant original line numbers and content from the Python and/or Groovy
    side.
    """
    try:
        with open(file1_path, "r", encoding="utf-8") as f1:
            content1 = f1.read()
        with open(file2_path, "r", encoding="utf-8") as f2:
            content2 = f2.read()
    except Exception as exc:
        print(f"  WARNING: could not read files: {exc}", file=sys.stderr)
        return None

    # Reference docs: ignore parameter sections
    if is_reference_doc(file1_path) and is_reference_doc(file2_path):
        content1 = normalize_reference_param_tables(content1)
        content2 = normalize_reference_param_tables(content2)

    content1_norm, line_map1 = normalize_code_blocks(content1)
    content2_norm, line_map2 = normalize_code_blocks(content2)

    if content1_norm == content2_norm:
        return None

    lines1_norm = content1_norm.splitlines(keepends=False)
    lines2_norm = content2_norm.splitlines(keepends=False)

    lines1_orig = content1.splitlines(keepends=False)
    lines2_orig = content2.splitlines(keepends=False)

    # difflib.Differ produces lines prefixed with:
    #   "  " = common to both,  "- " = only in seq1 (Python),
    #   "+ " = only in seq2 (Groovy),  "? " = intraline hint (ignored).
    differ = difflib.Differ()
    diff = list(differ.compare(lines1_norm, lines2_norm))

    differences = []
    line_num_python = 0
    line_num_groovy = 0

    def _orig_line(line_map, norm_idx, orig_lines, fallback):
        """Map a normalized-line index back to the original line number and text.

        Because code blocks were collapsed and some lines removed, the index
        into the normalized list doesn't equal the original line number.  The
        ``line_map`` bridges between the two coordinate systems.
        """
        if norm_idx < len(line_map):
            orig_num = line_map[norm_idx]
            if orig_num <= len(orig_lines):
                return orig_num, orig_lines[orig_num - 1]
        return norm_idx + 1, fallback

    i = 0
    while i < len(diff):
        d = diff[i]

        if d.startswith("  "):
            line_num_python += 1
            line_num_groovy += 1
            i += 1

        elif d.startswith("- "):
            py_lines = []
            py_nums = []

            while i < len(diff) and diff[i].startswith("- "):
                orig_num, orig_text = _orig_line(
                    line_map1, line_num_python, lines1_orig, diff[i][2:]
                )
                py_nums.append(orig_num)
                py_lines.append(orig_text)
                line_num_python += 1
                i += 1

            while i < len(diff) and diff[i].startswith("? "):
                i += 1

            gr_lines = []
            gr_nums = []

            if i < len(diff) and diff[i].startswith("+ "):
                while i < len(diff) and diff[i].startswith("+ "):
                    orig_num, orig_text = _orig_line(
                        line_map2, line_num_groovy, lines2_orig, diff[i][2:]
                    )
                    gr_nums.append(orig_num)
                    gr_lines.append(orig_text)
                    line_num_groovy += 1
                    i += 1

                while i < len(diff) and diff[i].startswith("? "):
                    i += 1

                # Paired changes: zip the removed (Python) and added (Groovy)
                # lines 1-to-1.  Lines that survive all skip/language filters
                # are reported as "changed".
                for idx, (py, gr) in enumerate(zip(py_lines, gr_lines)):
                    if should_skip_diff_line(py, "python") and should_skip_diff_line(gr, "groovy"):
                        continue
                    if is_markdown_table_line(py) and is_markdown_table_line(gr):
                        continue
                    if is_omitted_paramtable_line(py) and is_omitted_paramtable_line(gr):
                        continue
                    if not is_language_specific_difference(py, gr):
                        differences.append({
                            "type": "changed",
                            "python_line_num": py_nums[idx],
                            "groovy_line_num": gr_nums[idx],
                            "python_content": py,
                            "groovy_content": gr,
                        })

                # Leftover Python lines when more were removed than added
                for idx in range(len(gr_lines), len(py_lines)):
                    if not should_skip_diff_line(py_lines[idx], "python"):
                        differences.append({
                            "type": "removed",
                            "python_line_num": py_nums[idx],
                            "python_content": py_lines[idx],
                        })

                # Leftover Groovy lines when more were added than removed
                for idx in range(len(py_lines), len(gr_lines)):
                    if not should_skip_diff_line(gr_lines[idx], "groovy"):
                        differences.append({
                            "type": "added",
                            "groovy_line_num": gr_nums[idx],
                            "groovy_content": gr_lines[idx],
                        })
            else:
                # Removed-only lines
                for idx, py in enumerate(py_lines):
                    if not should_skip_diff_line(py, "python"):
                        differences.append({
                            "type": "removed",
                            "python_line_num": py_nums[idx],
                            "python_content": py,
                        })

        elif d.startswith("+ "):
            gr_lines = []
            gr_nums = []

            while i < len(diff) and diff[i].startswith("+ "):
                orig_num, orig_text = _orig_line(
                    line_map2, line_num_groovy, lines2_orig, diff[i][2:]
                )
                gr_nums.append(orig_num)
                gr_lines.append(orig_text)
                line_num_groovy += 1
                i += 1

            while i < len(diff) and diff[i].startswith("? "):
                i += 1

            for idx, gr in enumerate(gr_lines):
                if not should_skip_diff_line(gr, "groovy"):
                    differences.append({
                        "type": "added",
                        "groovy_line_num": gr_nums[idx],
                        "groovy_content": gr,
                    })

        elif d.startswith("? "):
            i += 1
        else:
            i += 1

    return differences if differences else None


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _escape_inline_backticks(text: str) -> str:
    """Escape content for display inside markdown inline code.

    If the text contains backticks, wrap it in double-backticks with spacing
    so the markdown renders correctly.
    """
    if "`" in text:
        return f"`` {text} ``"
    return f"`{text}`"


def _write_collapsed_block(f, diffs, start, end, side):
    """Write a collapsed block of consecutive added/removed lines."""
    key_line = "python_line_num" if side == "python" else "groovy_line_num"
    key_content = "python_content" if side == "python" else "groovy_content"
    other_side = "Groovy" if side == "python" else "Python"
    label = f"{side.capitalize()} only"

    start_line = diffs[start][key_line]
    end_line = diffs[end - 1][key_line]

    if end - start == 1:
        f.write(f"- **Line {start_line} ({label})**\n")
    else:
        f.write(f"- **Lines {start_line}-{end_line} ({label})**\n")

    for idx in range(start, end):
        escaped = _escape_inline_backticks(diffs[idx][key_content])
        f.write(f"  - {side.capitalize()}: {escaped}\n")

    f.write(f"  - {other_side}: *[line not present]*\n\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    script_dir = Path(__file__).parent
    python_docs_dir = script_dir / "python"
    groovy_docs_dir = script_dir / "groovy"

    excluded_files = load_exclusion_list(script_dir)
    if excluded_files:
        print(f"Loaded {len(excluded_files)} excluded files from excluded_files.txt\n")

    print("Scanning Python docs...")
    python_files = {}
    for filepath in python_docs_dir.rglob("*.md"):
        rel = str(filepath.relative_to(python_docs_dir))
        if should_exclude_reference_path(rel):
            continue
        if rel.startswith("reference/numpy/") or rel.startswith("reference/pandas/"):
            continue
        if rel in excluded_files:
            continue
        python_files[rel] = filepath

    print("Scanning Groovy docs...")
    groovy_files = {}
    for filepath in groovy_docs_dir.rglob("*.md"):
        rel = str(filepath.relative_to(groovy_docs_dir))
        if should_exclude_reference_path(rel):
            continue
        if rel in excluded_files:
            continue
        groovy_files[rel] = filepath

    print(f"\nFound {len(python_files)} Python docs")
    print(f"Found {len(groovy_files)} Groovy docs")

    common_files = set(python_files.keys()) & set(groovy_files.keys())
    python_only = sorted(set(python_files.keys()) - set(groovy_files.keys()))
    groovy_only = sorted(set(groovy_files.keys()) - set(python_files.keys()))

    print(f"\nComparing {len(common_files)} common files (excluding code blocks)...\n")

    files_with_diffs = {}
    for rel_path in sorted(common_files):
        differences = get_line_differences(python_files[rel_path], groovy_files[rel_path])
        if differences:
            files_with_diffs[rel_path] = differences

    # Write results
    output_file = script_dir / "docs_line_comparison_v5.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("# LINE-BY-LINE COMPARISON: Python vs Groovy Documentation (v5)\n\n")
        f.write("**Filtering:** Only excludes language-specific implementation details and git commit hashes\n\n")
        f.write(f"**Files with line differences:** {len(files_with_diffs)}\n")
        f.write(f"**Files only in Python:** {len(python_only)}\n")
        f.write(f"**Files only in Groovy:** {len(groovy_only)}\n\n")
        f.write("---\n\n")

        # Section 1: Files with line differences
        f.write("## Files with Line Differences\n\n")

        for rel_path in sorted(files_with_diffs.keys()):
            differences = files_with_diffs[rel_path]

            f.write(f"## FILE: {rel_path}\n\n")
            f.write(f"**{len(differences)} difference(s)**\n\n")
            f.write(f"[Python](./python/{rel_path}) | [Groovy](./groovy/{rel_path})\n\n")

            i = 0
            while i < len(differences):
                diff = differences[i]

                if diff["type"] == "changed":
                    f.write(f"- **Line {diff['python_line_num']}**\n")
                    f.write(f"  - Python: {_escape_inline_backticks(diff['python_content'])}\n")
                    f.write(f"  - Groovy: {_escape_inline_backticks(diff['groovy_content'])}\n\n")
                    i += 1

                elif diff["type"] == "removed":
                    # Collect consecutive removed lines
                    j = i + 1
                    while (
                        j < len(differences)
                        and differences[j]["type"] == "removed"
                        and differences[j]["python_line_num"]
                        == differences[j - 1]["python_line_num"] + 1
                    ):
                        j += 1
                    _write_collapsed_block(f, differences, i, j, "python")
                    i = j

                elif diff["type"] == "added":
                    j = i + 1
                    while (
                        j < len(differences)
                        and differences[j]["type"] == "added"
                        and differences[j]["groovy_line_num"]
                        == differences[j - 1]["groovy_line_num"] + 1
                    ):
                        j += 1
                    _write_collapsed_block(f, differences, i, j, "groovy")
                    i = j

            f.write("---\n\n")

        # Section 2: Files only in Python
        f.write("## Files Only in Python (missing in Groovy)\n\n")
        if python_only:
            for rel_path in python_only:
                f.write(f"- [{rel_path}](./python/{rel_path})\n")
            f.write("\n")
        else:
            f.write("*No Python-only files*\n\n")

        f.write("---\n\n")

        # Section 3: Files only in Groovy
        f.write("## Files Only in Groovy (missing in Python)\n\n")
        if groovy_only:
            for rel_path in groovy_only:
                f.write(f"- [{rel_path}](./groovy/{rel_path})\n")
            f.write("\n")
        else:
            f.write("*No Groovy-only files*\n\n")

    total_line_diffs = sum(len(d) for d in files_with_diffs.values())

    print(f"Results saved to: {output_file}")
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Files with line differences: {len(files_with_diffs)}")
    print(f"Total individual line differences: {total_line_diffs}")
    print(f"Files only in Python: {len(python_only)}")
    print(f"Files only in Groovy: {len(groovy_only)}")


if __name__ == "__main__":
    main()
