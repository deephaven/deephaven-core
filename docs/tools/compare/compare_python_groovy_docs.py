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

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Baseline management
# ---------------------------------------------------------------------------

def load_baseline(baseline_file: Path) -> dict[str, list[dict]]:
    """Load baseline differences from JSON file.
    
    Args:
        baseline_file: Path to baseline_diffs.json
        
    Returns:
        dict: Maps file path -> list of baseline difference dicts
    """
    if not baseline_file.exists():
        return {}
    
    try:
        with open(baseline_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to load baseline file: {e}", file=sys.stderr)
        return {}


def save_baseline(baseline_file: Path, baseline_data: dict[str, list[dict]]) -> None:
    """Save baseline differences to JSON file.
    
    Args:
        baseline_file: Path to baseline_diffs.json
        baseline_data: Maps file path -> list of difference dicts
    """
    with open(baseline_file, 'w', encoding='utf-8') as f:
        json.dump(baseline_data, f, indent=2, sort_keys=True)
    print(f"\nBaseline saved to: {baseline_file}")
    print(f"Captured {len(baseline_data)} files with {sum(len(diffs) for diffs in baseline_data.values())} total differences")


def is_baseline_diff(diff: dict, baseline_diffs: list[dict]) -> bool:
    """Check if a difference matches one in the baseline.
    
    Args:
        diff: A difference dict from get_line_differences()
        baseline_diffs: List of baseline differences for this file
        
    Returns:
        bool: True if this diff is in the baseline
    """
    for baseline_diff in baseline_diffs:
        # Match by type and content
        if diff.get('type') != baseline_diff.get('type'):
            continue
            
        if diff['type'] == 'changed':
            if (diff.get('python_line_num') == baseline_diff.get('python_line_num') and
                diff.get('groovy_line_num') == baseline_diff.get('groovy_line_num') and
                diff.get('python_content') == baseline_diff.get('python_content') and
                diff.get('groovy_content') == baseline_diff.get('groovy_content')):
                return True
        elif diff['type'] == 'removed':
            if (diff.get('python_line_num') == baseline_diff.get('python_line_num') and
                diff.get('python_content') == baseline_diff.get('python_content')):
                return True
        elif diff['type'] == 'added':
            if (diff.get('groovy_line_num') == baseline_diff.get('groovy_line_num') and
                diff.get('groovy_content') == baseline_diff.get('groovy_content')):
                return True
    
    return False


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


def _compile_equivalent_api_patterns() -> list[re.Pattern[str]]:
    """Compile regex replacement list from _EQUIVALENT_API_PAIRS.

    For each pair, generates patterns matching backticked versions of:
    - The exact name
    - The camelCase variant (underscores removed, following chars uppercased)
    """
    patterns: list[re.Pattern[str]] = []

    def _snake_to_camel(name: str) -> str | None:
        """Convert snake_case to camelCase (e.g., 'weighted_avg' -> 'weightedAvg')."""
        parts = name.split("_")
        # If no underscores, no camelCase variant exists
        if len(parts) <= 1:
            return None
        # Keep first part lowercase, capitalize rest
        return parts[0] + "".join(w.capitalize() for w in parts[1:] if w)

    # For each pair, expand into all plausible surface forms so that e.g.
    # ("agg.weighted_avg", "AggWAvg") generates patterns for:
    #   `agg.weighted_avg`, `agg.weightedAvg`, `weighted_avg`, `weightedAvg`, `AggWAvg`
    for py_name, gr_name in _EQUIVALENT_API_PAIRS:
        # Start with both the Python and Groovy names
        names = {py_name, gr_name}
        
        # Add camelCase variant of the Python name
        camel = _snake_to_camel(py_name)
        if camel:
            names.add(camel)
        
        # Strip agg. prefix and add that variant too
        # This handles cases like "agg.weighted_avg" -> "weighted_avg"
        for n in list(names):
            if n.startswith("agg."):
                names.add(n[4:])

        # Each pattern matches the name wrapped in backticks, e.g. `AggWAvg`
        # We escape special regex chars to match literal strings
        for name in names:
            escaped = re.escape(name)
            patterns.append(re.compile(rf"`{escaped}`"))

    return patterns


# Pre-compiled at import time so we don't rebuild on every normalize_line() call.
_EQUIVALENT_API_COMPILED = _compile_equivalent_api_patterns()

# Normalized replacement string for all equivalent API names (e.g., `agg.weighted_avg`, 
# `AggWAvg`, `weightedAvg` all become this canonical form during normalization).
_NORMALIZED_API_TOKEN = "`EQUIVALENT_API`"


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


def _compile_equivalent_link_patterns() -> list[tuple[re.Pattern[str], str]]:
    """Compile (regex, replacement) pairs from _EQUIVALENT_LINK_SPECS.

    When path_variants is None the pattern matches any markdown link target.
    When path_variants is set the target URL must contain one of the variants
    immediately before ``.md``.
    """
    compiled: list[tuple[re.Pattern[str], str]] = []
    for text_variants, canonical, path_variants in _EQUIVALENT_LINK_SPECS:
        # Build regex alternation for all text variants (e.g., "empty_table|emptyTable")
        text_alt = "|".join(re.escape(v) for v in text_variants)
        
        if path_variants is None:
            # Match any link target: [`text_variant`](...)
            pattern = re.compile(rf"\[`(?:{text_alt})`\]\([^\)]+\)")
        else:
            # Match only if path contains one of the variants before .md
            # Example: [`multiJoin`](./multi-join.md) or [`multiJoin`](./multijoin.md)
            path_alt = "|".join(re.escape(p) for p in path_variants)
            pattern = re.compile(
                rf"\[`(?:{text_alt})`\]\([^\)]*(?:{path_alt})\.md[^\)]*\)"
            )
        
        # Replace all variants with canonical form and placeholder target
        replacement = f"[`{canonical}`](LINK_TARGET)"
        compiled.append((pattern, replacement))
    return compiled


# Pre-compiled at import time alongside the API patterns.
_EQUIVALENT_LINK_COMPILED = _compile_equivalent_link_patterns()


# ---------------------------------------------------------------------------
# Language-specific difference patterns
# ---------------------------------------------------------------------------

# Human-readable pattern strings for Python-specific constructs.
# These are used in fallback heuristics to detect Python-only content.
_PYTHON_SPECIFIC_PATTERN_STRINGS = [
    r"Python\s+\[`with`\]",      # References to Python's with statement
    r"`with`\s+statement",        # Direct mentions of with statement
    r"pandas\s+DataFrames?",      # Pandas DataFrame references
    r"pip[-\s]install",           # pip install commands
    r"\bpip\b",                   # pip package manager
    r"\.py\b",                    # .py file extensions
]

# Human-readable pattern strings for Groovy-specific constructs.
# These are used in fallback heuristics to detect Groovy-only content.
_GROOVY_SPECIFIC_PATTERN_STRINGS = [
    r"try-with-resources",        # Java/Groovy resource management
    r"SafeCloseable",             # Deephaven's SafeCloseable interface
    r"DataFrames?\b(?!\s*\()",    # DataFrame (not followed by parentheses)
    r"\.groovy\b",                # .groovy file extensions
]


def _compile_language_specific_patterns() -> tuple[list[re.Pattern[str]], list[re.Pattern[str]]]:
    """Compile language-specific pattern strings into regex objects.
    
    Returns:
        tuple: (python_patterns, groovy_patterns) - two lists of compiled regex patterns
    """
    python_patterns = [
        re.compile(pattern, re.IGNORECASE) 
        for pattern in _PYTHON_SPECIFIC_PATTERN_STRINGS
    ]
    groovy_patterns = [
        re.compile(pattern, re.IGNORECASE) 
        for pattern in _GROOVY_SPECIFIC_PATTERN_STRINGS
    ]
    return python_patterns, groovy_patterns


# Pre-compiled at import time for use in is_language_specific_difference.
_PYTHON_SPECIFIC_PATTERNS, _GROOVY_SPECIFIC_PATTERNS = _compile_language_specific_patterns()


# ---------------------------------------------------------------------------
# Reference-doc helpers
# ---------------------------------------------------------------------------

def _normalize_reference_param_tables(content: str) -> str:
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
        # Start of a ParamTable block
        if stripped.startswith("<ParamTable"):
            in_param_table = True
            result.append("<ParamTable OMITTED/>")
            continue

        # Inside a ParamTable block - replace all lines with placeholder
        if in_param_table:
            result.append("<ParamTable OMITTED/>")
            # Check if this is the closing tag
            if stripped.startswith("</ParamTable"):
                in_param_table = False
            continue

        # Regular line outside ParamTable - keep as-is
        result.append(line)

    return "\n".join(result)


def is_reference_doc(file_path: Path | str) -> bool:
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
    # Must be a bullet point
    if not stripped.startswith("- "):
        return False
    # Must contain a link with "Pydoc" in the text
    return bool(re.search(r"\[[^\]]*\bPydoc\b[^\]]*\]\([^\)]+\)", stripped))


def _is_groovy_parquet_instructions_line(line: str) -> bool:
    """Return True if line is a Groovy-only Parquet instructions related-doc link."""
    stripped = line.strip()
    # Must be a bullet point
    if not stripped.startswith("- "):
        return False
    # Must be exactly "- [Parquet instructions](...)"
    return bool(re.search(r"^-\s*\[Parquet instructions\]\([^\)]+\)\s*$", stripped))


def _is_markdown_table_line(line: str) -> bool:
    """Return True if line is part of a markdown table (starts with |)."""
    return line.strip().startswith("|")


def _is_omitted_paramtable_line(line: str) -> bool:
    """Return True if line is our ParamTable placeholder."""
    return line.strip() == "<ParamTable OMITTED/>"


def should_exclude_diff_line(line: str, side: str) -> bool:
    """Return True if a lone added/removed diff line should be excluded from the report.

    Some line types are expected to appear on only one side (e.g. Pydoc links
    only in Python, Parquet instructions only in Groovy).  Markdown table rows
    and omitted param-table placeholders are always skipped because they are
    handled by broader normalization elsewhere.

    Args:
        line: The content of the diff line.
        side: ``'python'`` or ``'groovy'``.
    """
    if _is_markdown_table_line(line):
        return True
    if _is_omitted_paramtable_line(line):
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
        """Normalize a single backticked token to lowercase without underscores."""
        # Guard: only transform tokens that look like identifiers (letters,
        # digits, underscores, dots).  Strings like "col1 + col2" are left as-is.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\.]*", token):
            return token
        # Handle dotted paths like "agg.sorted_last" segment by segment.
        # Each segment: remove underscores and lowercase
        parts = token.split(".")
        return ".".join(p.replace("_", "").lower() for p in parts)

    def _repl(match: re.Match[str]) -> str:
        """Replacement function for backticked spans."""
        # Extract the content between backticks, canonicalize it, re-wrap
        return f"`{_canonicalize_token(match.group(1))}`"

    # Find all backticked spans and canonicalize their contents
    return re.sub(r"`([^`]+)`", _repl, text)


def _normalize_snake_case_outside_backticks(text: str) -> str:
    """Convert snake_case words to camelCase outside backticked spans.

    This is used to normalize prose table-name references like ``my_table``
    vs ``myTable``.
    """
    def _snake_to_camel(match: re.Match[str]) -> str:
        """Convert matched snake_case word to camelCase."""
        name = match.group(0)
        parts = name.split("_")
        # Only convert if there are underscores
        if len(parts) > 1:
            return parts[0] + "".join(w.capitalize() for w in parts[1:])
        return name

    # Split on backtick spans so that even-indexed elements are prose and
    # odd-indexed elements are backticked code.  Only transform the prose.
    parts = re.split(r"(`[^`]*`)", text)
    # Process only even-indexed parts (prose, not code)
    for i in range(0, len(parts), 2):
        # Find snake_case words and convert them to camelCase
        parts[i] = re.sub(r"\b[a-z]+_[a-z_]+\b", _snake_to_camel, parts[i])
    return "".join(parts)


def _normalize_link_anchors(text: str) -> str:
    """Canonicalize markdown link anchors by lowercasing and stripping underscores.

    Converts fragments like ``#new_table`` or ``#newTable`` into a canonical
    form so that equivalent anchors compare equal.
    """
    def _canon_anchor(match: re.Match[str]) -> str:
        """Canonicalize a matched anchor fragment."""
        prefix = match.group(1)  # The '#' character
        anchor = match.group(2)  # The anchor name
        # Remove underscores and lowercase for canonical form
        return prefix + anchor.replace("_", "").lower()

    # Find all anchor fragments (#anchor_name) and canonicalize them
    return re.sub(r"(#)([A-Za-z_][A-Za-z0-9_]*)", _canon_anchor, text)


def _normalize_git_commit_hashes(text: str) -> str:
    """Replace git commit hashes and line numbers in GitHub URLs with placeholders."""
    # Pattern: github.com/org/repo/blob/HASH/path/file.java#L123
    # Replace: github.com/org/repo/blob/COMMIT_HASH/path/file.java#LNUM
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
    # Apply all pre-compiled link patterns
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
        text = pattern.sub(_NORMALIZED_API_TOKEN, text)

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

def normalize_code_blocks(content: str) -> tuple[str, list[int]]:
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
    line_map: list[int] = []  # Maps normalized line index -> original line number
    in_code_block = False
    code_block_counter = 0

    for orig_idx, line in enumerate(lines):
        orig_line_num = orig_idx + 1  # Line numbers are 1-indexed

        # Check for code fence markers (```)
        if line.strip().startswith("```"):
            if not in_code_block:
                # Opening fence: start a new code block
                in_code_block = True
                code_block_counter += 1
                placeholder = f"CODE_BLOCK_{code_block_counter}"
                result.append(placeholder)
                line_map.append(orig_line_num)
            else:
                # Closing fence: end the code block (don't output anything)
                in_code_block = False
        elif in_code_block:
            # Inside code block: skip this line entirely
            pass
        else:
            # Regular prose line: normalize and keep it
            result.append(normalize_line(line))
            line_map.append(orig_line_num)

    return "\n".join(result), line_map


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

def get_line_differences(file1_path: Path, file2_path: Path) -> list[dict[str, Any]] | None:
    """Get detailed line-by-line differences between two files.

    Returns a list of diff dicts (or None if the files are equivalent).  Each
    dict has a ``type`` key ("changed", "removed", or "added") plus the
    relevant original line numbers and content from the Python and/or Groovy
    side.
    """
    # Read both files
    try:
        with open(file1_path, "r", encoding="utf-8") as f1:
            content1 = f1.read()
        with open(file2_path, "r", encoding="utf-8") as f2:
            content2 = f2.read()
    except Exception as exc:
        print(f"  WARNING: could not read files: {exc}", file=sys.stderr)
        return None

    # Reference docs: ignore parameter sections (they're expected to differ)
    if is_reference_doc(file1_path) and is_reference_doc(file2_path):
        content1 = _normalize_reference_param_tables(content1)
        content2 = _normalize_reference_param_tables(content2)

    # Collapse code blocks and normalize prose
    content1_norm, line_map1 = normalize_code_blocks(content1)
    content2_norm, line_map2 = normalize_code_blocks(content2)

    # If normalized content is identical, no differences to report
    if content1_norm == content2_norm:
        return None

    # Split into lines for diffing
    lines1_norm = content1_norm.splitlines(keepends=False)
    lines2_norm = content2_norm.splitlines(keepends=False)

    # Keep original lines for reporting
    lines1_orig = content1.splitlines(keepends=False)
    lines2_orig = content2.splitlines(keepends=False)

    # difflib.Differ produces lines prefixed with:
    #   "  " = common to both,  "- " = only in seq1 (Python),
    #   "+ " = only in seq2 (Groovy),  "? " = intraline hint (ignored).
    differ = difflib.Differ()
    diff = list(differ.compare(lines1_norm, lines2_norm))

    differences = []
    # Track our position in the normalized line lists
    line_num_python = 0
    line_num_groovy = 0

    def _orig_line(line_map: list[int], norm_idx: int, orig_lines: list[str], fallback: str) -> tuple[int, str]:
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

    # Process diff output line by line
    i = 0
    while i < len(diff):
        d = diff[i]

        if d.startswith("  "):
            # Common line: advance both counters
            line_num_python += 1
            line_num_groovy += 1
            i += 1

        elif d.startswith("- "):
            # Removed lines (only in Python)
            py_lines = []
            py_nums = []

            # Collect all consecutive removed lines
            while i < len(diff) and diff[i].startswith("- "):
                orig_num, orig_text = _orig_line(
                    line_map1, line_num_python, lines1_orig, diff[i][2:]
                )
                py_nums.append(orig_num)
                py_lines.append(orig_text)
                line_num_python += 1
                i += 1

            # Skip intraline diff hints ("? ")
            while i < len(diff) and diff[i].startswith("? "):
                i += 1

            # Check if there are corresponding added lines (in Groovy)
            gr_lines = []
            gr_nums = []

            if i < len(diff) and diff[i].startswith("+ "):
                # Collect all consecutive added lines
                while i < len(diff) and diff[i].startswith("+ "):
                    orig_num, orig_text = _orig_line(
                        line_map2, line_num_groovy, lines2_orig, diff[i][2:]
                    )
                    gr_nums.append(orig_num)
                    gr_lines.append(orig_text)
                    line_num_groovy += 1
                    i += 1

                # Skip intraline diff hints
                while i < len(diff) and diff[i].startswith("? "):
                    i += 1

                # Paired changes: zip the removed (Python) and added (Groovy)
                # lines 1-to-1.  Lines that survive all skip/language filters
                # are reported as "changed".
                for idx, (py, gr) in enumerate(zip(py_lines, gr_lines)):
                    # Apply all exclusion filters
                    if should_exclude_diff_line(py, "python") and should_exclude_diff_line(gr, "groovy"):
                        continue
                    if _is_markdown_table_line(py) and _is_markdown_table_line(gr):
                        continue
                    if _is_omitted_paramtable_line(py) and _is_omitted_paramtable_line(gr):
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
                    if not should_exclude_diff_line(py_lines[idx], "python"):
                        differences.append({
                            "type": "removed",
                            "python_line_num": py_nums[idx],
                            "python_content": py_lines[idx],
                        })

                # Leftover Groovy lines when more were added than removed
                for idx in range(len(py_lines), len(gr_lines)):
                    if not should_exclude_diff_line(gr_lines[idx], "groovy"):
                        differences.append({
                            "type": "added",
                            "groovy_line_num": gr_nums[idx],
                            "groovy_content": gr_lines[idx],
                        })
            else:
                # Removed-only lines (no corresponding Groovy additions)
                for idx, py in enumerate(py_lines):
                    if not should_exclude_diff_line(py, "python"):
                        differences.append({
                            "type": "removed",
                            "python_line_num": py_nums[idx],
                            "python_content": py,
                        })

        elif d.startswith("+ "):
            # Added-only lines (no corresponding Python removals)
            gr_lines = []
            gr_nums = []

            # Collect all consecutive added lines
            while i < len(diff) and diff[i].startswith("+ "):
                orig_num, orig_text = _orig_line(
                    line_map2, line_num_groovy, lines2_orig, diff[i][2:]
                )
                gr_nums.append(orig_num)
                gr_lines.append(orig_text)
                line_num_groovy += 1
                i += 1

            # Skip intraline diff hints
            while i < len(diff) and diff[i].startswith("? "):
                i += 1

            # Report each added line that passes filters
            for idx, gr in enumerate(gr_lines):
                if not should_exclude_diff_line(gr, "groovy"):
                    differences.append({
                        "type": "added",
                        "groovy_line_num": gr_nums[idx],
                        "groovy_content": gr,
                    })

        elif d.startswith("? "):
            # Intraline diff hint: skip it
            i += 1
        else:
            # Unexpected line type: skip it
            i += 1

    # Return differences list, or None if empty
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
        # Use double backticks with spaces to escape internal backticks
        return f"`` {text} ``"
    # Simple case: wrap in single backticks
    return f"`{text}`"


def _format_collapsed_diff_block(f: Any, diffs: list[dict[str, Any]], start: int, end: int, side: str) -> None:
    """Format and write a collapsed block of consecutive added/removed lines to output stream."""
    # Determine which dict keys to use based on side
    key_line = "python_line_num" if side == "python" else "groovy_line_num"
    key_content = "python_content" if side == "python" else "groovy_content"
    other_side = "Groovy" if side == "python" else "Python"
    label = f"{side.capitalize()} only"

    # Get the line range for the header
    start_line = diffs[start][key_line]
    end_line = diffs[end - 1][key_line]

    # Write header (single line vs range)
    if end - start == 1:
        f.write(f"- **Line {start_line} ({label})**\n")
    else:
        f.write(f"- **Lines {start_line}-{end_line} ({label})**\n")

    # Write each line's content
    for idx in range(start, end):
        escaped = _escape_inline_backticks(diffs[idx][key_content])
        f.write(f"  - {side.capitalize()}: {escaped}\n")

    # Indicate the other side doesn't have these lines
    f.write(f"  - {other_side}: *[line not present]*\n\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def is_diff_marker_line(line: str) -> bool:
    """Check if a line is a diff marker (Line X or Lines X-Y)."""
    stripped = line.strip()
    return stripped.startswith('- **Line ') or stripped.startswith('- **Lines ')


def parse_line_marker(line: str) -> tuple[int | None, int | None, str | None]:
    """Parse a line marker and return (start_line, end_line, marker_type).
    
    Returns:
        tuple: (start_line, end_line, marker_type) where marker_type is 'single', 'range', or None
    """
    stripped = line.strip()
    
    # Try single line: "- **Line 42**" or "- **Line 42 (Python only)**"
    match = re.match(r'- \*\*Line (\d+)(?:\s+\([^)]+\))?\*\*', stripped)
    if match:
        line_num = int(match.group(1))
        return (line_num, line_num, 'single')
    
    # Try range: "- **Lines 39-40 (Groovy only)**"
    match = re.match(r'- \*\*Lines (\d+)-(\d+)(?:\s+\([^)]+\))?\*\*', stripped)
    if match:
        start = int(match.group(1))
        end = int(match.group(2))
        return (start, end, 'range')
    
    return (None, None, None)


def create_annotated_comparison(docs_dir: Path) -> None:
    """Create an annotated comparison file from line-by-line diff output.
    
    Args:
        docs_dir: Directory containing the comparison file
    """
    # Read the comparison file
    comparison_file = docs_dir / "docs_line_comparison.md"
    if not comparison_file.exists():
        print(f"ERROR: Comparison file not found: {comparison_file}", file=sys.stderr)
        print("Please run without --annotate first to generate the comparison file.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(comparison_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"ERROR: Failed to read comparison file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Create annotated output
    output_file = docs_dir / "docs_line_comparison_annotated.md"
    
    stats = {
        'total_differences': 0,
        'single_line_diffs': 0,
        'multi_line_diffs': 0,
        'total_files': 0,
        'malformed_markers': 0
    }
    
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write("# LINE-BY-LINE COMPARISON: Python vs Groovy Documentation\n\n")
        out.write("**Filtering:** Only excludes language-specific implementation details and git commit hashes\n\n")
        out.write("## Files with Line Differences\n\n")

        current_file = None
        current_file_stats = {'single_line': 0, 'multi_line': 0}
        in_file_section = False

        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Check for file markers
            if line.strip().startswith('## FILE:'):
                # Extract the file path
                file_path = line.strip().replace('## FILE:', '').strip()
                
                # Write stats for previous file if exists
                if current_file and in_file_section:
                    out.write(f"\n**File Summary:** ")
                    out.write(f"Single-line differences: {current_file_stats['single_line']}, ")
                    out.write(f"Multi-line differences: {current_file_stats['multi_line']}\n\n")
                    out.write("---\n\n")
                
                # Start new file section
                current_file = file_path
                current_file_stats = {'single_line': 0, 'multi_line': 0}
                in_file_section = True
                stats['total_files'] += 1
                
                out.write(line)
                i += 1
                continue
            
            # Check for line difference markers
            if is_diff_marker_line(line) and current_file:
                start_line, end_line, marker_type = parse_line_marker(line)
                
                if marker_type is None:
                    # Malformed marker - log warning and write as-is
                    print(f"WARNING: Malformed diff marker in {current_file}: {line.strip()}", file=sys.stderr)
                    stats['malformed_markers'] += 1
                    out.write(line)
                    i += 1
                    continue
                
                # Write the line marker
                out.write(line)
                
                # Update statistics
                if marker_type == 'single':
                    current_file_stats['single_line'] += 1
                    stats['single_line_diffs'] += 1
                elif marker_type == 'range':
                    current_file_stats['multi_line'] += 1
                    stats['multi_line_diffs'] += 1
                
                stats['total_differences'] += 1
                i += 1
                continue
            
            # Write all other lines as-is
            out.write(line)
            i += 1
        
        # Write stats for last file
        if current_file and in_file_section:
            out.write(f"\n**File Summary:** ")
            out.write(f"Single-line differences: {current_file_stats['single_line']}, ")
            out.write(f"Multi-line differences: {current_file_stats['multi_line']}\n\n")
            out.write("---\n\n")
        
        # Write overall summary at the end
        out.write("\n\n# OVERALL SUMMARY\n\n")
        out.write(f"**Total files with differences:** {stats['total_files']}\n\n")
        out.write(f"**Breakdown by type:**\n")
        out.write(f"- Single-line differences: {stats['single_line_diffs']}\n")
        out.write(f"- Multi-line differences: {stats['multi_line_diffs']}\n")
        out.write(f"- **Total differences:** {stats['total_differences']}\n\n")
        
        if stats['malformed_markers'] > 0:
            out.write(f"**Warnings:** {stats['malformed_markers']} malformed diff markers found (see stderr)\n\n")
    
    print("=" * 80)
    print("ANNOTATED COMPARISON FILE CREATED")
    print("=" * 80)
    print(f"Total files: {stats['total_files']}")
    print(f"Total differences: {stats['total_differences']}")
    print(f"  - Single-line: {stats['single_line_diffs']}")
    print(f"  - Multi-line: {stats['multi_line_diffs']}")
    if stats['malformed_markers'] > 0:
        print(f"\nWarnings: {stats['malformed_markers']} malformed diff markers")
    print(f"\nOutput saved to: {output_file}")


def main(baseline_data: dict[str, list[dict]] | None = None, generate_baseline_files: list[str] | None = None) -> dict[str, list[dict]]:
    """Run the main comparison.
    
    Args:
        baseline_data: Optional baseline to filter against. If provided, only new diffs are reported.
        generate_baseline_files: Optional list of specific files to generate baseline for.
                                If None and generating baseline, all files are included.
    
    Returns:
        dict: Maps file path -> list of differences (for baseline generation)
    """
    # Set up directory paths
    script_dir = Path(__file__).parent
    docs_dir = script_dir.parent.parent  # Go up to docs/ from tools/compare/
    python_docs_dir = docs_dir / "python"
    groovy_docs_dir = docs_dir / "groovy"
    
    # Load baseline if not provided
    if baseline_data is None:
        baseline_file = script_dir / "baseline_diffs.json"
        baseline_data = load_baseline(baseline_file)
        if baseline_data:
            print(f"Loaded baseline with {len(baseline_data)} files\n")

    # Scan Python documentation directory
    print("Scanning Python docs...")
    python_files = {}  # Maps relative path -> absolute Path object
    for filepath in python_docs_dir.rglob("*.md"):
        rel = str(filepath.relative_to(python_docs_dir))
        # Skip reference docs (except community-questions)
        if should_exclude_reference_path(rel):
            continue
        # Skip numpy/pandas reference docs
        if rel.startswith("reference/numpy/") or rel.startswith("reference/pandas/"):
            continue
        python_files[rel] = filepath

    # Scan Groovy documentation directory
    print("Scanning Groovy docs...")
    groovy_files = {}  # Maps relative path -> absolute Path object
    for filepath in groovy_docs_dir.rglob("*.md"):
        rel = str(filepath.relative_to(groovy_docs_dir))
        # Skip reference docs (except community-questions)
        if should_exclude_reference_path(rel):
            continue
        groovy_files[rel] = filepath

    # Report scan results
    print(f"\nFound {len(python_files)} Python docs")
    print(f"Found {len(groovy_files)} Groovy docs")

    # Categorize files: common, Python-only, Groovy-only
    common_files = set(python_files.keys()) & set(groovy_files.keys())
    python_only = sorted(set(python_files.keys()) - set(groovy_files.keys()))
    groovy_only = sorted(set(groovy_files.keys()) - set(python_files.keys()))

    print(f"\nComparing {len(common_files)} common files (excluding code blocks)...\n")

    # Compare each common file and collect differences
    all_diffs = {}  # Maps relative path -> list of ALL diff dicts (for baseline generation)
    files_with_diffs = {}  # Maps relative path -> list of NEW diff dicts (after baseline filtering)
    
    for rel_path in sorted(common_files):
        # If generating baseline for specific files, skip files not in the list
        if generate_baseline_files is not None and rel_path not in generate_baseline_files:
            continue
            
        differences = get_line_differences(python_files[rel_path], groovy_files[rel_path])
        
        if differences:
            # Store all diffs for baseline generation
            all_diffs[rel_path] = differences
            
            # Filter out baseline diffs if baseline exists
            if baseline_data and rel_path in baseline_data:
                baseline_diffs = baseline_data[rel_path]
                new_diffs = [d for d in differences if not is_baseline_diff(d, baseline_diffs)]
                if new_diffs:
                    files_with_diffs[rel_path] = new_diffs
            else:
                # No baseline for this file, report all diffs
                files_with_diffs[rel_path] = differences

    # Write results to markdown report
    output_file = docs_dir / "docs_line_comparison.md"
    with open(output_file, "w", encoding="utf-8") as f:
        # Write header and summary
        f.write("# LINE-BY-LINE COMPARISON: Python vs Groovy Documentation\n\n")
        f.write("**Filtering:** Only excludes language-specific implementation details and git commit hashes\n\n")
        f.write(f"**Files with line differences:** {len(files_with_diffs)}\n")
        f.write(f"**Files only in Python:** {len(python_only)}\n")
        f.write(f"**Files only in Groovy:** {len(groovy_only)}\n\n")
        f.write("---\n\n")

        # Section 1: Files with line differences
        f.write("## Files with Line Differences\n\n")

        for rel_path in sorted(files_with_diffs.keys()):
            differences = files_with_diffs[rel_path]

            # Write file header with links
            f.write(f"## FILE: {rel_path}\n\n")
            f.write(f"**{len(differences)} difference(s)**\n\n")
            f.write(f"[Python](./python/{rel_path}) | [Groovy](./groovy/{rel_path})\n\n")

            # Process differences for this file
            i = 0
            while i < len(differences):
                diff = differences[i]

                if diff["type"] == "changed":
                    # Single line changed between Python and Groovy
                    f.write(f"- **Line {diff['python_line_num']}**\n")
                    f.write(f"  - Python: {_escape_inline_backticks(diff['python_content'])}\n")
                    f.write(f"  - Groovy: {_escape_inline_backticks(diff['groovy_content'])}\n\n")
                    i += 1

                elif diff["type"] == "removed":
                    # Collect consecutive removed lines (Python-only)
                    j = i + 1
                    while (
                        j < len(differences)
                        and differences[j]["type"] == "removed"
                        and differences[j]["python_line_num"]
                        == differences[j - 1]["python_line_num"] + 1
                    ):
                        j += 1
                    # Write as a collapsed block
                    _format_collapsed_diff_block(f, differences, i, j, "python")
                    i = j

                elif diff["type"] == "added":
                    # Collect consecutive added lines (Groovy-only)
                    j = i + 1
                    while (
                        j < len(differences)
                        and differences[j]["type"] == "added"
                        and differences[j]["groovy_line_num"]
                        == differences[j - 1]["groovy_line_num"] + 1
                    ):
                        j += 1
                    # Write as a collapsed block
                    _format_collapsed_diff_block(f, differences, i, j, "groovy")
                    i = j

            # Separator between files
            f.write("---\n\n")

        # Section 2: Files only in Python
        f.write("## Files Only in Python (missing in Groovy)\n\n")
        if python_only:
            # List each Python-only file with a link
            for rel_path in python_only:
                f.write(f"- [{rel_path}](./python/{rel_path})\n")
            f.write("\n")
        else:
            f.write("*No Python-only files*\n\n")

        f.write("---\n\n")

        # Section 3: Files only in Groovy
        f.write("## Files Only in Groovy (missing in Python)\n\n")
        if groovy_only:
            # List each Groovy-only file with a link
            for rel_path in groovy_only:
                f.write(f"- [{rel_path}](./groovy/{rel_path})\n")
            f.write("\n")
        else:
            f.write("*No Groovy-only files*\n\n")

    # Calculate total line differences across all files
    total_line_diffs = sum(len(d) for d in files_with_diffs.values())

    # Print summary to console
    print(f"Results saved to: {output_file}")
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Files with line differences: {len(files_with_diffs)}")
    print(f"Files with line differences: {len(files_with_diffs)}")
    print(f"Files only in Python: {len(python_only)}")
    print(f"Files only in Groovy: {len(groovy_only)}")
    
    # Return all diffs for baseline generation
    return all_diffs


# Script entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare Python and Groovy documentation files line-by-line.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full comparison (generates both raw and annotated output)
  python3 compare_python_groovy_docs.py
  
  # Generate only raw comparison (skip annotation step)
  python3 compare_python_groovy_docs.py --raw-only
  
  # Generate baseline from current differences (all files)
  python3 compare_python_groovy_docs.py --generate-baseline
  
  # Generate baseline for specific files only
  python3 compare_python_groovy_docs.py --generate-baseline --files "how-to-guides/filters.md" "conceptual/dag.md"
        """
    )
    parser.add_argument(
        '--raw-only',
        action='store_true',
        help='Generate only the raw comparison file without creating the annotated version'
    )
    parser.add_argument(
        '--generate-baseline',
        action='store_true',
        help='Generate baseline file from current differences instead of running comparison'
    )
    parser.add_argument(
        '--files',
        nargs='+',
        metavar='FILE',
        help='Specific files to include when generating baseline (relative to docs/)'
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    docs_dir = script_dir.parent.parent
    baseline_file = script_dir / "baseline_diffs.json"
    
    # Handle baseline generation mode
    if args.generate_baseline:
        print("=" * 80)
        if args.files:
            print(f"Generating baseline for {len(args.files)} specific file(s)...")
        else:
            print("Generating baseline for all files with differences...")
        print("=" * 80 + "\n")
        
        # Run comparison without baseline filtering to capture all diffs
        all_diffs = main(baseline_data={}, generate_baseline_files=args.files)
        
        if args.files:
            # Load existing baseline and merge with new files
            existing_baseline = load_baseline(baseline_file)
            for file_path in args.files:
                if file_path in all_diffs:
                    existing_baseline[file_path] = all_diffs[file_path]
                    print(f"Updated baseline for: {file_path}")
                else:
                    print(f"No differences found for: {file_path}")
            save_baseline(baseline_file, existing_baseline)
        else:
            # Save all diffs as baseline
            save_baseline(baseline_file, all_diffs)
        
        sys.exit(0)
    
    # Normal comparison mode
    main()
    
    # Unless --raw-only is specified, also create the annotated version
    if not args.raw_only:
        print("\n" + "=" * 80)
        print("Creating annotated comparison...")
        print("=" * 80 + "\n")
        
        create_annotated_comparison(docs_dir)
