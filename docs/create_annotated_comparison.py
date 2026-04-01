#!/usr/bin/env python3
"""Create an annotated comparison file from line-by-line diff output."""

import re
import sys
from pathlib import Path
from docs_utils import load_exclusion_list


def is_diff_marker_line(line: str) -> bool:
    """Check if a line is a diff marker (Line X or Lines X-Y)."""
    stripped = line.strip()
    return stripped.startswith('- **Line ') or stripped.startswith('- **Lines ')


def parse_line_marker(line: str):
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

def main():
    script_dir = Path(__file__).parent
    
    # Load exclusion list
    excluded_files = load_exclusion_list(script_dir)
    
    # Read the v5 comparison file
    comparison_file = script_dir / "docs_line_comparison_v5.md"
    if not comparison_file.exists():
        print(f"ERROR: Comparison file not found: {comparison_file}", file=sys.stderr)
        print("Please run compare_docs_v5.py first to generate the comparison file.", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(comparison_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"ERROR: Failed to read comparison file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Create annotated output
    output_file = script_dir / "docs_line_comparison_annotated.md"
    
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
                # Extract the file path and check if it's excluded
                file_path = line.strip().replace('## FILE:', '').strip()
                
                # Skip this entire file section if it's in the exclusion list
                if file_path in excluded_files:
                    # Skip until we hit the next file marker or end of file
                    i += 1
                    while i < len(lines) and not lines[i].strip().startswith('## FILE:'):
                        i += 1
                    continue
                
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

if __name__ == "__main__":
    main()
