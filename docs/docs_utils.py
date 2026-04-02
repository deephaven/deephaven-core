#!/usr/bin/env python3
"""Shared utilities for documentation comparison scripts."""

from pathlib import Path


def load_exclusion_list(script_dir):
    """Load list of files to exclude from comparison.
    
    Args:
        script_dir: Path to the directory containing excluded_files.txt
        
    Returns:
        set: Set of file paths (relative to docs/) to exclude
    """
    exclusion_file = script_dir / "excluded_files.txt"
    excluded = set()
    
    if exclusion_file.exists():
        with open(exclusion_file, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    excluded.add(line)
    
    return excluded
