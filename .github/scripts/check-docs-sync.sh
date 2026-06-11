#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

python_files=()
groovy_files=()

for file in ${CHANGED_FILES}; do
  if [[ "$file" == docs/python/* ]]; then
    python_files+=("$file")
  elif [[ "$file" == docs/groovy/* ]]; then
    groovy_files+=("$file")
  fi
done

has_python_changes=false
has_groovy_changes=false

if [ ${#python_files[@]} -gt 0 ]; then
  has_python_changes=true
fi

if [ ${#groovy_files[@]} -gt 0 ]; then
  has_groovy_changes=true
fi

if [ "$has_python_changes" == "false" ] && [ "$has_groovy_changes" == "false" ]; then
  echo "No Python or Groovy docs changes detected"
  exit 0
fi

if [ "$has_python_changes" == "true" ] && [ "$has_groovy_changes" == "true" ]; then
  unmatched_python=()
  unmatched_groovy=()

  # Check that each changed Python doc has a corresponding changed Groovy doc
  for file in "${python_files[@]}"; do
    corresponding_groovy="${file/docs\/python/docs\/groovy}"
    match_found=false
    for gfile in "${groovy_files[@]}"; do
      if [[ "$gfile" == "$corresponding_groovy" ]]; then
        match_found=true
        break
      fi
    done
    if [ "$match_found" == "false" ]; then
      unmatched_python+=("$file")
    fi
  done

  # Check that each changed Groovy doc has a corresponding changed Python doc
  for file in "${groovy_files[@]}"; do
    corresponding_python="${file/docs\/groovy/docs\/python}"
    match_found=false
    for pfile in "${python_files[@]}"; do
      if [[ "$pfile" == "$corresponding_python" ]]; then
        match_found=true
        break
      fi
    done
    if [ "$match_found" == "false" ]; then
      unmatched_groovy+=("$file")
    fi
  done

  # Allow unmatched files only when the appropriate "No*DocsChangesNeeded" label is set
  if [ "${HAS_NoGroovyDocsChangesNeeded}" == "true" ]; then
    unmatched_python=()
  fi
  if [ "${HAS_NoPythonDocsChangesNeeded}" == "true" ]; then
    unmatched_groovy=()
  fi

  if [ ${#unmatched_python[@]} -eq 0 ] && [ ${#unmatched_groovy[@]} -eq 0 ]; then
    echo "✓ Python and Groovy docs changes are in sync"
    exit 0
  fi

  if [ ${#unmatched_python[@]} -gt 0 ]; then
    >&2 echo "ERROR: The following Python docs were modified without corresponding Groovy doc changes:"
    for file in "${unmatched_python[@]}"; do
      >&2 echo "  - $file (expected corresponding: ${file/docs\/python/docs\/groovy})"
    done
    >&2 echo ""
    >&2 echo "Please either update the corresponding Groovy documentation files or add the 'NoGroovyDocsChangesNeeded' label if Groovy changes are not applicable."
  fi

  if [ ${#unmatched_groovy[@]} -gt 0 ]; then
    >&2 echo "ERROR: The following Groovy docs were modified without corresponding Python doc changes:"
    for file in "${unmatched_groovy[@]}"; do
      >&2 echo "  - $file (expected corresponding: ${file/docs\/groovy/docs\/python})"
    done
    >&2 echo ""
    >&2 echo "Please either update the corresponding Python documentation files or add the 'NoPythonDocsChangesNeeded' label if Python changes are not applicable."
  fi

  exit 1
fi

if [ "$has_python_changes" == "true" ] && [ "$has_groovy_changes" == "false" ]; then
  if [ "${HAS_NoGroovyDocsChangesNeeded}" == "true" ]; then
    echo "✓ Python docs changed, Groovy changes not needed (label applied)"
    exit 0
  fi
  
  echo "Python docs changed files:"
  for file in "${python_files[@]}"; do
    echo "  - $file"
    corresponding_groovy="${file/docs\/python/docs\/groovy}"
    if [ -f "$corresponding_groovy" ]; then
      echo "    → Corresponding Groovy file exists: $corresponding_groovy"
    fi
  done
  
  >&2 echo ""
  >&2 echo "ERROR: Python docs were modified but corresponding Groovy docs were not updated."
  >&2 echo "Please either:"
  >&2 echo "  1. Update the corresponding Groovy documentation files, or"
  >&2 echo "  2. Add the 'NoGroovyDocsChangesNeeded' label if Groovy changes are not applicable"
  exit 1
fi

if [ "$has_groovy_changes" == "true" ] && [ "$has_python_changes" == "false" ]; then
  if [ "${HAS_NoPythonDocsChangesNeeded}" == "true" ]; then
    echo "✓ Groovy docs changed, Python changes not needed (label applied)"
    exit 0
  fi
  
  echo "Groovy docs changed files:"
  for file in "${groovy_files[@]}"; do
    echo "  - $file"
    corresponding_python="${file/docs\/groovy/docs\/python}"
    if [ -f "$corresponding_python" ]; then
      echo "    → Corresponding Python file exists: $corresponding_python"
    fi
  done
  
  >&2 echo ""
  >&2 echo "ERROR: Groovy docs were modified but corresponding Python docs were not updated."
  >&2 echo "Please either:"
  >&2 echo "  1. Update the corresponding Python documentation files, or"
  >&2 echo "  2. Add the 'NoPythonDocsChangesNeeded' label if Python changes are not applicable"
  exit 1
fi

exit 0
