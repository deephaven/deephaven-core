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
  echo "✓ Both Python and Groovy docs have changes"
  exit 0
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
