#!/bin/bash
#
# Check if auto-imported function documentation is in sync with the code.
# This script runs the generator inside a Deephaven Docker container and
# compares the output with the committed documentation.
#
# Usage: ./check_autoimport_sync.sh [python|groovy|both] [image_tag]
#
# Arguments:
#   target    - Which docs to check: python, groovy, or both (default: both)
#   image_tag - Docker image tag to use (default: edge)
#
# Environment variables:
#   DEEPHAVEN_IMAGE_TAG - Alternative way to specify image tag
#
# Exit codes:
#   0 - Documentation is in sync
#   1 - Documentation is out of sync (differences found)
#   2 - Script error

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TARGET="${1:-both}"
IMAGE_TAG="${2:-${DEEPHAVEN_IMAGE_TAG:-edge}}"
IMAGE="ghcr.io/deephaven/server:${IMAGE_TAG}"

# Directories
PYTHON_DOCS_DIR="$REPO_ROOT/docs/python/reference/query-language/query-library/auto-imported"
GROOVY_DOCS_DIR="$REPO_ROOT/docs/groovy/reference/query-language/query-library/auto-imported"
TEMP_DIR=$(mktemp -d)
OUTPUT_DIR="$TEMP_DIR/autoimport_output"

cleanup() {
    echo "Cleaning up..."
    if [ -n "$CONTAINER_ID" ]; then
        docker stop "$CONTAINER_ID" 2>/dev/null || true
        docker rm "$CONTAINER_ID" 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}

trap cleanup EXIT

echo "=== Auto-Import Documentation Sync Check ==="
echo "Repository root: $REPO_ROOT"
echo "Target: $TARGET"
echo "Server image: $IMAGE"
echo "Temp directory: $TEMP_DIR"
echo ""

# Copy the generator script to temp directory
cp "$SCRIPT_DIR/generate_autoimport_docs.py" "$TEMP_DIR/"

# Modify the script to output to /data/autoimport_output
# (already configured in the script)

echo "Starting Deephaven server..."
CONTAINER_ID=$(docker run -d \
    --rm \
    -v "$TEMP_DIR:/data" \
    -p 10042:10000 \
    "$IMAGE")

echo "Container ID: $CONTAINER_ID"
echo "Waiting for server to be ready..."

# Wait for server to be ready (up to 120 seconds)
MAX_WAIT=120
WAIT_COUNT=0
until curl -s http://localhost:10042/ide/get_heap_info > /dev/null 2>&1; do
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo "ERROR: Server did not start within $MAX_WAIT seconds"
        docker logs "$CONTAINER_ID"
        exit 2
    fi
    echo "  Waiting... ($WAIT_COUNT/$MAX_WAIT seconds)"
done

echo "Server is ready!"
echo ""
echo "Running generator script (this may take several minutes)..."

# Run the script inside the container
docker exec "$CONTAINER_ID" python -c "exec(open('/data/generate_autoimport_docs.py').read())"

echo ""
echo "Generation complete. Comparing output..."
echo ""

# Check if output was generated
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "ERROR: Output directory not created"
    exit 2
fi

# Compare function
compare_docs() {
    local lang="$1"
    local docs_dir="$2"
    local has_diff=0
    
    echo "=== Checking $lang documentation ==="
    
    if [ ! -d "$docs_dir" ]; then
        echo "WARNING: Documentation directory does not exist: $docs_dir"
        return 1
    fi
    
    # Compare each generated file (excluding index.md which has language-specific content)
    for generated_file in "$OUTPUT_DIR"/*.md; do
        filename=$(basename "$generated_file")
        
        # Skip index.md as it has language-specific content
        if [ "$filename" = "index.md" ]; then
            continue
        fi
        
        committed_file="$docs_dir/$filename"
        
        if [ ! -f "$committed_file" ]; then
            echo "  MISSING: $filename not found in $lang docs"
            has_diff=1
            continue
        fi
        
        # Compare files (ignoring trailing whitespace)
        if ! diff -q -B "$generated_file" "$committed_file" > /dev/null 2>&1; then
            echo "  CHANGED: $filename"
            echo "    Diff preview (first 20 lines):"
            diff -u "$committed_file" "$generated_file" | head -30 | sed 's/^/      /'
            has_diff=1
        else
            echo "  OK: $filename"
        fi
    done
    
    return $has_diff
}

EXIT_CODE=0

if [ "$TARGET" = "python" ] || [ "$TARGET" = "both" ]; then
    if ! compare_docs "Python" "$PYTHON_DOCS_DIR"; then
        EXIT_CODE=1
    fi
    echo ""
fi

if [ "$TARGET" = "groovy" ] || [ "$TARGET" = "both" ]; then
    if ! compare_docs "Groovy" "$GROOVY_DOCS_DIR"; then
        EXIT_CODE=1
    fi
    echo ""
fi

if [ $EXIT_CODE -eq 0 ]; then
    echo "=== SUCCESS: All auto-import documentation is in sync ==="
else
    echo "=== FAILURE: Auto-import documentation is out of sync ==="
    echo ""
    echo "To update the documentation, run:"
    echo "  1. Follow the instructions in docs/tools/autoimport/README.md"
    echo "  2. Copy the generated files to the appropriate docs directory"
    echo "  3. Run ./docs/format && ./docs/updateSnapshots"
fi

exit $EXIT_CODE
