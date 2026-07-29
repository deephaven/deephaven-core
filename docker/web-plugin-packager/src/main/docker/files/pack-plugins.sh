#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

# Output directory where all the extracted plugins will be placed
OUTPUT_DIR=js-plugins

# Output file name for the manifest.json
MANIFEST_FILE=${OUTPUT_DIR}/manifest.json

# Temporary directory name for downloading/unzipping the package into
TMP_DIR=tmp
TMP_PACKAGE_DIR=${TMP_DIR}/package

# Make a output folder
mkdir "${OUTPUT_DIR}"

# Keep track of the directories of all the packed plugins so we can build the manifest.json once everything has
# been downloaded and extracted
PACKAGE_DIRS=()

echo "Packing plugins $@..."

# Iterate through each plugin defined in the plugin list, download the package and move it into place
# Can/should include the version number in the plugin line item
for PACKAGE in "$@"
do
  # Make a temporary directory for downloading/extracting the package into
  mkdir -p "${TMP_DIR}"
  cd "${TMP_DIR}"

  # Download the package
  echo "Downloading package ${PACKAGE}..."

  # Download the package and unzip it
  # Use the wildcard because it's hard to parse out what the actual name of the tar will be
  # Should be the only file in this directory since we just created it
  npm pack "$PACKAGE"
  tar --touch --extract --file *.tgz

  echo "Returning to working dir..."

  # Return to the working dir
  cd -

  echo "In working dir $(pwd)"

  # It always unzips to the folder "package". Get the name from the package so we can move it to the correct
  # location
  PACKAGE_NAME=$(npm pkg get name --prefix="${TMP_PACKAGE_DIR}")
  # Need to remove quotes from the package name
  PACKAGE_NAME="${PACKAGE_NAME%\"}"
  PACKAGE_NAME="${PACKAGE_NAME#\"}"

  echo "Got package name ${PACKAGE_NAME}"

  # Move the plugin to the correct directory
  # Need to make the directory based on the name first
  PACKAGE_DIR=${OUTPUT_DIR}/${PACKAGE_NAME}
  mkdir -p "${PACKAGE_DIR}"
  mv "${TMP_PACKAGE_DIR}/"* "${PACKAGE_DIR}"
  rm --recursive "${TMP_DIR}"
  PACKAGE_DIRS+=("${PACKAGE_DIR}")
done

# Assemble the full manifest in one shot by reading each plugin's package.json. The optional "loader" field is
# preserved as-is so plugin authors cannot shadow core fields like name/version/main.
"${NODE_EXE:-node}" -e '
  const fs = require("fs");
  const path = require("path");
  const plugins = process.argv.slice(1).map((dir) => {
    const pkg = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    const entry = { name: pkg.name, version: pkg.version, main: pkg.main };
    if (pkg.loader && typeof pkg.loader === "object" && !Array.isArray(pkg.loader)
        && Object.keys(pkg.loader).length > 0) {
      entry.loader = pkg.loader;
    }
    return entry;
  });
  process.stdout.write(JSON.stringify({ plugins }, null, 2));
' ${PACKAGE_DIRS[@]+"${PACKAGE_DIRS[@]}"} > "${MANIFEST_FILE}"

echo "Done!"
