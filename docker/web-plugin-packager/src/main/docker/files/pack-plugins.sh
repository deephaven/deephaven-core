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

# Start the manifest.json file. It will add all the dependencies automatically
echo "{\"plugins\":[" > "${MANIFEST_FILE}"

# Keep track of the count so we add a comma when necessary
PLUGIN_COUNT=0

echo "Packing plugins $@..."

# Iterate through each plugin defined in the plugin list, download the package and adding info to the manifest
# Can/should include the version number in the plugin line item
for PACKAGE in "$@"
do
  # Add a comma to the manifest.json if this is not the first plugin
  if [ $PLUGIN_COUNT -gt 0 ]; then
    echo "," >> "${MANIFEST_FILE}"
  fi

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

  # It always unzips to the folder "package". Get the name and version info from the package so we can move it to
  # the correct location and add the info to the manifest
  PACKAGE_NAME=$(npm pkg get name --prefix="${TMP_PACKAGE_DIR}")
  PACKAGE_INFO=$(npm pkg get name version main --prefix="${TMP_PACKAGE_DIR}")
  # Need to remove quotes from the package name
  PACKAGE_NAME="${PACKAGE_NAME%\"}"
  PACKAGE_NAME="${PACKAGE_NAME#\"}"

  echo "Got package name ${PACKAGE_NAME}"

  # Add the info to the manifest file
  echo "${PACKAGE_INFO}" >> "${MANIFEST_FILE}"

  # Move the plugin to the correct directory
  # Need to make the directory based on the name first
  mkdir -p "${OUTPUT_DIR}/${PACKAGE_NAME}"
  mv "${TMP_PACKAGE_DIR}/"* "${OUTPUT_DIR}/${PACKAGE_NAME}"
  rm --recursive "${TMP_DIR}"

  # Increment the plugin count
  PLUGIN_COUNT=$((PLUGIN_COUNT + 1))
done

echo "Done!"

# Close out the manifest file
echo "]}" >> "${MANIFEST_FILE}"
