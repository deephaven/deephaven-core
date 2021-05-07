#!/usr/bin/env bash

function upload_ftp {
    echo Uploading ${1}: ${2}
    curl --ftp-create-dirs -T $2 -u "$FTP_USER:$FTP_PASSWORD" "ftp://$FTP_HOST/software/$TRAVIS_OS_NAME/"
}

echo "success pushing artifacts to FTP..."
upload_ftp "wheel" "dist/*.whl"
