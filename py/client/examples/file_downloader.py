#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import os.path

from requests import get


def download_file(url: str, file_name, reuse_existing=False):
    """ download a file located at the supplied URL and store it in file_name.
    However, if the file named file_name already exists, and reuse_existing is true,
    then return immediately.
    """
    if os.path.exists(file_name):
        return

    r = get(url, allow_redirects=True)
    open(file_name, "wb").write(r.content)
