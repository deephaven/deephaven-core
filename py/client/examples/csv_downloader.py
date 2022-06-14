#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os.path

from requests import get


def download_csv(url: str, file_name=None) -> str:
    """ download a CSV located at the supplied URL and return the file name. """
    if not file_name:
        file_name = "sample.csv"

    if os.path.exists(file_name):
        return file_name

    r = get(url, allow_redirects=True)
    open(file_name, "wb").write(r.content)
    return file_name
