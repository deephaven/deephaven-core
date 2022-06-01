#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os.path

from requests import get

def download_file(url: str, file_name: str) -> str:
    """ download a data file located at the supplied URL and return the file name. """
    if os.path.exists(file_name):
        return file_name

    r = get(url, allow_redirects=True)
    open(file_name, "wb").write(r.content)
    return file_name
