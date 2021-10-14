#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The deephaven.csv module supports reading an external CSV file into a Deephaven table and writing a
Deephaven table out as a CSV file.
"""
from enum import Enum
from typing import Dict, Optional

import jpy

from deephaven2 import DHError
from deephaven2.dtypes import DType
from deephaven2.table import Table

_csv_helper_cls = jpy.get_type("io.deephaven.db.tables.utils.CsvHelpers")
_csv_specs_cls = jpy.get_type("io.deephaven.db.tables.utils.csv.CsvSpecs")
_table_header_cls = jpy.get_type("io.deephaven.qst.table.TableHeader")
_inference_specs = jpy.get_type("io.deephaven.db.tables.utils.csv.InferenceSpecs")
_j_Charset = jpy.get_type("java.nio.charset.Charset")


class Inference(Enum):
    """ An Enum of predefined inference rules.
    """
    MINIMAL = _inference_specs.minimal()
    STANDARD = _inference_specs.standard()
    STANDARD_TIMES = _inference_specs.standardTimes()


def _build_header(header: Dict[str, DType] = None):
    if not header:
        return None

    table_header_builder = _table_header_cls.builder()
    for k, v in header.items():
        table_header_builder.putHeaders(k, v.value)

    return table_header_builder.build()


def read_csv(path: str,
             header: Dict[str, DType] = None,
             inference: Inference = Inference.STANDARD_TIMES,
             headless: bool = False,
             delimiter: str = ",",
             quote: str = "\"",
             ignore_surrounding_spaces: bool = True,
             trim: bool = True,
             charset: str = "utf-8") -> Table:
    """ read the CSV data specified by the path parameter.

    Args:
        path (str): a file path or a URL string
        header (Dict): a dict to define the table columns with key being the name, value being the data type
        inference (csv.Inference): an Enum value specifying the rules for data type inference, default is STANDARD_TIMES
        headless (bool): indicates is the CSV data headless, default is False
        delimiter (str): the delimiter used by the CSV, default is the comma
        quote (str): the quote character for the CSV, default is double quote
        ignore_surrounding_spaces (bool): indicates whether surrounding white space should be ignored for text field,
            default is True
        trim (bool) : indicates whether to trim white space inside a quoted string
        charset (str): the name of the charset used for the CSV data

    Returns:
        a Table object

    Raises:
        DHError
    """
    try:
        csv_specs_builder = _csv_specs_cls.builder()

        # build the head spec
        table_header = _build_header(header)
        if table_header:
            csv_specs_builder.header(table_header)

        csv_specs = (csv_specs_builder.inference(inference.value)
                     .hasHeaderRow(not headless)
                     .delimiter(ord(delimiter))
                     .quote(ord(quote))
                     .ignoreSurroundingSpaces(ignore_surrounding_spaces)
                     .trim(trim)
                     .charset(_j_Charset.forName(charset))
                     .build())

        db_table = _csv_helper_cls.readCsv(path, csv_specs)

        if not header:
            db_table = _csv_helper_cls.readCsv(path)
            return Table(db_table=db_table)

        return Table(db_table=db_table)
    except Exception as e:
        raise DHError(e, "read_csv failed") from e
