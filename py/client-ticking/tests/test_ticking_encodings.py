#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import queue
import unittest
from typing import Any, Dict, List

import pydeephaven as dh

_NUM_ROWS = 100

# Makes plain_table, and encoded_table: the same data, with a Barrage schema attribute that asks for dictionary encoded
# columns (strings, and ints with nulls), run-end encoded columns (ints, and doubles and timestamps with runs of nulls),
# a run-end encoded column over dictionary encoded values, and a list column whose elements are dictionary encoded.
_TABLES_SCRIPT = f"""
import jpy
from deephaven import empty_table

_JBarrageUtil = jpy.get_type("io.deephaven.extensions.barrage.util.BarrageUtil")
_JSchema = jpy.get_type("org.apache.arrow.vector.types.pojo.Schema")
_JField = jpy.get_type("org.apache.arrow.vector.types.pojo.Field")
_JFieldType = jpy.get_type("org.apache.arrow.vector.types.pojo.FieldType")
_JDictionaryEncoding = jpy.get_type("org.apache.arrow.vector.types.pojo.DictionaryEncoding")
_JArrowTypeInt = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Int")
_JRunEndEncoded = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded")
_JArrayList = jpy.get_type("java.util.ArrayList")
_JCollections = jpy.get_type("java.util.Collections")

def _dictionary_encoded(j_field, dict_id):
    j_type = _JFieldType(j_field.isNullable(), j_field.getType(),
                         _JDictionaryEncoding(dict_id, False, _JArrowTypeInt(32, True)), j_field.getMetadata())
    return _JField(j_field.getName(), j_type, j_field.getChildren())

def _run_end_encoded(j_field, j_values):
    j_children = _JArrayList()
    j_children.add(_JField("run_ends", _JFieldType(False, _JArrowTypeInt(32, True), None), _JCollections.emptyList()))
    j_children.add(_JField("values", j_values.getFieldType(), j_values.getChildren()))
    j_type = _JFieldType(j_field.isNullable(), _JRunEndEncoded.INSTANCE, None, j_field.getMetadata())
    return _JField(j_field.getName(), j_type, j_children)

def _with_barrage_encodings(table):
    j_natural = _JBarrageUtil.schemaFromTable(table.j_table)
    j_natural_fields = j_natural.getFields()
    j_fields = _JArrayList()
    for i in range(j_natural_fields.size()):
        j_field = jpy.cast(j_natural_fields.get(i), _JField)
        name = j_field.getName()
        if name == "Sym":
            j_field = _dictionary_encoded(j_field, 0)
        elif name == "Block":
            j_field = _run_end_encoded(j_field, j_field)
        elif name == "Both":
            j_field = _run_end_encoded(j_field, _dictionary_encoded(j_field, 1))
        elif name == "Tags":
            j_children = _JArrayList()
            j_children.add(_dictionary_encoded(jpy.cast(j_field.getChildren().get(0), _JField), 2))
            j_field = _JField(name, j_field.getFieldType(), j_children)
        elif name == "Dbl" or name == "Ts":
            j_field = _run_end_encoded(j_field, j_field)
        elif name == "Num":
            j_field = _dictionary_encoded(j_field, 3)
        j_fields.add(j_field)
    return table.with_attributes({{"BarrageSchema": _JSchema(j_fields, j_natural.getCustomMetadata())}})

plain_table = empty_table({_NUM_ROWS}).update([
    "Sym = ii % 3 == 0 ? `AAPL` : `MSFT`",
    "Block = (int) (ii / 10)",
    "Both = ((int) (ii / 10)) % 2 == 0 ? `X` : null",
    "Tags = new String[] {{ Sym, ii % 2 == 0 ? `even` : `odd` }}",
    "Dbl = ((int) (ii / 10)) % 2 == 0 ? NULL_DOUBLE : 1.5",
    "Num = ii % 3 == 0 ? NULL_INT : (int) (ii % 2)",
    "Ts = ((int) (ii / 10)) % 2 == 0 ? null : '2024-01-01T00:00:00Z'",
])
encoded_table = _with_barrage_encodings(plain_table)
"""

_COLUMNS = ["Sym", "Block", "Both", "Tags", "Dbl", "Num", "Ts"]


class TickingEncodingsTestCase(unittest.TestCase):
    def test_subscribe_to_barrage_encoded_columns(self):
        session = dh.Session()
        try:
            session.run_script(_TABLES_SCRIPT)
            plain = self._subscribe_and_collect(session.open_table("plain_table"))
            encoded = self._subscribe_and_collect(session.open_table("encoded_table"))
        finally:
            session.close()
        for name in _COLUMNS:
            self.assertEqual(len(plain[name]), _NUM_ROWS, name)
            self.assertEqual(plain[name], encoded[name], name)

    def _subscribe_and_collect(self, table) -> Dict[str, List[Any]]:
        """Subscribes to table and returns the values of its added rows, column by column, once all have arrived."""
        values: Dict[str, List[Any]] = {name: [] for name in _COLUMNS}
        done: queue.Queue = queue.Queue()

        def on_update(update):
            added = update.added()
            for name in _COLUMNS:
                if name in added:
                    values[name].extend(added[name].to_pylist())
            if len(values[_COLUMNS[0]]) >= _NUM_ROWS:
                done.put(None)

        def on_error(error: Exception):
            done.put(error)

        listener_handle = dh.listen(table, on_update, on_error)
        listener_handle.start()
        try:
            result = done.get(block=True, timeout=30)
        except queue.Empty:
            self.fail("Timed out waiting for the subscription's rows")
        finally:
            listener_handle.stop()
        if isinstance(result, Exception):
            raise result
        return values


if __name__ == "__main__":
    unittest.main()
