#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""A helper that sets the Barrage encodings a table's columns get, for tests of encoded Arrow columns."""

from collections.abc import Sequence

import jpy

from deephaven.table import Table

_JBarrageUtil = jpy.get_type("io.deephaven.extensions.barrage.util.BarrageUtil")
_JSchema = jpy.get_type("org.apache.arrow.vector.types.pojo.Schema")
_JField = jpy.get_type("org.apache.arrow.vector.types.pojo.Field")
_JFieldType = jpy.get_type("org.apache.arrow.vector.types.pojo.FieldType")
_JDictionaryEncoding = jpy.get_type(
    "org.apache.arrow.vector.types.pojo.DictionaryEncoding"
)
_JArrowTypeInt = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Int")
_JRunEndEncoded = jpy.get_type(
    "org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded"
)
_JArrayList = jpy.get_type("java.util.ArrayList")
_JCollections = jpy.get_type("java.util.Collections")
_JMap = jpy.get_type("java.util.Map")


def with_barrage_encodings(
    table: Table,
    dict_cols: Sequence[str] = (),
    ree_cols: Sequence[str] = (),
    ree_dict_cols: Sequence[str] = (),
) -> Table:
    """Returns the table with a Barrage schema attribute that dictionary encodes dict_cols, run-end encodes ree_cols,
    and run-end encodes ree_dict_cols over dictionary encoded values."""
    # makeSchema, unlike schemaFromTable, never infers encodings, so the encodings named here are the only ones
    j_natural = _JBarrageUtil.makeSchema(
        _JBarrageUtil.DEFAULT_SNAPSHOT_OPTIONS,
        table.j_table.getDefinition(),
        _JMap.of(),
        table.j_table.isFlat(),
    )
    j_natural_fields = j_natural.getFields()
    j_fields = _JArrayList()
    next_dict_id = 0
    for i in range(j_natural_fields.size()):
        j_field = jpy.cast(j_natural_fields.get(i), _JField)
        name = j_field.getName()
        j_encoding = None
        if name in dict_cols or name in ree_dict_cols:
            j_encoding = _JDictionaryEncoding(
                next_dict_id, False, _JArrowTypeInt(32, True)
            )
            next_dict_id += 1
        j_values_type = _JFieldType(
            j_field.isNullable(), j_field.getType(), j_encoding, j_field.getMetadata()
        )
        if name in ree_cols or name in ree_dict_cols:
            j_children = _JArrayList()
            j_children.add(
                _JField(
                    "run_ends",
                    _JFieldType(False, _JArrowTypeInt(32, True), None),
                    _JCollections.emptyList(),
                )
            )
            j_children.add(_JField("values", j_values_type, j_field.getChildren()))
            j_field = _JField(
                name,
                _JFieldType(
                    j_field.isNullable(),
                    _JRunEndEncoded.INSTANCE,
                    None,
                    j_field.getMetadata(),
                ),
                j_children,
            )
        else:
            j_field = _JField(name, j_values_type, j_field.getChildren())
        j_fields.add(j_field)
    j_schema = _JSchema(j_fields, j_natural.getCustomMetadata())
    return table.with_attributes({"BarrageSchema": j_schema})
