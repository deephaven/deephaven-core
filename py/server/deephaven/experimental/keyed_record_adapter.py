#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import typing
from collections.abc import Iterable

import jpy
import numpy

from deephaven import dtypes
from deephaven.dtypes import DType
from deephaven.table import Table

K = typing.TypeVar("K")
T = typing.TypeVar("T")

j_object = jpy.get_type("java.lang.Object")
j_pykeyedrecordadapter = jpy.get_type("io.deephaven.dataadapter.PyKeyedRecordAdapter")


class KeyedRecordAdapter(typing.Generic[K, T]):
    j_object_type = j_pykeyedrecordadapter

    record_adapter_type: type
    j_kra: jpy.JType
    table: Table

    is_single_key_col: bool
    key_dtypes: typing.Union[DType, list[DType]]

    @property
    def j_object(self):
        return self.j_kra

    def __init__(
        self,
        table: Table,
        record_adapter_function: typing.Callable[..., T],
        key_cols: typing.Union[str, list[str]],
        data_cols: typing.Union[str, list[str]],
    ):
        self.table = table
        self.record_adapter_function = record_adapter_function

        if type(key_cols) is str:
            key_cols = [key_cols]

        if type(data_cols) is str:
            data_cols = [data_cols]

        self.j_kra = self.j_object_type(table.j_table, key_cols, data_cols)
        self.is_single_key_col = self.j_kra.isSingleKeyCol()

        # Build a list of the DTypes corresponding to the key columns
        if self.is_single_key_col:
            self.key_dtypes = self.table.definition[key_cols[0]].data_type
        else:
            self.key_dtypes = [
                self.table.definition[col_name].data_type for col_name in key_cols
            ]

    def get_records(
        self, data_keys: typing.Union[typing.Sequence[K], numpy.ndarray]
    ) -> typing.Mapping[K, typing.Union[None, T]]:
        """

        :param data_keys:
        :return: A dictionary of keys to results
        """

        unmapped_requested_data_keys = set(data_keys)

        if self.is_single_key_col:
            key_dtype = typing.cast(DType, self.key_dtypes)
            data_keys_data = typing.cast(
                typing.Sequence[T],
                self._convert_keys_seq_for_java(data_keys, key_dtype),
            )

            # Convert to a Java array whose type corresponds to the key column's type in the source table
            data_keys_java_arg = dtypes.array(key_dtype, data_keys_data)
        else:
            # Key should be a list of lists (i.e. each data_key is a list of key components)
            # Convert each data_key list to a Java array, then wrap each of those arrays into
            # another array, and pass that as the arg.
            key_dtypes = typing.cast(list[DType], self.key_dtypes)
            data_keys_java_arg = jpy.array("java.lang.Object", len(data_keys))
            for ii, composite_key in enumerate(data_keys):
                composite_key_array = jpy.array("java.lang.Object", len(composite_key))
                # Cast each key component to the appropriate type, then wrap them in an array.
                for jj, key_cmpnt in enumerate(composite_key):
                    # Convert the key components from whatever Python type they happen to be to the appropriate
                    # Java type (from key_dtypes)
                    composite_key_array[jj] = jpy.convert(
                        key_cmpnt, key_dtypes[jj].j_type
                    )

                data_keys_java_arg[ii] = composite_key_array

        results = self.j_kra.getRecordsForPython(data_keys_java_arg)

        # Now that we have retrieved a consistent snapshot, create records for each row.

        ## This is a TLongIntMap:
        row_key_to_data_key_positional_index: jpy.JType = (
            results.rowKeyToDataKeyPositionalIndex
        )

        ## This is an int[]:
        record_data_row_keys: typing.Sequence[int] = results.recordDataRowKeys

        ## This is an Object[] (where each element is an array, of either primitives or references):
        record_data_arrs: typing.Sequence[jpy.JType] = results.recordDataArrs

        ## This is the dict of results we return:
        result_records_dict: dict[K, typing.Union[None, T]] = dict()

        # Index of current row (i.e. index into arrays of data retrieved from each DB column)
        row_idx = 0
        for row_key in record_data_row_keys:
            # Find the data key corresponding to the DB row key from
            # which this record was retrieved. Map the data key to the record.
            data_key_idx: int = row_key_to_data_key_positional_index.get(row_key)
            data_key_for_row = data_keys[data_key_idx]

            # print(f'Building record for row_idx {row_idx}, row_key {row_key}, data_key {data_key_idx}/{data_key_for_row}')

            row_data = list()
            if self.is_single_key_col:
                row_data.append(data_key_for_row)
            else:
                row_data.extend(typing.cast(Iterable, data_key_for_row))

            unmapped_requested_data_keys.remove(data_key_for_row)

            for col_arr in record_data_arrs:
                # print(f'Adding data: {col_arr[row_idx]}')
                row_data.append(col_arr[row_idx])

            print(f"row_data for row {row_idx} is: {row_data}")
            record = self.record_adapter_function(*row_data)

            # Add the record to the results dictionary
            if self.is_single_key_col:
                result_records_dict[data_key_for_row] = record
            else:
                data_key_for_row_tuple = typing.cast(
                    K, tuple(typing.cast(Iterable, data_key_for_row))
                )
                result_records_dict[data_key_for_row_tuple] = record

            row_idx += 1

        # Add mappings to 'None' for data keys that were not found:
        for k in unmapped_requested_data_keys:
            result_records_dict[k] = None

        return result_records_dict

    def _convert_keys_seq_for_java(
        self,
        data_keys: typing.Union[typing.Sequence[K], numpy.ndarray],
        key_dtype: DType,
    ):
        if type(data_keys) is numpy.ndarray:
            # Some types (e.g. bool/DateTime) have special handling for numpy:
            return dtypes.array(key_dtype, data_keys.tolist())
        else:
            return data_keys


def make_record_adapter_with_constructor(
    table: Table,
    record_adapter_type: typing.Type[T],
    key_cols: typing.Union[str, list[str]],
    data_cols: typing.Union[str, list[str]],
) -> KeyedRecordAdapter[K, T]:
    def constructor_adapter_function(*row_data):
        # Create the record from the data key and row data
        # The constructor MUST take the keys + the data as arguments, in order.
        record = record_adapter_type.__new__(record_adapter_type)
        record_adapter_type.__init__(record, *row_data)
        return record

    return KeyedRecordAdapter(table, constructor_adapter_function, key_cols, data_cols)


def make_record_adapter(
    table: Table,
    record_adapter_function: typing.Callable[..., T],
    key_cols: typing.Union[str, list[str]],
    data_cols: typing.Union[str, list[str]],
) -> KeyedRecordAdapter[K, T]:
    return KeyedRecordAdapter(table, record_adapter_function, key_cols, data_cols)
