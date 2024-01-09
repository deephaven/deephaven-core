import typing

import jpy
import numpy

from deephaven import dtypes
from deephaven.dtypes import DType
from deephaven.table import Table

K = typing.TypeVar('K')
T = typing.TypeVar('T')

j_object = jpy.get_type('java.lang.Object')
j_pykeyedrecordadapter = jpy.get_type('io.deephaven.dataadapter.PyKeyedRecordAdapter')


class KeyedRecordAdapter(typing.Generic[K, T]):
    j_object_type = j_pykeyedrecordadapter

    record_adapter_type: type
    j_kra: jpy.JType
    table: Table

    is_single_key_col: bool
    key_dtypes: typing.Union[DType, typing.List[DType]]

    @property
    def j_object(self):
        return self.j_kra

    def __init__(self,
                 table: Table,
                 record_adapter_function: typing.Callable[..., T],
                 key_cols: [str, typing.List[str]],
                 data_cols: [str, typing.List[str]]
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
            self.key_dtypes = self.table.columns_dict()[key_cols[0]].data_type
        else:
            self.key_dtypes = [self.table.columns_dict()[col_name].data_type for col_name in key_cols]

    def get_records(self, data_keys: [typing.List[K], numpy.ndarray]) -> [typing.Dict[K, T], typing.List[T]]:
        """

        :param data_keys:
        :return: A dictionary of keys to results if the keys are single values or a list of results
        (in the same order as the provided keys) if the keys are composite values.
        """

        if self.is_single_key_col:
            key_dtype: DType = self.key_dtypes
            data_keys_data = self._convert_keys_seq_for_java(data_keys, key_dtype)

            # Convert to a Java array whose type corresponds to the key column's type in the source table
            data_keys_java_arg = dtypes.array(key_dtype, data_keys_data)
        else:
            # Key should be a list of lists (i.e. each data_key is a list of key components)
            # Convert each data_key list to a Java array, then wrap each of those arrays into
            # another array, and pass that as the arg.
            # TODO: Primitive elements of a composite_data_key need to be converted to the correct Java type corresponding to the column.
            data_keys_java_arg = jpy.array('java.lang.Object', len(data_keys))
            for ii, composite_key in enumerate(data_keys):
                composite_key_array = jpy.array('java.lang.Object', len(composite_key))
                # Cast each key component to the appropriate type, then wrap them in an array.
                for jj, key_cmpnt in enumerate(composite_key):
                    # TODO: cannot just cast these; need to really *convert* them
                    composite_key_array[jj] = jpy.cast(key_cmpnt, self.key_dtypes[jj].j_type)

                data_keys_java_arg[ii] = composite_key_array

        results = self.j_kra.getRecordsForPython(data_keys_java_arg)

        # Now that we have retrieved a consistent snapshot, create records for each row.

        ## This is a TLongIntMap:
        row_key_to_data_key_positional_index: jpy.JType = results.rowKeyToDataKeyPositionalIndex

        ## This is an int[]:
        record_data_row_keys: typing.Sequence[int] = results.recordDataRowKeys

        ## This is an Object[] (where each element is an array, of either primitives or references):
        record_data_arrs: typing.Sequence[jpy.JType] = results.recordDataArrs

        if self.is_single_key_col:
            result_records_dict: typing.Dict[K, T] = dict()
        else:
            result_records_dict: typing.Dict[int, T] = dict()

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
                row_data.extend(data_key_for_row)

            for col_arr in record_data_arrs:
                # print(f'Adding data: {col_arr[row_idx]}')
                row_data.append(col_arr[row_idx])

            print(f'row_data for row {row_idx} is: {row_data}')
            record = self.record_adapter_function(*row_data)

            if self.is_single_key_col:
                # Add the record to the results dictionary
                result_records_dict[data_key_for_row] = record
            else:
                result_records_dict[data_key_idx] = record

            row_idx += 1

        if self.is_single_key_col:
            return result_records_dict
        else:
            # Build a list with the results at the same indices as the input data keys
            # (or None if no result was found for the requested key).
            # This is because Python does not allow lists to be used as dictionary keys.
            results_list: typing.List[T] = list()
            for idx in range(0, len(data_keys)):
                if idx in result_records_dict:
                    results_list.append(result_records_dict[idx])
                else:
                    results_list.append(None)

            return results_list

    def _convert_keys_seq_for_java(self, data_keys: typing.Sequence, key_dtype: DType) -> typing.Sequence:
        if type(data_keys) is numpy.ndarray:
            # Some types (e.g. bool/DateTime) have special handling for numpy:
            return dtypes.array(key_dtype, data_keys)
        else:
            return data_keys


def make_record_adapter_with_constructor(table: Table,
                                         record_adapter_type: typing.Type[T],
                                         key_cols: [str, typing.List[str]],
                                         data_cols: [str, typing.List[str]]) -> KeyedRecordAdapter[K, T]:
    def constructor_adapter_function(*row_data):
        # Create the record from the data key and row data
        # The constructor MUST take the keys + the data as arguments, in order.
        record = record_adapter_type.__new__(record_adapter_type)
        record_adapter_type.__init__(record, *row_data)
        return record

    return KeyedRecordAdapter(
        table,
        constructor_adapter_function,
        key_cols,
        data_cols
    )


def make_record_adapter(table: Table,
                        record_adapter_function: typing.Callable[..., T],
                        key_cols: [str, typing.List[str]],
                        data_cols: [str, typing.List[str]]) -> KeyedRecordAdapter[K, T]:
    return KeyedRecordAdapter(
        table,
        record_adapter_function,
        key_cols,
        data_cols
    )
