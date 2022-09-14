import typing

import jpy

from deephaven.table import Table

K = typing.TypeVar('K')
T = typing.TypeVar('T')

j_object = jpy.get_type('java.lang.Object')


class KeyedRecordAdapter(typing.Generic[K, T]):
    j_object_type = jpy.get_type('io.deephaven.queryutil.dataadapter.PyKeyedRecordAdapter')

    record_adapter_type: type
    j_kra: jpy.JType
    table: Table

    is_single_key_col: bool

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

    def get_records(self, data_keys: typing.List[K]) -> [typing.Dict[K, T], typing.List[T]]:
        """

        :param data_keys:
        :return: A dictionary of keys to results if the keys are single values or a list of results
        (in the same order as the provided keys) if the keys are composite values.
        """

        # TODO: Need to handle primitives better, e.g. convert python int list to java int[]/long[] as appropriate.

        if self.is_single_key_col:
            # TODO: convert these to the appropriate Java type for the key column. Ultimately must be boxed.
            data_keys_arg = data_keys
        else:
            # Key should be a list of lists (i.e. each data_key is a list of key components)
            # Convert each data_key list to a Java array, then wrap each of those arrays into
            # another array, and pass that as the arg.
            # TODO: Primitive elements of a composite_data_key need to be converted to the correct Java type corresponding to the column.
            data_keys_arg = list(
                map(lambda composite_data_key: jpy.array('java.lang.Object', composite_data_key), data_keys))

            data_keys_arg = jpy.array(j_object, data_keys_arg)

        results = self.j_kra.getRecordsForPython(data_keys_arg)

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
