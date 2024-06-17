#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""The deephaven JSON module presents a declarative and composable configuration layer for describing the structure of a
JSON (https://www.json.org) value. Most commonly, this will be used to model the structure for a JSON object. For
example, the JSON object

.. code-block:: json
    { "name": "Foo", "age": 42, "location": { "lat": 45.018269, "lon": -93.473892 } }

can be modelled with the dictionary

.. code-block:: python
    { "name": str, "age": int, "location": { "lat": float, "lon": float } }

Notice that this allows for the nested modelling of JSON values. Other common constructions involve the modelling of
JSON arrays. For example, a variable-length JSON array where the elements are the same type

.. code-block:: json
    [42, 31, ..., 12345]

can be modelled with a single-element list containing the element type

.. code-block:: python
    [ int ]

If the JSON array is a fixed size and each elements' type is known, for example

.. code-block:: json
    ["Foo", 42, [45.018269, -93.473892]]

can be modelled with a tuple containing each type

.. code-block:: python
    (str, int, (float, float))

Notice again that this allows for the nested modelling of JSON values. Of course, these constructions can be all be used
together. For example, the JSON object

.. code-block:: json
    {
      "name": "Foo",
      "locations": [
        [45.018269, -93.473892],
        ...,
        [40.730610, -73.935242]
      ]
    }

can be modelled as

.. code-block:: python
    {"name": str, "locations": [(float, float)]}

See the methods in this module more more details on modelling JSON values.
"""

import jpy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Union, Tuple, Optional, Literal, Any

from deephaven import dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.time import to_j_instant
from deephaven._jpy import strict_cast


__all__ = [
    "string_val",
    "bool_val",
    "char_val",
    "byte_val",
    "short_val",
    "int_val",
    "long_val",
    "float_val",
    "double_val",
    "instant_val",
    "big_integer_val",
    "big_decimal_val",
    "array_val",
    "object_val",
    "typed_object_val",
    "object_entries_val",
    "tuple_val",
    "any_val",
    "skip_val",
    "json_val",
    "JsonValue",
    "JsonValueType",
    "RepeatedFieldBehavior",
    "ObjectField",
]

_JValue = jpy.get_type("io.deephaven.json.Value")
_JObjectValue = jpy.get_type("io.deephaven.json.ObjectValue")
_JTypedObjectValue = jpy.get_type("io.deephaven.json.TypedObjectValue")
_JArrayValue = jpy.get_type("io.deephaven.json.ArrayValue")
_JObjectEntriesValue = jpy.get_type("io.deephaven.json.ObjectEntriesValue")
_JTupleValue = jpy.get_type("io.deephaven.json.TupleValue")
_JObjectField = jpy.get_type("io.deephaven.json.ObjectField")
_JRepeatedFieldBehavior = jpy.get_type("io.deephaven.json.ObjectField$RepeatedBehavior")
_JJsonValueTypes = jpy.get_type("io.deephaven.json.JsonValueTypes")
_JBoolValue = jpy.get_type("io.deephaven.json.BoolValue")
_JCharValue = jpy.get_type("io.deephaven.json.CharValue")
_JByteValue = jpy.get_type("io.deephaven.json.ByteValue")
_JShortValue = jpy.get_type("io.deephaven.json.ShortValue")
_JIntValue = jpy.get_type("io.deephaven.json.IntValue")
_JLongValue = jpy.get_type("io.deephaven.json.LongValue")
_JFloatValue = jpy.get_type("io.deephaven.json.FloatValue")
_JDoubleValue = jpy.get_type("io.deephaven.json.DoubleValue")
_JStringValue = jpy.get_type("io.deephaven.json.StringValue")
_JSkipValue = jpy.get_type("io.deephaven.json.SkipValue")
_JInstantValue = jpy.get_type("io.deephaven.json.InstantValue")
_JInstantNumberValue = jpy.get_type("io.deephaven.json.InstantNumberValue")
_JInstantNumberValueFormat = jpy.get_type("io.deephaven.json.InstantNumberValue$Format")
_JBigIntegerValue = jpy.get_type("io.deephaven.json.BigIntegerValue")
_JBigDecimalValue = jpy.get_type("io.deephaven.json.BigDecimalValue")
_JAnyValue = jpy.get_type("io.deephaven.json.AnyValue")


_VALUE_STRING = _JJsonValueTypes.STRING
_VALUE_NULL = _JJsonValueTypes.NULL
_VALUE_INT = _JJsonValueTypes.INT
_VALUE_DECIMAL = _JJsonValueTypes.DECIMAL
_VALUE_BOOL = _JJsonValueTypes.BOOL
_VALUE_OBJECT = _JJsonValueTypes.OBJECT
_VALUE_ARRAY = _JJsonValueTypes.ARRAY

_EPOCH_SECONDS = _JInstantNumberValueFormat.EPOCH_SECONDS
_EPOCH_MILLIS = _JInstantNumberValueFormat.EPOCH_MILLIS
_EPOCH_MICROS = _JInstantNumberValueFormat.EPOCH_MICROS
_EPOCH_NANOS = _JInstantNumberValueFormat.EPOCH_NANOS


class JsonValue(JObjectWrapper):
    """The JSON Value type."""

    j_object_type = _JValue

    def __init__(self, j_value: jpy.JType):
        self.j_value = j_value

    @property
    def j_object(self) -> jpy.JType:
        return self.j_value


JsonValueType = Union[
    JsonValue,
    dtypes.DType,
    type,
    Dict[str, Union["JsonValueType", "ObjectField"]],
    List["JsonValueType"],
    Tuple["JsonValueType", ...],
]
"""The JSON value alias"""


def json_val(json_value_type: JsonValueType) -> JsonValue:
    """Creates a JsonValue from a JsonValueType.

     - JsonValue is returned unchanged
     - bool returns bool_val()
     - int returns long_val()
     - float returns double_val()
     - str returns string_val()
     - datetime.datetime returns instant_val()
     - object returns any_val()
     - Dictionaries returns object_val(json_value_type)
     - Lists of length 1 returns array_val(json_value_type[0]) (Lists of other sizes are not supported)
     - Tuples returns tuple_val(json_value_type)

    Args:
        json_value_type (JsonValueType): the JSON value type

    Returns:
        the JSON value
    """
    if isinstance(json_value_type, JsonValue):
        return json_value_type
    if isinstance(json_value_type, dtypes.DType):
        return _dtype_dict[json_value_type]
    if isinstance(json_value_type, type):
        return _type_dict[json_value_type]
    if isinstance(json_value_type, Dict):
        return object_val(json_value_type)
    if isinstance(json_value_type, List):
        if len(json_value_type) is not 1:
            raise TypeError("Expected List as json type to have exactly one element")
        return array_val(json_value_type[0])
    if isinstance(json_value_type, Tuple):
        return tuple_val(json_value_type)
    raise TypeError(f"Unsupported JSON value type {type(json_value_type)}")


class RepeatedFieldBehavior(Enum):
    """
    The behavior to use when a repeated field is encountered in a JSON object. For example,
    .. code-block:: json
        {
          "foo": 42,
          "foo": 43
        }
    """

    USE_FIRST = _JRepeatedFieldBehavior.USE_FIRST
    """Use the first field"""

    ERROR = _JRepeatedFieldBehavior.ERROR
    """Raise an error"""


@dataclass
class ObjectField:
    """The object field options.

    In contexts where the user needs to create an object field value and isn't changing any default values, the user can
    simplify by just using the JsonValueType. For example,

    .. code-block:: python
        {
            "name": ObjectField(str),
            "age": ObjectField(int),
        }

    could be simplified to

    .. code-block:: python
        {
            "name": str,
            "age": int,
        }
    """

    value_type: JsonValueType
    """The json value type"""
    aliases: Union[str, List[str]] = field(default_factory=list)
    """The field name aliases. By default, is an empty list."""
    repeated_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.ERROR
    """The repeated field behavior. By default, is RepeatedFieldBehavior.ERROR."""
    case_sensitive: bool = True
    """If the field name and aliases should be compared using case-sensitive equality. By default, is True."""

    def _j_field_options(self, name: str) -> jpy.JType:
        builder = (
            _JObjectField.builder()
            .name(name)
            .options(json_val(self.value_type).j_value)
            .repeatedBehavior(self.repeated_behavior.value)
            .caseSensitive(self.case_sensitive)
        )
        if self.aliases:
            builder.addAliases(
                [self.aliases] if isinstance(self.aliases, str) else self.aliases
            )
        return builder.build()


def _build(
    builder,
    allow_missing: bool,
    allow_null: bool,
    allow_int: bool = False,
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_bool: bool = False,
    allow_object: bool = False,
    allow_array: bool = False,
):
    builder.allowMissing(allow_missing)
    builder.allowedTypes(
        ([_VALUE_STRING] if allow_string else [])
        + ([_VALUE_NULL] if allow_null else [])
        + ([_VALUE_INT] if allow_int else [])
        + ([_VALUE_DECIMAL] if allow_decimal else [])
        + ([_VALUE_BOOL] if allow_bool else [])
        + ([_VALUE_OBJECT] if allow_object else [])
        + ([_VALUE_ARRAY] if allow_array else [])
    )


def object_val(
    fields: Dict[str, Union[JsonValueType, ObjectField]],
    allow_unknown_fields: bool = True,
    allow_missing: bool = True,
    allow_null: bool = True,
    repeated_field_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.ERROR,
    case_sensitive: bool = True,
) -> JsonValue:
    """Creates an object value. For example, the JSON object

    .. code-block:: json
        { "name": "foo", "age": 42 }

    might be modelled as the object type

    .. code-block:: python
        object_val({ "name": str, "age": int })

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using a Dict[str, Union[JsonValueType, ObjectField]]. For example,

    .. code-block:: python
        some_method(object_val({ "name": str, "age": int }))

    could be simplified to

    .. code-block:: python
        some_method({ "name": str, "age": int })

    Args:
        fields (Dict[str, Union[JsonValueType, ObjectField]]): the fields
        allow_unknown_fields (bool): if unknown fields are allow, by default is True
        allow_missing (bool): if the object is allowed to be missing, by default is True
        allow_null (bool): if the object is allowed to be a JSON null type, by default is True
        repeated_field_behavior (RepeatedFieldBehavior): the default repeated field behavior, only used for fields that
            are specified using JsonValueType, by default is RepeatedFieldBehavior.ERROR
        case_sensitive (bool): if the field name and aliases should be compared using case-sensitive equality, only used
            for fields that are specified using JsonValueType, by default is True

    Returns:
        the object value
    """
    builder = _JObjectValue.builder()
    _build(builder, allow_missing, allow_null, allow_object=True)
    builder.allowUnknownFields(allow_unknown_fields)
    for field_name, field_opts in fields.items():
        field_opts = (
            field_opts
            if isinstance(field_opts, ObjectField)
            else ObjectField(
                field_opts,
                repeated_behavior=repeated_field_behavior,
                case_sensitive=case_sensitive,
            )
        )
        # noinspection PyProtectedMember
        builder.addFields(field_opts._j_field_options(field_name))
    return JsonValue(builder.build())


def typed_object_val(
    type_field: str,
    shared_fields: Dict[str, Union[JsonValueType, ObjectField]],
    objects: Dict[str, JsonValueType],
    allow_unknown_types: bool = True,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[str] = None,
    on_null: Optional[str] = None,
) -> JsonValue:
    """Creates a type-discriminated object value. For example, the JSON objects

    .. code-block:: json
        { "type": "trade", "symbol": "FOO", "price": 70.03, "size": 42 }

    .. code-block:: json
        { "type": "quote", "symbol": "BAR", "bid": 10.01, "ask": 10.05 }

    might be modelled as a type-discriminated object with "type" as the type field, "symbol" as a shared field, with a
    "trade" object containing a "bid" and an "ask" field, and with a "quote" object containing a "price" and a "size"
    field:

    .. code-block:: python
        typed_object_val(
            "type",
            {"symbol": str},
            {
                "quote": {
                    "price": float,
                    "size": int
                },
                "trade": {
                    "bid": float,
                    "ask": float
                }
            }
        )

    Args:
        type_field (str): the type-discriminating field
        shared_fields (Dict[str, Union[JsonValueType, ObjectField]]): the shared fields
        objects (Dict[str, Union[JsonValueType, ObjectField]]): the individual objects, keyed by their
            type-discriminated value. The values must be object options.
        allow_unknown_types (bool): if unknown types are allow, by default is True
        allow_missing (bool): if the object is allowed to be missing, by default is True
        allow_null (bool): if the object is allowed to be a JSON null type, by default is True
        on_missing (Optional[str]): the type value to use when the JSON value is missing and allow_missing is True,
            default is None
        on_null (Optional[str]): the type value to use when the JSON value is null and allow_null is True, default is
            None

    Returns:
        the typed object value
    """
    builder = _JTypedObjectValue.builder()
    _build(builder, allow_missing, allow_null, allow_object=True)
    builder.typeFieldName(type_field)
    builder.allowUnknownTypes(allow_unknown_types)
    if on_missing:
        builder.onMissing(on_missing)
    if on_null:
        builder.onNull(on_null)
    for shared_field_name, shared_field_opts in shared_fields.items():
        shared_field_opts = (
            shared_field_opts
            if isinstance(shared_field_opts, ObjectField)
            else ObjectField(
                shared_field_opts,
                repeated_behavior=RepeatedFieldBehavior.ERROR,
                case_sensitive=True,
            )
        )
        # noinspection PyProtectedMember
        builder.addSharedFields(shared_field_opts._j_field_options(shared_field_name))
    for object_name, object_type in objects.items():
        builder.putObjects(
            object_name, strict_cast(json_val(object_type).j_value, _JObjectValue)
        )
    return JsonValue(builder.build())


def array_val(
    element: JsonValueType,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonValue:
    """Creates a "typed array", where all elements of the array have the same element type. For example, the JSON array

    .. code-block:: json
        [1, 42, 43, 13]

    might be modelled as an array of ints

    .. code-block:: python
        array_val(int)

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using a list with a single element type. For example,

    .. code-block:: python
        some_method(array_val(element))

    could be simplified to

    .. code-block:: python
        some_method([element])

    Args:
        element (JsonValueType): the array element type
        allow_missing (bool): if the array is allowed to be missing, by default is True
        allow_null (bool): if the array is allowed to be a JSON null type, by default is True

    Returns:
        the array value
    """
    builder = _JArrayValue.builder()
    builder.element(json_val(element).j_value)
    _build(builder, allow_missing, allow_null, allow_array=True)
    return JsonValue(builder.build())


def object_entries_val(
    value_type: JsonValueType,
    key_type: JsonValueType = str,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonValue:
    """Creates an object entries value. This is used in situations where the number of fields in an object is
    variable and all the values types are the same. For example, the JSON object

    .. code-block:: json
        {
            "foo": 1,
            "bar": 42,
            "baz": 3,
            ...
            "xyz": 100
        }

    might be modelled as the object kv type

    .. code-block:: python
        object_entries_val(int)

    Args:
        value_type (JsonValueType): the value type element, required
        key_type (JsonValueType): the key type element, by default is type str
        allow_missing (bool): if the object is allowed to be missing, by default is True
        allow_null (bool): if the object is allowed to be a JSON null type, by default is True

    Returns:
        the object entries value
    """
    builder = _JObjectEntriesValue.builder()
    builder.key(json_val(key_type).j_value)
    builder.value(json_val(value_type).j_value)
    _build(builder, allow_missing, allow_null, allow_object=True)
    return JsonValue(builder.build())


def tuple_val(
    values: Union[Tuple[JsonValueType, ...], Dict[str, JsonValueType]],
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonValue:
    """Creates a tuple value. For example, the JSON array

    .. code-block:: json
        ["foo", 42, 5.72]

    might be modelled as the tuple type

    .. code-block:: python
        tuple_val((str, int, float))

    To provide meaningful names, a dictionary can be used:

    .. code-block:: python
        tuple_val({"name": str, "age": int, "height": float})

    otherwise, default names based on the indexes of the values will be used.

    In contexts where the user needs to create a JsonValueType and isn't changing any default values nor is setting
    names, the user can simplify passing through a python tuple type. For example,

    .. code-block:: python
        some_method(tuple_val((tuple_type_1, tuple_type_2)))

    could be simplified to

    .. code-block:: python
        some_method((tuple_type_1, tuple_type_2))

    Args:
        values (Union[Tuple[JsonValueType, ...], Dict[str, JsonValueType]]): the tuple value types
        allow_missing (bool): if the array is allowed to be missing, by default is True
        allow_null (bool): if the array is allowed to be a JSON null type, by default is True
    Returns:
        the tuple value
    """
    if isinstance(values, Tuple):
        kvs = enumerate(values)
    elif isinstance(values, Dict):
        kvs = values.items()
    else:
        raise TypeError(f"Invalid tuple type: {type(values)}")
    builder = _JTupleValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_array=True,
    )
    for name, json_value_type in kvs:
        builder.putNamedValues(str(name), json_val(json_value_type).j_value)
    return JsonValue(builder.build())


def bool_val(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[bool] = None,
    on_null: Optional[bool] = None,
) -> JsonValue:
    """Creates a bool value. For example, the JSON boolean

    .. code-block:: json
        True

    might be modelled as the bool type

    .. code-block:: python
        bool_val()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in bool type. For example,

    .. code-block:: python
        some_method(bool_val())

    could be simplified to

    .. code-block:: python
        some_method(bool)

    Args:
        allow_string (bool): if the bool value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the bool value is allowed to be missing, default is True
        allow_null (bool): if the bool value is allowed to be a JSON null type, default is True
        on_missing (Optional[bool]): the value to use when the JSON value is missing and allow_missing is True, default is None
        on_null (Optional[bool]): the value to use when the JSON value is null and allow_null is True, default is None

    Returns:
        the bool value
    """
    builder = _JBoolValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_bool=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def char_val(
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[str] = None,
    on_null: Optional[str] = None,
) -> JsonValue:
    """Creates a char value. For example, the JSON string

    .. code-block:: json
        "F"

    might be modelled as the char type

    .. code-block:: python
        char_val()

    Args:
        allow_missing (bool): if the char value is allowed to be missing, default is True
        allow_null (bool): if the char value is allowed to be a JSON null type, default is True
        on_missing (Optional[str]): the value to use when the JSON value is missing and allow_missing is True, default is None. If specified, must be a single character.
        on_null (Optional[str]): the value to use when the JSON value is null and allow_null is True, default is None. If specified, must be a single character.

    Returns:
        the char value
    """
    builder = _JCharValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_string=True,
    )
    if on_null:
        builder.onNull(ord(on_null))
    if on_missing:
        builder.onMissing(ord(on_missing))
    return JsonValue(builder.build())


def byte_val(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonValue:
    """Creates a byte (signed 8-bit) value. For example, the JSON integer

    .. code-block:: json
        42

    might be modelled as the byte type

    .. code-block:: python
        byte_val()

    Args:
        allow_decimal (bool): if the byte value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the byte value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the byte value is allowed to be missing, default is True
        allow_null (bool): if the byte value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the byte value
    """
    builder = _JByteValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def short_val(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonValue:
    """Creates a short (signed 16-bit) value. For example, the JSON integer

    .. code-block:: json
        30000

    might be modelled as the short type

    .. code-block:: python
        short_val()

    Args:
        allow_decimal (bool): if the short value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the short value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the short value is allowed to be missing, default is True
        allow_null (bool): if the short value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the short value
    """
    builder = _JShortValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def int_val(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonValue:
    """Creates an int (signed 32-bit) value. For example, the JSON integer

    .. code-block:: json
        100000

    might be modelled as the int type

    .. code-block:: python
        int_val()

    Args:
        allow_decimal (bool): if the int value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the int value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the int value is allowed to be missing, default is True
        allow_null (bool): if the int value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the int value
    """
    builder = _JIntValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def long_val(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonValue:
    """Creates a long (signed 64-bit) value. For example, the JSON integer

    .. code-block:: json
        8000000000

    might be modelled as the long type

    .. code-block:: python
        long_val()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in long type. For example,

    .. code-block:: python
        some_method(long_val())

    could be simplified to

    .. code-block:: python
        some_method(int)

    Args:
        allow_decimal (bool): if the long value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the long value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the long value is allowed to be missing, default is True
        allow_null (bool): if the long value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the long value
    """
    builder = _JLongValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def float_val(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[float] = None,
    on_null: Optional[float] = None,
) -> JsonValue:
    """Creates a float (signed 32-bit) value. For example, the JSON decimal

    .. code-block:: json
        42.42

    might be modelled as the float type

    .. code-block:: python
        float_val()

    Args:
        allow_string (bool): if the float value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the float value is allowed to be missing, default is True
        allow_null (bool): if the float value is allowed to be a JSON null type, default is True
        on_missing (Optional[float]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[float]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the float value
    """
    builder = _JFloatValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def double_val(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[float] = None,
    on_null: Optional[float] = None,
) -> JsonValue:
    """Creates a double (signed 64-bit) value. For example, the JSON decimal

    .. code-block:: json
        42.42424242

    might be modelled as the double type

    .. code-block:: python
        double_val()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in float type. For example,

    .. code-block:: python
        some_method(double_val())

    could be simplified to

    .. code-block:: python
        some_method(float)

    Args:
        allow_string (bool): if the double value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the double value is allowed to be missing, default is True
        allow_null (bool): if the double value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the double value
    """
    builder = _JDoubleValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


def string_val(
    allow_int: bool = False,
    allow_decimal: bool = False,
    allow_bool: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[str] = None,
    on_null: Optional[str] = None,
) -> JsonValue:
    """Creates a String value. For example, the JSON string

    .. code-block:: json
        "Hello, world!"

    might be modelled as the string type

    .. code-block:: python
        string_val()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in str type. For example,

    .. code-block:: python
        some_method(string_val())

    could be simplified to

    .. code-block:: python
        some_method(str)

    Args:
        allow_int (bool): if the string value is allowed to be a JSON integer type, default is False
        allow_decimal (bool): if the string value is allowed to be a JSON decimal type, default is False
        allow_bool (bool): if the string value is allowed to be a JSON boolean type, default is False
        allow_missing (bool): if the double value is allowed to be missing, default is True
        allow_null (bool): if the double value is allowed to be a JSON null type, default is True
        on_missing (Optional[str]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[str]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the String value
    """
    builder = _JStringValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_string=True,
        allow_int=allow_int,
        allow_decimal=allow_decimal,
        allow_bool=allow_bool,
    )
    if on_null:
        builder.onNull(on_null)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonValue(builder.build())


# TODO(deephaven-core#5269): Create deephaven.time time-type aliases
def instant_val(
    allow_missing: bool = True,
    allow_null: bool = True,
    number_format: Literal[None, "s", "ms", "us", "ns"] = None,
    allow_decimal: bool = False,
    on_missing: Optional[Any] = None,
    on_null: Optional[Any] = None,
) -> JsonValue:
    """Creates an Instant value. For example, the JSON string

    .. code-block:: json
        "2009-02-13T23:31:30.123456789Z"

    might be modelled as the Instant type

    .. code-block:: python
        instant_val()

    In another example, the JSON decimal

    .. code-block:: json
        1234567890.123456789

    might be modelled as the Instant type

    .. code-block:: python
        instant_val(number_format="s", allow_decimal=True)

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python datetime.datetime type. For example,

    .. code-block:: python
        some_method(instant_val())

    could be simplified to

    .. code-block:: python
        some_method(datetime.datetime)

    Args:
        allow_missing (bool): if the Instant value is allowed to be missing, default is True
        allow_null (bool): if the Instant value is allowed to be a JSON null type, default is True
        number_format (Literal[None, "s", "ms", "us", "ns"]): when set, signifies that a JSON numeric type is expected.
            "s" is for seconds, "ms" is for milliseconds, "us" is for microseconds, and "ns" is for nanoseconds since
            the epoch. When not set, a JSON string in the ISO-8601 format is expected.
        allow_decimal (bool): if the Instant value is allowed to be a JSON decimal type, default is False. Only valid
            when number_format is specified.
        on_missing (Optional[Any]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[Any]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the Instant value
    """
    if number_format:
        builder = _JInstantNumberValue.builder()
        if on_missing:
            builder.onMissing(to_j_instant(on_missing))
        if on_null:
            builder.onNull(to_j_instant(on_null))
        _build(
            builder,
            allow_missing,
            allow_null,
            allow_int=True,
            allow_decimal=allow_decimal,
        )
        if number_format == "s":
            builder.format(_EPOCH_SECONDS)
        elif number_format == "ms":
            builder.format(_EPOCH_MILLIS)
        elif number_format == "us":
            builder.format(_EPOCH_MICROS)
        elif number_format == "ns":
            builder.format(_EPOCH_NANOS)
        else:
            raise TypeError(f"Invalid number format: {number_format}")
        return JsonValue(builder.build())
    else:
        if allow_decimal:
            raise TypeError("allow_decimal is only valid when using number_format")
        builder = _JInstantValue.builder()
        if on_missing:
            builder.onMissing(to_j_instant(on_missing))
        if on_null:
            builder.onNull(to_j_instant(on_null))
        _build(
            builder,
            allow_missing,
            allow_null,
            allow_string=True,
        )
        return JsonValue(builder.build())


def big_integer_val(
    allow_string: bool = False,
    allow_decimal: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[Union[int, str]] = None,
    on_null: Optional[Union[int, str]] = None,
) -> JsonValue:
    """Creates a BigInteger value. For example, the JSON integer

    .. code-block:: json
        123456789012345678901

    might be modelled as the BigInteger type

    .. code-block:: python
        big_integer_val()

    Args:
        allow_string (bool): if the BigInteger value is allowed to be a JSON string type, default is False.
        allow_decimal (bool): if the BigInteger value is allowed to be a JSON decimal type, default is False.
        allow_missing (bool): if the BigInteger value is allowed to be missing, default is True
        allow_null (bool): if the BigInteger value is allowed to be a JSON null type, default is True
        on_missing (Optional[Union[int, str]]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[Union[int, str]]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the BigInteger value
    """
    builder = _JBigIntegerValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_missing:
        builder.onMissing(dtypes.BigInteger(str(on_missing)))
    if on_null:
        builder.onNull(dtypes.BigInteger(str(on_null)))
    return JsonValue(builder.build())


def big_decimal_val(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[Union[float, str]] = None,
    on_null: Optional[Union[float, str]] = None,
) -> JsonValue:
    """Creates a BigDecimal value. For example, the JSON decimal

    .. code-block:: json
        123456789012345678901.42

    might be modelled as the BigDecimal type

    .. code-block:: python
        big_decimal_val()

    Args:
        allow_string (bool): if the BigDecimal value is allowed to be a JSON string type, default is False.
        allow_missing (bool): if the BigDecimal value is allowed to be missing, default is True
        allow_null (bool): if the BigDecimal value is allowed to be a JSON null type, default is True
        on_missing (Optional[Union[float, str]]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[Union[float, str]]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the BigDecimal value
    """
    builder = _JBigDecimalValue.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=True,
        allow_string=allow_string,
    )
    if on_missing:
        builder.onMissing(dtypes.BigDecimal(str(on_missing)))
    if on_null:
        builder.onNull(dtypes.BigDecimal(str(on_null)))
    return JsonValue(builder.build())


def any_val() -> JsonValue:
    """Creates an "any" value. The resulting type is implementation dependant.

    Returns:
        the "any" value
    """
    return JsonValue(_JAnyValue.of())


def skip_val(
    allow_missing: Optional[bool] = None,
    allow_null: Optional[bool] = None,
    allow_int: Optional[bool] = None,
    allow_decimal: Optional[bool] = None,
    allow_string: Optional[bool] = None,
    allow_bool: Optional[bool] = None,
    allow_object: Optional[bool] = None,
    allow_array: Optional[bool] = None,
    allow_by_default: bool = True,
) -> JsonValue:
    """Creates a "skip" value. No resulting type will be returned, but the JSON types will be validated as configured.
    This may be useful in combination with an object type where allow_unknown_fields=False. For example, the JSON object

    .. code-block:: json
        { "name": "foo", "age": 42 }

    might be modelled as the object type

    .. code-block:: python
        object_val({ "name": str, "age": skip_val() }, allow_unknown_fields=False)

    Args:
        allow_missing (Optional[bool]): if a missing JSON value is allowed, by default is None
        allow_null (Optional[bool]): if a JSON null type is allowed, by default is None
        allow_int (Optional[bool]): if a JSON integer type is allowed, by default is None
        allow_decimal (Optional[bool]): if a JSON decimal type is allowed, by default is None
        allow_string (Optional[bool]): if a JSON string type is allowed, by default is None
        allow_bool (Optional[bool]): if a JSON boolean type is allowed, by default is None
        allow_object (Optional[bool]): if a JSON object type is allowed, by default is None
        allow_array (Optional[bool]): if a JSON array type is allowed, by default is None
        allow_by_default (bool): the default behavior for the other arguments when they are set to None, by default is True

    Returns:
        the "skip" value
    """

    def _allow(x: Optional[bool]) -> bool:
        return x if x is not None else allow_by_default

    builder = _JSkipValue.builder()
    _build(
        builder,
        allow_missing=_allow(allow_missing),
        allow_null=_allow(allow_null),
        allow_int=_allow(allow_int),
        allow_decimal=_allow(allow_decimal),
        allow_string=_allow(allow_string),
        allow_bool=_allow(allow_bool),
        allow_object=_allow(allow_object),
        allow_array=_allow(allow_array),
    )
    return JsonValue(builder.build())


_dtype_dict = {
    dtypes.bool_: bool_val(),
    dtypes.char: char_val(),
    dtypes.int8: byte_val(),
    dtypes.int16: short_val(),
    dtypes.int32: int_val(),
    dtypes.int64: long_val(),
    dtypes.float32: float_val(),
    dtypes.float64: double_val(),
    dtypes.string: string_val(),
    dtypes.Instant: instant_val(),
    dtypes.BigInteger: big_integer_val(),
    dtypes.BigDecimal: big_decimal_val(),
    dtypes.JObject: any_val(),
    dtypes.bool_array: array_val(bool_val()),
    dtypes.char_array: array_val(char_val()),
    dtypes.int8_array: array_val(byte_val()),
    dtypes.int16_array: array_val(short_val()),
    dtypes.int32_array: array_val(int_val()),
    dtypes.int64_array: array_val(long_val()),
    dtypes.float32_array: array_val(float_val()),
    dtypes.float64_array: array_val(double_val()),
    dtypes.string_array: array_val(string_val()),
    dtypes.instant_array: array_val(instant_val()),
}

_type_dict = {
    bool: bool_val(),
    int: long_val(),
    float: double_val(),
    str: string_val(),
    datetime: instant_val(),
    object: any_val(),
}
