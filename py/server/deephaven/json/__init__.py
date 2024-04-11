#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""The deephaven JSON module presents a declarative and composable configuration layer for describing the structure of a
JSON value. It is meant to have sane defaults while also providing finer-grained configuration options for typical
scenarios. The primary purpose of this module is to provide a common layer that various consumers can use to parse JSON
values into appropriate Deephaven structures. As such (and by the very nature of JSON), these types represent a superset
of JSON. This module can also service other use cases where the JSON structuring is necessary (for example, producing a
JSON value from a Deephaven structure).
"""

# todo: should be on the classpath by default, but doesn't have to be
# todo: would be nice if this code could live in the JSON jar

import jpy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Union, Tuple, Optional, Literal, Any

from deephaven import dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.time import to_j_instant

__all__ = [
    "string_",
    "bool_",
    "char_",
    "byte_",
    "short_",
    "int_",
    "long_",
    "float_",
    "double_",
    "instant_",
    "big_integer_",
    "big_decimal_",
    "array_",
    "object_",
    "object_kv_",
    "tuple_",
    "any_",
    "skip_",
    "json",
    "JsonOptions",
    "JsonValueType",
    "RepeatedFieldBehavior",
    "FieldOptions",
]

# https://deephaven.atlassian.net/browse/DH-15061
# It is important that ValueOptions gets imported before the others.
_JValueOptions = jpy.get_type("io.deephaven.json.ValueOptions")
_JObjectOptions = jpy.get_type("io.deephaven.json.ObjectOptions")
_JArrayOptions = jpy.get_type("io.deephaven.json.ArrayOptions")
_JObjectKvOptions = jpy.get_type("io.deephaven.json.ObjectKvOptions")
_JTupleOptions = jpy.get_type("io.deephaven.json.TupleOptions")
_JObjectFieldOptions = jpy.get_type("io.deephaven.json.ObjectFieldOptions")
_JRepeatedFieldBehavior = jpy.get_type(
    "io.deephaven.json.ObjectFieldOptions$RepeatedBehavior"
)
_JJsonValueTypes = jpy.get_type("io.deephaven.json.JsonValueTypes")
_JBoolOptions = jpy.get_type("io.deephaven.json.BoolOptions")
_JCharOptions = jpy.get_type("io.deephaven.json.CharOptions")
_JByteOptions = jpy.get_type("io.deephaven.json.ByteOptions")
_JShortOptions = jpy.get_type("io.deephaven.json.ShortOptions")
_JIntOptions = jpy.get_type("io.deephaven.json.IntOptions")
_JLongOptions = jpy.get_type("io.deephaven.json.LongOptions")
_JFloatOptions = jpy.get_type("io.deephaven.json.FloatOptions")
_JDoubleOptions = jpy.get_type("io.deephaven.json.DoubleOptions")
_JStringOptions = jpy.get_type("io.deephaven.json.StringOptions")
_JSkipOptions = jpy.get_type("io.deephaven.json.SkipOptions")
_JInstantOptions = jpy.get_type("io.deephaven.json.InstantOptions")
_JInstantNumberOptions = jpy.get_type("io.deephaven.json.InstantNumberOptions")
_JInstantNumberOptionsFormat = jpy.get_type(
    "io.deephaven.json.InstantNumberOptions$Format"
)
_JBigIntegerOptions = jpy.get_type("io.deephaven.json.BigIntegerOptions")
_JBigDecimalOptions = jpy.get_type("io.deephaven.json.BigDecimalOptions")
_JAnyOptions = jpy.get_type("io.deephaven.json.AnyOptions")


_VALUE_STRING = _JJsonValueTypes.STRING
_VALUE_NULL = _JJsonValueTypes.NULL
_VALUE_INT = _JJsonValueTypes.INT
_VALUE_DECIMAL = _JJsonValueTypes.DECIMAL
_VALUE_BOOL = _JJsonValueTypes.BOOL
_VALUE_OBJECT = _JJsonValueTypes.OBJECT
_VALUE_ARRAY = _JJsonValueTypes.ARRAY

_EPOCH_SECONDS = _JInstantNumberOptionsFormat.EPOCH_SECONDS
_EPOCH_MILLIS = _JInstantNumberOptionsFormat.EPOCH_MILLIS
_EPOCH_MICROS = _JInstantNumberOptionsFormat.EPOCH_MICROS
_EPOCH_NANOS = _JInstantNumberOptionsFormat.EPOCH_NANOS


class JsonOptions(JObjectWrapper):
    """The JSON options object. Provides a named object processor provider."""

    j_object_type = _JValueOptions

    def __init__(self, j_options: jpy.JType):
        self.j_options = j_options

    @property
    def j_object(self) -> jpy.JType:
        return self.j_options


# todo use type alias instead of Any in the future
# todo named tuple
JsonValueType = Union[
    JsonOptions,
    dtypes.DType,
    type,
    Dict[str, Union["JsonValueType", "FieldOptions"]],
    List["JsonValueType"],
    Tuple["JsonValueType", ...],
]


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
class FieldOptions:
    """The object field options.

    In contexts where the user needs to create an object field value and isn't changing any default values, the user can
    simplify by just using the JsonValueType. For example,

    .. code-block:: python
        {
            "name": FieldOptions(str),
            "age": FieldOptions(int),
        }

    could be simplified to

    .. code-block:: python
        {
            "name": str,
            "age": int,
        }
    """

    value: JsonValueType
    """The json value type"""
    aliases: Union[str, List[str]] = field(default_factory=list)
    """The field name aliases. By default, is an empty list."""
    repeated_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.ERROR
    """The repeated field behavior. By default, is RepeatedFieldBehavior.ERROR."""
    case_sensitive: bool = True
    """If the field name and aliases should be compared using case-sensitive equality. By default, is True."""

    def _j_field_options(self, name: str) -> jpy.JType:
        builder = (
            _JObjectFieldOptions.builder()
            .name(name)
            .options(json(self.value).j_options)
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


def object_(
    fields: Dict[str, Union[JsonValueType, FieldOptions]],
    allow_unknown_fields: bool = True,
    allow_missing: bool = True,
    allow_null: bool = True,
    repeated_field_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.ERROR,
    case_sensitive: bool = True,
) -> JsonOptions:
    """Creates an object options. For example, the JSON object

    .. code-block:: json
        { "name": "foo", "age": 42 }

    might be modelled as the object type

    .. code-block:: python
        object_({ "name": str, "age": int })

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using a Dict[str, Union[JsonValueType, FieldOptions]]. For example,

    .. code-block:: python
        some_method(object_({ "name": str, "age": int }))

    could be simplified to

    .. code-block:: python
        some_method({ "name": str, "age": int })

    Args:
        fields (Dict[str, Union[JsonValueType, FieldOptions]]): the fields
        allow_unknown_fields (bool): if unknown fields are allow, by default is True
        allow_missing (bool): if the object is allowed to be missing, by default is True
        allow_null (bool): if the object is allowed to be a JSON null type, by default is True
        repeated_field_behavior (RepeatedFieldBehavior): the default repeated field behavior, only used for fields that
            are specified using JsonValueType, by default is RepeatedFieldBehavior.ERROR
        case_sensitive (bool): if default to use for field case-sensitivity. Only used for fields that are specified
            using JsonValueType, by default is True

    Returns:
        the object options
    """
    builder = _JObjectOptions.builder()
    _build(builder, allow_missing, allow_null, allow_object=True)
    builder.allowUnknownFields(allow_unknown_fields)
    for field_name, field_opts in fields.items():
        field_opts = (
            field_opts
            if isinstance(field_opts, FieldOptions)
            else FieldOptions(
                field_opts,
                repeated_behavior=repeated_field_behavior,
                case_sensitive=case_sensitive,
            )
        )
        # noinspection PyProtectedMember
        builder.addFields(field_opts._j_field_options(field_name))
    return JsonOptions(builder.build())


def array_(
    element: JsonValueType,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonOptions:
    """Creates a array of element options. For example, the JSON array

    .. code-block:: json
        [1, 42, 43, 13]

    might be modelled as an array of ints

    .. code-block:: python
        array_(int)

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using a list with a single element type. For example,

    .. code-block:: python
        some_method(array_(element))

    could be simplified to

    .. code-block:: python
        some_method([element])

    Args:
        element (JsonValueType): the array element type
        allow_missing (bool): if the array is allowed to be missing, by default is True
        allow_null (bool): if the array is allowed to be a JSON null type, by default is True

    Returns:
        the array options
    """
    builder = _JArrayOptions.builder()
    builder.element(json(element).j_options)
    _build(builder, allow_missing, allow_null, allow_array=True)
    return JsonOptions(builder.build())


def object_kv_(
    key_type: JsonValueType = str,
    value_type: Optional[JsonValueType] = None,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonOptions:
    """Creates an object key-value options. This is used in situations where the number of fields in an object is
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
        object_kv_(value_element=int)

    Args:
        key_type (JsonValueType): the key element, by defaults is type str
        value_type (Optional[JsonValueType]): the value element, required
        allow_missing (bool): if the object is allowed to be missing, by default is True
        allow_null (bool): if the object is allowed to be a JSON null type, by default is True

    Returns:
        the object kv options
    """
    builder = _JObjectKvOptions.builder()
    builder.key(json(key_type).j_options)
    builder.value(json(value_type).j_options)
    _build(builder, allow_missing, allow_null, allow_object=True)
    return JsonOptions(builder.build())


def tuple_(
    values: Union[Tuple[JsonValueType, ...], Dict[str, JsonValueType]],
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonOptions:
    """Creates a tuple options. For example, the JSON array

    .. code-block:: json
        ["foo", 42, 5.72]

    might be modelled as the tuple type

    .. code-block:: python
        tuple_((str, int, float))

    To provide meaningful names, a dictionary can be used:

    .. code-block:: python
        tuple_({"name": str, "age": int, "height": float})

    In contexts where the user needs to create a JsonValueType and isn't changing any default values nor is setting
    names, the user can simplify passing through a python tuple type. For example,

    .. code-block:: python
        some_method(tuple_((tuple_type_1, tuple_type_2)))

    could be simplified to

    .. code-block:: python
        some_method((tuple_type_1, tuple_type_2))

    Args:
        values (Union[Tuple[JsonValueType, ...], Dict[str, JsonValueType]]): the tuple value types
        allow_missing (bool): if the array is allowed to be missing, by default is True
        allow_null (bool): if the array is allowed to be a JSON null type, by default is True
    Returns:
        the tuple options
    """
    if isinstance(values, Tuple):
        kvs = enumerate(values)
    elif isinstance(values, Dict):
        kvs = values.items()
    else:
        raise TypeError(f"Invalid tuple type: {type(values)}")
    builder = _JTupleOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_array=True,
    )
    for name, json_value_type in kvs:
        builder.putNamedValues(str(name), json(json_value_type).j_options)
    return JsonOptions(builder.build())


def bool_(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[bool] = None,
    on_null: Optional[bool] = None,
) -> JsonOptions:
    """Creates a bool options. For example, the JSON boolean

    .. code-block:: json
        True

    might be modelled as the bool type

    .. code-block:: python
        bool_()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in bool type. For example,

    .. code-block:: python
        some_method(bool_())

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
        the bool options
    """
    builder = _JBoolOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_bool=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def char_(
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[str] = None,
    on_null: Optional[str] = None,
) -> JsonOptions:
    """Creates a char options. For example, the JSON string

    .. code-block:: json
        "F"

    might be modelled as the char type

    .. code-block:: python
        char_()

    Args:
        allow_missing (bool): if the char value is allowed to be missing, default is True
        allow_null (bool): if the char value is allowed to be a JSON null type, default is True
        on_missing (Optional[str]): the value to use when the JSON value is missing and allow_missing is True, default is None. If specified, must be a single character.
        on_null (Optional[str]): the value to use when the JSON value is null and allow_null is True, default is None. If specified, must be a single character.

    Returns:
        the char options
    """
    builder = _JCharOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_string=True,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def byte_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonOptions:
    """Creates a byte (signed 8-bit) options. For example, the JSON integer

    .. code-block:: json
        42

    might be modelled as the byte type

    .. code-block:: python
        byte_()

    Args:
        allow_decimal (bool): if the byte value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the byte value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the byte value is allowed to be missing, default is True
        allow_null (bool): if the byte value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the byte options
    """
    builder = _JByteOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def short_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonOptions:
    """Creates a short (signed 16-bit) options. For example, the JSON integer

    .. code-block:: json
        30000

    might be modelled as the short type

    .. code-block:: python
        short_()

    Args:
        allow_decimal (bool): if the short value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the short value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the short value is allowed to be missing, default is True
        allow_null (bool): if the short value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the short options
    """
    builder = _JShortOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def int_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonOptions:
    """Creates an int (signed 32-bit) options. For example, the JSON integer

    .. code-block:: json
        100000

    might be modelled as the int type

    .. code-block:: python
        int_()

    Args:
        allow_decimal (bool): if the int value is allowed to be a JSON decimal type, default is False
        allow_string (bool): if the int value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the int value is allowed to be missing, default is True
        allow_null (bool): if the int value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the int options
    """
    builder = _JIntOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def long_(
    allow_decimal: bool = False,
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[int] = None,
    on_null: Optional[int] = None,
) -> JsonOptions:
    """Creates a long (signed 64-bit) options. For example, the JSON integer

    .. code-block:: json
        8000000000

    might be modelled as the long type

    .. code-block:: python
        long_()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in long type. For example,

    .. code-block:: python
        some_method(long_())

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
        the long options
    """
    builder = _JLongOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def float_(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[float] = None,
    on_null: Optional[float] = None,
) -> JsonOptions:
    """Creates a float (signed 32-bit) options. For example, the JSON decimal

    .. code-block:: json
        42.42

    might be modelled as the float type

    .. code-block:: python
        float_()

    Args:
        allow_string (bool): if the float value is allowed to be a JSON string type, default is False
        allow_missing (bool): if the float value is allowed to be missing, default is True
        allow_null (bool): if the float value is allowed to be a JSON null type, default is True
        on_missing (Optional[float]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[float]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the float options
    """
    builder = _JFloatOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def double_(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[float] = None,
    on_null: Optional[float] = None,
) -> JsonOptions:
    """Creates a double (signed 64-bit) options. For example, the JSON decimal

    .. code-block:: json
        42.42424242

    might be modelled as the double type

    .. code-block:: python
        double_()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in float type. For example,

    .. code-block:: python
        some_method(double_())

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
        the double options
    """
    builder = _JDoubleOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_decimal=True,
        allow_int=True,
        allow_string=allow_string,
    )
    if on_null:
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


def string_(
    allow_int: bool = False,
    allow_decimal: bool = False,
    allow_bool: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[str] = None,
    on_null: Optional[str] = None,
) -> JsonOptions:
    """Creates a String options. For example, the JSON string

    .. code-block:: json
        "Hello, world!"

    might be modelled as the string type

    .. code-block:: python
        string_()

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python built-in str type. For example,

    .. code-block:: python
        some_method(string_())

    could be simplified to

    .. code-block:: python
        some_method(str)

    Args:
        allow_int (bool): if the string value is allowed to be a JSON integer type, default is False
        allow_decimal (bool): if the string value is allowed to be a JSON decimal type, default is False
        allow_bool (bool): if the string value is allowed to be a JSON boolean type, default is False
        allow_missing (bool): if the double value is allowed to be missing, default is True
        allow_null (bool): if the double value is allowed to be a JSON null type, default is True
        on_missing (Optional[int]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[int]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the double options
    """
    builder = _JStringOptions.builder()
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
        builder.onNull(onNull)
    if on_missing:
        builder.onMissing(on_missing)
    return JsonOptions(builder.build())


# TODO(deephaven-core#5269): Create deephaven.time time-type aliases
def instant_(
    allow_missing: bool = True,
    allow_null: bool = True,
    number_format: Literal[None, "s", "ms", "us", "ns"] = None,
    allow_decimal: bool = False,
    on_missing: Optional[Any] = None,
    on_null: Optional[Any] = None,
) -> JsonOptions:
    """Creates an Instant options. For example, the JSON string

    .. code-block:: json
        "2009-02-13T23:31:30.123456789Z"

    might be modelled as the Instant type

    .. code-block:: python
        instant_()

    In another example, the JSON decimal

    .. code-block:: json
        1234567890.123456789

    might be modelled as the Instant type

    .. code-block:: python
        instant_(number_format="s", allow_decimal=True)

    In contexts where the user needs to create a JsonValueType and isn't changing any default values, the user can
    simplify by using the python datetime type. For example,

    .. code-block:: python
        some_method(instant_())

    could be simplified to

    .. code-block:: python
        some_method(datetime)

    Args:
        allow_missing (bool): if the Instant value is allowed to be missing, default is True
        allow_null (bool): if the Instant value is allowed to be a JSON null type, default is True
        number_format (Literal[None, "s", "ms", "us", "ns"]): when set, signifies that a JSON numeric type is expected.
            "s" is for seconds, "ms" is for milliseconds, "us" is for microseconds, and "ns" is for nanoseconds.
        allow_decimal (bool): if the Instant value is allowed to be a JSON decimal type, default is False. Only valid
            when number_format is specified.
        on_missing (Optional[Any]): the value to use when the JSON value is missing and allow_missing is True, default is None.
        on_null (Optional[Any]): the value to use when the JSON value is null and allow_null is True, default is None.

    Returns:
        the Instant options
    """
    if number_format:
        builder = _JInstantNumberOptions.builder()
        if on_missing:
            builder.onMull(to_j_instant(on_missing))
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
        return JsonOptions(builder.build())
    else:
        if allow_decimal:
            raise TypeError("allow_decimal is only valid when using number_format")
        builder = _JInstantOptions.builder()
        if on_missing:
            builder.onMull(to_j_instant(on_missing))
        if on_null:
            builder.onNull(to_j_instant(on_null))
        _build(
            builder,
            allow_missing,
            allow_null,
            allow_string=True,
        )
        return JsonOptions(builder.build())


def big_integer_(
    allow_string: bool = False,
    allow_decimal: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    # todo on_null, on_missing
) -> JsonOptions:
    """Creates a BigInteger options. For example, the JSON integer

    .. code-block:: json
        123456789012345678901

    might be modelled as the BigInteger type

    .. code-block:: python
        big_integer_()

    Args:
        allow_string (bool): if the BigInteger value is allowed to be a JSON string type, default is False.
        allow_decimal (bool): if the BigInteger value is allowed to be a JSON decimal type, default is False.
        allow_missing (bool): if the BigInteger value is allowed to be missing, default is True
        allow_null (bool): if the BigInteger value is allowed to be a JSON null type, default is True

    Returns:
        the BigInteger options
    """
    builder = _JBigIntegerOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
    )
    return JsonOptions(builder.build())


def big_decimal_(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    # todo on_null, on_missing
) -> JsonOptions:
    """Creates a BigDecimal options. For example, the JSON decimal

    .. code-block:: json
        123456789012345678901.42

    might be modelled as the BigDecimal type

    .. code-block:: python
        big_decimal_()

    Args:
        allow_string (bool): if the BigDecimal value is allowed to be a JSON string type, default is False.
        allow_missing (bool): if the BigDecimal value is allowed to be missing, default is True
        allow_null (bool): if the BigDecimal value is allowed to be a JSON null type, default is True

    Returns:
        the BigDecimal options
    """
    builder = _JBigDecimalOptions.builder()
    _build(
        builder,
        allow_missing,
        allow_null,
        allow_int=True,
        allow_decimal=True,
        allow_string=allow_string,
    )
    return JsonOptions(builder.build())


def any_() -> JsonOptions:
    """Creates an "any" options. The resulting type is implementation dependant.

    Returns:
        the "any" options
    """
    return JsonOptions(_JAnyOptions.of())


def skip_(
    allow_missing: Optional[bool] = None,
    allow_null: Optional[bool] = None,
    allow_int: Optional[bool] = None,
    allow_decimal: Optional[bool] = None,
    allow_string: Optional[bool] = None,
    allow_bool: Optional[bool] = None,
    allow_object: Optional[bool] = None,
    allow_array: Optional[bool] = None,
    allow_by_default: bool = True,
) -> JsonOptions:
    """Creates a "skip" type. No resulting type will be returned, but the JSON types will be validated as configured.
    This may be useful in combination with an object type where allow_unknown_fields=False. For example, the JSON object

    .. code-block:: json
        { "name": "foo", "age": 42 }

    might be modelled as the object type

    .. code-block:: python
        object_({ "name": str, "age": skip_() }, allow_unknown_fields=False)

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
        the "skip" options
    """

    def _allow(x: Optional[bool]) -> bool:
        return x if x is not None else allow_by_default

    builder = _JSkipOptions.builder()
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
    return JsonOptions(builder.build())


def json(json_value_type: JsonValueType) -> JsonOptions:
    """Creates a JsonOptions from a JsonValueType.

    Args:
        json_value_type (JsonValueType): the JSON value type

    Returns:
        the JSON options
    """
    if isinstance(json_value_type, JsonOptions):
        return json_value_type
    if isinstance(json_value_type, dtypes.DType):
        return _dtype_dict[json_value_type]
    if isinstance(json_value_type, type):
        return _type_dict[json_value_type]
    if isinstance(json_value_type, Dict):
        return object_(json_value_type)
    if isinstance(json_value_type, List):
        if len(json_value_type) is not 1:
            raise TypeError("Expected List as json type to have exactly one element")
        return array_(json_value_type[0])
    if isinstance(json_value_type, Tuple):
        return tuple_(json_value_type)
    raise TypeError(f"Unsupported JSON value type {type(json_value_type)}")


_dtype_dict = {
    dtypes.bool_: bool_(),
    dtypes.char: char_(),
    dtypes.int8: byte_(),
    dtypes.int16: short_(),
    dtypes.int32: int_(),
    dtypes.int64: long_(),
    dtypes.float32: float_(),
    dtypes.float64: double_(),
    dtypes.string: string_(),
    dtypes.Instant: instant_(),
    dtypes.BigInteger: big_integer_(),
    dtypes.BigDecimal: big_decimal_(),
    dtypes.JObject: any_(),
    dtypes.bool_array: array_(bool_()),
    dtypes.char_array: array_(char_()),
    dtypes.int8_array: array_(byte_()),
    dtypes.int16_array: array_(short_()),
    dtypes.int32_array: array_(int_()),
    dtypes.int64_array: array_(long_()),
    dtypes.float32_array: array_(float_()),
    dtypes.float64_array: array_(double_()),
    dtypes.string_array: array_(string_()),
    dtypes.instant_array: array_(instant_()),
}

_type_dict = {
    bool: bool_(),
    int: long_(),
    float: double_(),
    str: string_(),
    datetime: instant_(),
    object: any_(),
}
