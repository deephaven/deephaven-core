#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy
from datetime import datetime
from enum import Enum
from typing import Dict, List, Union, Tuple, Optional, Literal

from deephaven import dtypes
from deephaven._wrapper import JObjectWrapper

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
]

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
_JValueOptions = jpy.get_type("io.deephaven.json.ValueOptions")


class JsonOptions(JObjectWrapper):
    j_object_type = _JValueOptions

    def __init__(self, j_options: jpy.JType):
        self.j_options = j_options

    @property
    def j_object(self) -> jpy.JType:
        return self.j_options


class RepeatedFieldBehavior(Enum):
    ERROR = _JRepeatedFieldBehavior.ERROR
    USE_FIRST = _JRepeatedFieldBehavior.USE_FIRST


# todo use type alias instead of Any in the future
# todo named tuple
JsonValueType = Union[
    JsonOptions,
    dtypes.DType,
    type,
    Dict[str, "JsonValueType"],
    List["JsonValueType"],
    Tuple["JsonValueType", ...],
]

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
    builder.desiredTypes(
        ([_VALUE_STRING] if allow_string else [])
        + ([_VALUE_NULL] if allow_null else [])
        + ([_VALUE_INT] if allow_int else [])
        + ([_VALUE_DECIMAL] if allow_decimal else [])
        + ([_VALUE_BOOL] if allow_bool else [])
        + ([_VALUE_OBJECT] if allow_object else [])
        + ([_VALUE_ARRAY] if allow_array else [])
    )


def object_(
    fields: Dict[str, JsonValueType],
    allow_unknown_fields: bool = True,
    allow_missing: bool = True,
    allow_null: bool = True,
    repeated_field_behavior: RepeatedFieldBehavior = RepeatedFieldBehavior.USE_FIRST,
    case_insensitive: bool = False,
) -> JsonOptions:
    builder = _JObjectOptions.builder()
    _build(builder, allow_missing, allow_null, allow_object=True)
    builder.allowUnknownFields(allow_unknown_fields)
    for field_name, field_opts in fields.items():
        builder.addFields(
            _JObjectFieldOptions.builder()
            .name(field_name)
            .options(json(field_opts).j_options)
            .repeatedBehavior(repeated_field_behavior.value)
            .caseInsensitiveMatch(case_insensitive)
            .build()
        )
    return JsonOptions(builder.build())


def array_(
    element: JsonValueType,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonOptions:
    builder = _JArrayOptions.builder()
    builder.element(json(element).j_options)
    _build(builder, allow_missing, allow_null, allow_array=True)
    return JsonOptions(builder.build())


def object_kv_(
    key_element: Optional[JsonValueType] = None,
    value_element: Optional[JsonValueType] = None,
    allow_missing: bool = True,
    allow_null: bool = True,
) -> JsonOptions:
    builder = _JObjectKvOptions.builder()
    if key_element is not None:
        builder.key(json(key_element).j_options)
    if value_element is not None:
        builder.value(json(value_element).j_options)
    _build(builder, allow_missing, allow_null, allow_object=True)
    return JsonOptions(builder.build())


def tuple_(values: Tuple[JsonValueType, ...]) -> JsonOptions:
    return JsonOptions(_JTupleOptions.of([json(opt).j_options for opt in values]))


def bool_(
    allow_string: bool = False,
    allow_missing: bool = True,
    allow_null: bool = True,
    on_missing: Optional[bool] = None,
    on_null: Optional[bool] = None,
) -> JsonOptions:
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


def instant_(
    allow_missing: bool = True,
    allow_null: bool = True,
    number_format: Literal[None, "s", "ms", "us", "ns"] = None,
    allow_decimal: bool = False,
) -> JsonOptions:
    if number_format:
        builder = _JInstantNumberOptions.builder()
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
            raise TypeError(f"allow_decimal is only valid when using number_format")
        builder = _JInstantOptions.builder()
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
    return JsonOptions(_JAnyOptions.of())


def skip_(
    allow_missing: bool = True,
    allow_null: bool = True,
    allow_int: bool = True,
    allow_decimal: bool = True,
    allow_string: bool = True,
    allow_bool: bool = True,
    allow_object: bool = True,
    allow_array: bool = True,
) -> JsonOptions:
    builder = _JSkipOptions.builder()
    _build(
        builder,
        allow_missing=allow_missing,
        allow_null=allow_null,
        allow_int=allow_int,
        allow_decimal=allow_decimal,
        allow_string=allow_string,
        allow_bool=allow_bool,
        allow_object=allow_object,
        allow_array=allow_array,
    )
    return JsonOptions(builder.build())


def json(json_value_type: JsonValueType) -> JsonOptions:
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
    raise TypeError("unexpected")


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
