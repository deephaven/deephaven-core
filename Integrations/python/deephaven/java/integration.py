import jpy

from . import primitives

__DeephavenCompatibleFunction__ = jpy.get_type('io.deephaven.db.v2.select.python.DeephavenCompatibleFunction')
__FormulaColumnPython__ = jpy.get_type('io.deephaven.db.v2.select.python.FormulaColumnPython')
__ConditionFilterPython__ = jpy.get_type('io.deephaven.db.v2.select.python.ConditionFilterPython')


def formula(func, *, name, column_names, return_type=None, is_vectorized=None):
    if hasattr(func, '__dh_return_type__'):
        return_type = return_type if return_type is not None else func.__dh_return_type__
    if hasattr(func, '__dh_is_vectorized__'):
        is_vectorized = is_vectorized if is_vectorized is not None else func.__dh_is_vectorized__
    if return_type is None:
        raise TypeError("Must provide return_type")
    if is_vectorized is None:
        raise TypeError("Must provide is_vectorized")
    return __FormulaColumnPython__.create(name, __DeephavenCompatibleFunction__.create(func, return_type, column_names,
                                                                                       is_vectorized))


def filter(func, *, column_names, is_vectorized):
    if hasattr(func, '__dh_is_vectorized__'):
        is_vectorized = is_vectorized if is_vectorized is not None else func.__dh_is_vectorized__
    return __ConditionFilterPython__.create(
        __DeephavenCompatibleFunction__.create(func, primitives.boolean, column_names, is_vectorized))
