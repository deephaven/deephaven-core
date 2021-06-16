#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""Utilities for converting between Deephaven tables and numpy arrays."""

import collections
import jpy
import wrapt
import numpy as np


__all__ = ['numpy_sample', 'numpy_slice']


_class_name_ = "io.deephaven.integrations.numpy.Java2NumpyCopy"
_Java2NumpyCopy_ = None

_supportedNpTypesNumber_ = [np.int8, np.int16, np.int32, np.int64, np.float32, np.float64]
__supportedNpTypesImage_ = [np.int16, np.int32, np.int64, np.float32, np.float64]


def _annotate_Java2NumpyCopy_methods(type, method):
    if method.name == 'copySlice':
        method.set_param_mutable(2, True)
        # method.set_param_return(0, True)

    if method.name == 'copyRand':
        method.set_param_mutable(1, True)
        # method.set_param_return(0, True)

    if method.name == 'copyImageSlice':
        method.set_param_mutable(2, True)
        # method.set_param_return(0, True)

    if method.name == 'copyImageRand':
        method.set_param_mutable(1, True)
        # method.set_param_return(0, True)

    return True


jpy.type_callbacks[_class_name_] = _annotate_Java2NumpyCopy_methods


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _Java2NumpyCopy_
    if _Java2NumpyCopy_ is None:
        # This will raise an exception if the desired object is not in the classpath
        _Java2NumpyCopy_ = jpy.get_type(_class_name_)


# every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


def _isSequence(a):
    return isinstance(a, collections.Sequence)


def _toSequence(dtype, n):
    if _isSequence(dtype):
        return dtype
    else:
        return [dtype for i in range(n)]


def _check_supported_dtype_(dtype, type_collection):
    if dtype not in type_collection:
        raise ValueError("Unsupported dtype, dtype={}, supported={}".format(dtype, type_collection))


def _numpySliceNumber(t, rowStart, nRow, dtype):
    """
    Creates a numpy array and populates it with sequential data from a NUMBER table.

    :param t: input table
    :param rowStart: starting row to copy
    :param nRow:  number of rows to copy
    :param dtype: numpy datatype for the resulting array
    :return: numpy array
    """

    _check_supported_dtype_(dtype, _supportedNpTypesNumber_)

    nCol = len(t.getColumns())
    a = np.empty([nRow, nCol], dtype=dtype, order="C")
    _Java2NumpyCopy_.copySlice(t, rowStart, a, nRow, nCol)
    return a


def _numpySliceImage(t, rowStart, nRow, dtype, imageWidth, imageHeight, imageResize, imageColor):
    """
    Creates a numpy array and populates it with sequential data from an IMAGE table.

    :param t: input table
    :param rowStart: starting row to copy
    :param nRow: number of rows to copy
    :param dtype: numpy datatype for the resulting array
    :param imageWidth: width of the output image in pixels
    :param imageHeight: height of the output image in pixels
    :param imageResize: true to resize the image to the desired size; false to raise an error if the image is
      not the desired size
    :param imageColor: true to output a color image; false to output a grayscale image
    :return: numpy array
    """

    _check_supported_dtype_(dtype, __supportedNpTypesImage_)

    if imageWidth is None:
        raise ValueError("imageWidth must be set for image tables.")

    if imageHeight is None:
        raise ValueError("imageHeight must be set for image tables.")

    if imageColor is None:
        raise ValueError("imageColor must be set for image tables.")

    if imageColor:
        shape = [nRow, imageHeight, imageWidth, 3]
    else:
        shape = [nRow, imageHeight, imageWidth]

    a = np.empty(shape, dtype=dtype, order="C")
    _Java2NumpyCopy_.copyImageSlice(t, rowStart, a, nRow, imageWidth, imageHeight, imageResize, imageColor)
    return a


def _numpyRandNumber(t, rows, dtype):
    """
    Creates a numpy array and populates it with random-access data from a NUMBER table.

    :param t: input table
    :param rows: array of row indices to include in the result
    :param dtype: numpy datatype for the resulting array
    :return: numpy array
    """

    _check_supported_dtype_(dtype, _supportedNpTypesNumber_)

    nRow = len(rows)
    nCol = len(t.getColumns())
    a = np.empty([nRow, nCol], dtype=dtype, order="C")
    _Java2NumpyCopy_.copyRand(t, a, nRow, nCol, rows)
    return a


def _numpyRandImage(t, rows, dtype, imageWidth, imageHeight, imageResize, imageColor):
    """
    Creates a numpy array and populates it with random-access data from an IMAGE table.

    :param t: input table
    :param rows: array of row indices to include in the result
    :param dtype: numpy datatype for the resulting array
    :param imageWidth: width of the output image in pixels
    :param imageHeight: height of the output image in pixels
    :param imageResize: true to resize the image to the desired size; false to raise an error if the image
      is not the desired size
    :param imageColor: true to output a color image; false to output a grayscale image
    :return: numpy array
    """

    _check_supported_dtype_(dtype, __supportedNpTypesImage_)

    if imageWidth is None:
        raise ValueError("imageWidth must be set for image tables.")

    if imageHeight is None:
        raise ValueError("imageHeight must be set for image tables.")

    if imageColor is None:
        raise ValueError("imageColor must be set for image tables.")

    nRow = len(rows)

    if imageColor:
        shape = [nRow, imageHeight, imageWidth, 3]
    else:
        shape = [nRow, imageHeight, imageWidth]

    a = np.empty(shape, dtype=dtype, order="C")
    _Java2NumpyCopy_.copyImageRand(t, a, nRow, imageWidth, imageHeight, imageResize, imageColor, rows)
    return a


def _validateSize(t, item, name):
    """
    Helper method for checking consistency of input.

    :param t: single table or list of tables
    :param item: single item or list of items to be checked for shape consistency with t
    :param name: name of item type - for logging
    :return: appropriate version of item, in keeping with t
    """

    if _isSequence(t):
        item = _toSequence(item, len(t))
        if len(t) != len(item):
            raise ValueError("Number of tables does not match number of {0!s}. "
                             "len(t)={1:d} len({0:s})={2:d}".format(name, len(t), len(item)))
        return item

    if _isSequence(item):
        if len(item) != 1:
            raise ValueError("Number of tables does not match number of {0:s}. "
                             "len(t)=1 len({0:s})={1:d}".format(name, len(item)))
        return item[0]

    return item


@_passThrough
def numpy_slice(t, rowStart, nRow, dtype=np.float64, imageWidth=None, imageHeight=None, imageResize=True, imageColor=None):
    """
    Convert a table or tables into numpy arrays.

    :param t: table or list of tables
    :param rowStart: first row of the slice
    :param nRow: number of rows in the slice
    :param dtype: numpy datatype or array of numpy datatypes to generate from the table(s)
    :param imageWidth: width of the output image in pixels.
    :param imageHeight: height of the output image in pixels.
    :param imageResize: True to resize images; False otherwise.
    :param imageColor: True to return color images; False to return gray-scale images.
    :return: numpy data sampled from the input tables.  Scalar if one input table or tuple if input table list.
    """

    dt = _validateSize(t, dtype, 'dtype')
    iw = _validateSize(t, imageWidth, 'imageWidth')
    ih = _validateSize(t, imageHeight, 'imageHeight')
    ir = _validateSize(t, imageResize, 'imageResize')
    ic = _validateSize(t, imageColor, 'imageColor')

    if _isSequence(t):
        rst = []
        s = None

        for tt, dti, iwi, ihi, iri, ici in zip(t, dt, iw, ih, ir, ic):
            if s is None:
                s = tt.size()
            elif s != tt.size():
                raise ValueError("Tables are different sizes ({} != {})".format(s, tt.size()))

            rst.append(numpy_slice(tt, rowStart, nRow, dtype=dti, imageWidth=iwi,
                                   imageHeight=ihi, imageResize=iri, imageColor=ici))
        return tuple(rst)

    tt = _Java2NumpyCopy_.tableType(t)
    stt = str(tt)

    if stt == "NUMBER":
        return _numpySliceNumber(t, rowStart, nRow, dt)
    elif stt == "IMAGE":
        return _numpySliceImage(t, rowStart, nRow, dt, iw, ih, ir, ic)
    else:
        raise ValueError("Unsupported table type: {}".format(stt))


@_passThrough
def numpy_sample(t, nRow=None, rows=None, replace=False, dtype=np.float64, imageWidth=None, imageHeight=None, imageResize=True, imageColor=None):
    """Randomly sample rows from a a table or tables into numpy arrays.

    :param t: table or list of tables
    :param rowStart: first row of the slice
    :param dtype: numpy datatype or array of numpy datatypes to generate from the table(s)
    :param rows: row indices to produce the sample.  None randomly selects rows.
    :param imageWidth: width of the output image in pixels.
    :param imageHeight: height of the output image in pixels.
    :param imageResize: True to resize images; False otherwise.
    :param imageColor: True to return color images; False to return gray-scale images.
    :return: numpy data sampled from the input tables.  Scalar if one input table or tuple if input table list.
    """
    # this requires that one of nRow or rows are provided, but not both.
    if ((nRow is None) and (rows is None)) or ((nRow is not None) and (rows is not None)):
        raise ValueError("Please provide exactly one of nRow or rows.")

    # this requires that you not attempt to sample more rows than are available when replace is false
    if (((nRow is not None) and (nRow > t.size())) or ((rows is not None) and (len(rows) > t.size()))) and (replace is False):
        raise ValueError("Cannot select more rows than the size of table when replace is False.")

    dt = _validateSize(t, dtype, 'dtype')
    iw = _validateSize(t, imageWidth, 'imageWidth')
    ih = _validateSize(t, imageHeight, 'imageHeight')
    ir = _validateSize(t, imageResize, 'imageResize')
    ic = _validateSize(t, imageColor, 'imageColor')

    if _isSequence(t):
        rst = []
        s = None

        for tt, dti, iwi, ihi, iri, ici in zip(t, dt, iw, ih, ir, ic):
            if s is None:
                s = tt.size()
            elif s != tt.size():
                raise ValueError("Tables are different sizes ({} != {})".format(s, tt.size()))

            rst.append(numpy_sample(tt, nRow=nRow, dtype=dti, rows=rows,
                                    imageWidth=iwi, imageHeight=ihi, imageResize=iri, imageColor=ici))
        return tuple(rst)

    tt = _Java2NumpyCopy_.tableType(t)
    stt = str(tt)

    rows = _Java2NumpyCopy_.randRows(nRow, t.size(), replace) if rows == None else rows

    if stt == "NUMBER":
        return _numpyRandNumber(t, rows, dt)
    elif stt == "IMAGE":
        return _numpyRandImage(t, rows, dt, iw, ih, ir, ic)
    else:
        raise ValueError("Unsupported table type: {}".format(stt))
