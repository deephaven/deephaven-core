# If you want to try out running code on cuda,
# you must first run `conda install cudatoolkit=9.0`
# and make sure intellij is setup correctly (use your local conda install for environment).
# We will use https://github.com/JetBrains/gradle-python-envs/ later to create the correct environment for you.

from numba import guvectorize, vectorize
from numpy import arange, empty, int32, int64, multiply, array_equal


# vectorize can be cuda or nopython=True, but it cannot be both
@vectorize(['int32(int32,int32)', 'int64(int64,int64)'], target='cuda')
def add_vectorize_32_64_cuda(a, b):
    return a + b


# guvectorize can be both cuda and nopython=True
@guvectorize(['void(int32[:],int32[:],int32[:])', 'void(int64[:],int64[:],int64[:])'], '(n),(n)->(n)', target='cuda', nopython=True)
def add_guvectorize_32_64_cuda(a, b, o):
    for i in range(0, o.size):
        o[i] = a[i] + b[i]


a32 = arange(0, 100000, dtype=int32)
b32 = arange(0, 100000, dtype=int32)
o32 = empty(100000, dtype=int32)

a64 = arange(0, 100000, dtype=int64)
b64 = arange(0, 100000, dtype=int64)
o64 = empty(100000, dtype=int64)

res = add_guvectorize_32_64_cuda(a64, b64, o64)
assert array_equal(o64, multiply(2, a64))
