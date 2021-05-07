# Inspired by the extremely useful read @ stackoverflow:
# https://stackoverflow.com/questions/52046102/numba-and-guvectorize-for-cuda-target-code-running-slower-than-expected
# I cranked the numbers up a little and then played w/ the various settings

import numpy as np
from numba import guvectorize, vectorize
from timeit import default_timer as timer

g = np.int32(100)


@vectorize(['int32(int32,int32,int32)'])
def test(a, b, c):
    return a + b + c


into = np.empty(100, dtype=np.int32)
v = test(
    np.arange(100, dtype=np.int32),
    np.arange(100, dtype=np.int32),
    g,
    out=into
)
print(v)

@guvectorize(['void(float64[:,:], float64[:,:], int64, int64, float64[:,:])'], '(m,o),(m,o),(),() -> (m,o)', nopython=True, target='cpu')
def cVestDiscount (multBy, discount, n, countCol, cv):
        for ID in range(0,countCol):
            for num in range(0,n):
                cv[ID][num] = multBy[ID][num] * discount[ID][num]

multBy = np.float64(np.arange(2000000).reshape(40000,50))
discount = np.float64(np.arange(200000000).reshape(100,40000,50))
n = np.int64(5)
countCol = np.int64(40000)
cv = np.zeros(shape=(100,40000,50), dtype=np.float64)
func_start = timer()
cv = cVestDiscount(multBy, discount, n, countCol, cv)
timing=timer()-func_start
print("Function: discount factor cumVest duration (seconds):" + str(timing))