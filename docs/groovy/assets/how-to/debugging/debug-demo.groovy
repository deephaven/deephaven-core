def addOne(int x) {
    return x + 1
}

t = emptyTable(10).update("X = ii")

// updateView is lazy — addOne is NOT called during this operation.
// A breakpoint inside addOne will NOT be hit.
tLazy = t.updateView("Y = addOne(X)")

// select forces immediate evaluation — addOne IS called here.
// A breakpoint inside addOne WILL be hit.
tEager = t.select("X", "Y = addOne(X)")
