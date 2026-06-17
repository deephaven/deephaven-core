addOne = { long x -> x + 1 }

t = emptyTable(10).update("X = ii")

// updateView is lazy — addOne is NOT called during this operation.
tLazy = t.updateView("Y = addOne(X)")

// select forces immediate evaluation — addOne IS called here, once per row.
tEager = t.select("X", "Y = addOne(X)")
