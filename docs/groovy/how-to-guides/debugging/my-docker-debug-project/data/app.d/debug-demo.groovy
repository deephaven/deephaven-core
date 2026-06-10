addOne = { long x ->
    println "[DEBUG] addOne called: x=$x"
    def result = x + 1
    println "[DEBUG] addOne returning: $result"
    result
}

t = emptyTable(10).update("X = ii")
tLazy = t.updateView("Y = addOne(X)")
tEager = t.select("X", "Y = addOne(X)")