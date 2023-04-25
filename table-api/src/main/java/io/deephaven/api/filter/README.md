# Filter

Deephaven filters can be constructed via where filter expression parsing using strings or directly via the
`io.deephaven.api.filter.Filter` APIs / `deephaven.filters` python package[^1].

## Logic

One of the main goals in using filter APIs is to remain consistent with what you might expect of boolean algebra:

```python
from deephaven.filters import not_, or_

# any Table
t = ...

# any API filter
f = ...

# f ∧ ¬f = 0
# no rows match
no_rows = t.where([f, not_(f)])

# f ∨ ¬f = 1
# all rows match
all_rows = t.where(or_([f, not_(f)]))
```

That is, `not_(f)` matches all rows that `f` does _not_ match.

## Safety

`io.deephaven.api.filter.Filter`s are safe-by-default whereas filter expression strings may not be safe:

```python
from deephaven.filters import pattern, PatternMode

# Table with string column Foo
t = ...

# t1 is safe, will exclude rows where Foo == null
t1 = t.where(pattern(PatternMode.MATCHES, "Foo", "a.*z"))

# t2 is safe due to explicit nullness checking
t2 = t.where("!isNull(Foo) && Foo.matches(`a.*z`)")

# t3 is unsafe, will throw null pointer exception if Foo == null during evaluation
t3 = t.where("Foo.matches(`a.*z`)")
```

## Filter flags

`io.deephaven.api.filter.Filter`s may have flag(s) to express inverting their "post-null-check logic", and this is _not_
the same as using `not_`.

Here is an illustration of the four distinct cases concerning nullability with `pattern` and the `invert_pattern` flag:

```python
from deephaven.filters import not_, pattern, PatternMode

# Table with string column Foo
t = ...

# Foo != null && Foo.matches(...)
include_match_exclude_null = t.where(pattern(...))

# Foo == null || Foo.matches(...)
include_match_include_null = t.where(not_(pattern(..., invert_pattern=True)))
# include_match_include_null = t.where(or_([is_null("Foo"), pattern(...)]))

# Foo != null && !Foo.matches(...)
exclude_match_exclude_null = t.where(pattern(..., invert_pattern=True))

# Foo == null || !Foo.matches(...)
exclude_match_include_null = t.where(not_(pattern(...)))
# exclude_match_include_null = t.where(or_([is_null("Foo"), pattern(..., invert_pattern=True)]))
```

[^1]: Advanced users may also choose to build their own filter logic using the engine API `io.deephaven.engine.table.impl.select.WhereFilter`.