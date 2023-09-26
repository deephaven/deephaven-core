# Filter

Deephaven filters can be constructed via expression parsing using strings or directly via the
`io.deephaven.api.filter.Filter` APIs / `deephaven.filters` python package[^1].

## Logic

One of the main goals in using filter APIs is to remain consistent with what you might expect of boolean algebra:

```python
from deephaven.filters import not_, or_

# any Table
t = ...

# any filter API
f = ...

# f ∧ ¬f = 0
# no rows match
no_rows = t.where([f, not_(f)])

# f ∨ ¬f = 1
# all rows match
all_rows = t.where(or_([f, not_(f)]))
```

That is, `not_(f)` (the "inverse" of `f`) matches the rows that `f` does _not_ match and does _not_ match the rows that
`f` does match. This also implies that for any (properly typed) input `x`, `x` is in exactly one of `f` or `not_(f)`.


## Well-defined

`io.deephaven.api.filter.Filter`s are well-defined by-design.

This means that the types of column that the filter works with is defined, and that all possible combinations of inputs
result in a well-defined output - either it is matched, or not. This is in contrast with filter expression strings which
may have input cases that result in exceptions:

```python
from deephaven.filters import pattern, PatternMode

# Table with string column Foo
t = ...

# t1 is well-defined by-design, will exclude rows where Foo == null
t1 = t.where(pattern(PatternMode.MATCHES, "Foo", "a.*z"))

# t2 is well-defined due to explicit nullness checking
t2 = t.where("!isNull(Foo) && Foo.matches(`a.*z`)")

# t3 is not well-defined, and will throw a null pointer exception if Foo == null during evaluation
t3 = t.where("Foo.matches(`a.*z`)")
```

Well-defined may encompass more thorough cases than simply defining behaviour around nulls. Consider the theoretical
filter `is_positive` that operates on a string column, roughly defined as an equivalent to
`Double.parseDouble(Foo) > 0.0`:

```python
from deephaven.filters import is_positive

# Table with string column Foo
t = ...
t1 = t.where(is_positive("Foo"))
```

In this case, we would likely have the well-defined `is_positive` behavior documented as null values and strings not
parseable as doubles are excluded from matching.

Even though a filter may be well-defined, that doesn't mean it's applicable against all tables. For example, the
filter `is_null("Foo")` is only applicable for tables that have a column named "Foo"; `pattern(MATCHES, "Foo", "a.*z")`
is only applicable to tables that have a CharSequence compatible column named "Foo". When a filter is applied to a table
for which it is not applicable, the user should expect to get back an appropriate error from the engine.

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

# Foo != null && !Foo.matches(...)
exclude_match_exclude_null = t.where(pattern(..., invert_pattern=True))

# Foo == null || !Foo.matches(...)
exclude_match_include_null = t.where(not_(pattern(...)))
```

## Comparison operators

Given all of the above, there is a design choice we have to make regarding comparison filters - does the inverse of a
comparison filter result in another comparison filter?

The reasons why we might want a comparison filter to have an inverse that is another comparison filter is because that
is likely what most users expect. That is, `not_(Foo > Bar)` is equivalent to `Foo <= Bar`; `not_(Foo == Bar)`
is equivalent to `Foo != Bar`. This also implies there is a total-ordering among inputs.

The reasons why we might _not_ want a comparison filter to have an inverse that is another comparison filter is that
there isn't a natural total-ordering among floating point values.[^2]  It would also give us the opportunity to filter
out special values by default. For example, `Foo > 42` and `Foo <= 42` could both be defined to filter out nulls (in the
case of integral values), and to filter out nulls and NaNs (in the case of floating-point values).

Neither of the above design choices would exclude a user from composing their own more specific filters to achieve the
behavior they desire.

To most closely match the behavior in others Deephaven contexts (namely, `sort()`), comparison filters have been
defined with "the inverse of a comparison filter results in another comparison filter", thus implying a total-ordering
among inputs. The Deephaven total-ordering defines the null value as coming before all other values. In the case of
floating point values, the NaN value comes after all other values.

The one caveat to total-ordering is that Deephaven does not differentiate between NaN representations, nor between
-0.0 and 0.0.

[^1]: Advanced users may also choose to build their own filter logic using the engine implementation API `io.deephaven.engine.table.impl.select.WhereFilter`.
[^2]: It appears there may be _some_ standard, but it's behind a paywall: https://en.wikipedia.org/wiki/IEEE_754#Total-ordering_predicate