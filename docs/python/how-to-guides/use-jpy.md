---
title: Use jpy
---

[`jpy`](/core/pydoc/code/jpy.html) is a bidirectional Java-Python bridge. The Deephaven query engine is implemented in Java, and [`jpy`](/core/pydoc/code/jpy.html) is the bridge that lets Deephaven's Python API call it. That makes it relatively easy to use Java from within Python.

This guide shows how to use Java from Python. You'll learn how to:

- Get Java types.
- Call their methods.
- Create Java arrays.
- Pass Deephaven tables to Java methods and wrap the tables they return.
- Use Java objects in [query strings](./query-string-overview.md).

It doesn't cover calling Python from Java, which is much less common.

For more details on [`jpy`](/core/pydoc/code/jpy.html), see the [jpy GitHub project](https://github.com/jpy-consortium/jpy), or use `help("jpy")` to see the package documentation.

> [!CAUTION]
> [`jpy`](/core/pydoc/code/jpy.html) is a low-level tool. Use it only when Deephaven has no Python wrapper for the Java objects you need. A Python wrapper is a Python module or class that calls the Java object for you.
>
> For many tasks in this guide, the [`deephaven.dtypes`](/core/pydoc/code/deephaven.dtypes.html#module-deephaven.dtypes) module is the better choice. It defines Deephaven's Java data types, including the primitives, and creates Java arrays and instances of many Java object types. This guide uses primitive types and simple objects to show how [`jpy`](/core/pydoc/code/jpy.html) works, not to recommend it for those cases.

> [!NOTE]
> To request a Python wrapper for a particular Java type or library, open an issue in the [Deephaven Core GitHub repository](https://github.com/deephaven/deephaven-core).

## Get Java types

Most tasks on this page start with a Java type, which [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) retrieves by name.

### Use `jpy.get_type`

To get a Java type from Python, pass the fully qualified class name of the desired type as a string argument to [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type). Here's a simple example using [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html).

```python test-set=1
import jpy

# use get_type to get the Python class that jpy generates for java.lang.String
_JString = jpy.get_type("java.lang.String")
```

By convention, this guide prefixes the names of Java types with `_J` (for example, `_JString`), and the names of Java tables returned by Java methods with `_j` (for example, `_j_merged_t`). [`jpy`](/core/pydoc/code/jpy.html) doesn't require either prefix.

[`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) can get any valid Java type that is present in the Java classpath, including Deephaven-specific types. To make a third-party library available, see [Install and use Java packages](./install-and-use-java-packages.md). Here is an example using Deephaven's [`DoubleVector`](/core/javadoc/io/deephaven/vector/DoubleVector.html).

```python test-set=1
# get the Deephaven representation of a vector of doubles
_JDoubleVector = jpy.get_type("io.deephaven.vector.DoubleVector")
```

### What does `get_type` return?

[`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) returns a unique Python class that wraps the underlying Java type. You can inspect this class with familiar Python functions.

```python test-set=2
import jpy

_JURL = jpy.get_type("java.net.URL")

# the class is named after the underlying Java type, so printing it shows the fully qualified Java class name
print(_JURL)
# type() shows that the wrapper is an ordinary Python class; every wrapped Java type is a subclass of jpy.JType
print(type(_JURL))
```

[`jpy`](/core/pydoc/code/jpy.html) gives this class methods that mirror those of the underlying Java type. To print the full class, including every Java method, call Python's [`help`](https://docs.python.org/3/library/functions.html#help) function: `help(_JURL)`. Its output is long, so the following example prints only the names of the Java methods:

```python test-set=2
# list the Java methods that jpy exposes on the class
java_methods = sorted(
    name
    for name in dir(_JURL)
    if not name.startswith("_") and callable(getattr(_JURL, name))
)
print(java_methods)
```

Check the [Javadoc for `java.net.URL`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/URL.html) to confirm that the [`jpy`](/core/pydoc/code/jpy.html) class includes all of its public methods. [`jpy`](/core/pydoc/code/jpy.html) doesn't expose protected or private members.

### Access nested (inner) classes

Java code refers to an inner class with dot notation: `OuterClass.InnerClass`. However, [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) expects the class's binary name, which separates the outer and inner class names with `$`. This is the same form that `Class.forName` takes. Dot notation fails.

```python should-fail
import jpy

# ❌ Dot notation does not work for inner classes
_JMapEntry = jpy.get_type("java.util.Map.Entry")
```

Use `$` instead:

```python test-set=inner-classes
import jpy

# ✅ Use $ to separate the outer and inner class names
_JMapEntry = jpy.get_type("java.util.Map$Entry")
```

The pattern is `jpy.get_type("fully.qualified.OuterClass$InnerClass")`. Each additional level of nesting adds another `$`. For example, a class nested two levels deep is `OuterClass$InnerClass$DeepInner`.

Here are examples using Deephaven types:

```python test-set=inner-classes
# Builder inner class of SyncTableFilter
_JSyncTableFilterBuilder = jpy.get_type(
    "io.deephaven.engine.table.impl.util.SyncTableFilter$Builder"
)

# Builder inner classes of LeaderTableFilter
_JLeaderTableFilterTableBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$TableBuilder"
)
_JLeaderTableFilterPartitionedTableBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$PartitionedTableBuilder"
)
```

> [!NOTE]
> To find the outer and inner class names, look in the Java source or [Javadoc](./read-javadocs.md) for the nested class or interface declared inside the outer class definition, such as `interface Entry` inside `Map`.

## Call methods on Java types

Python code can use Java methods directly.

### Call static methods

Static methods are methods that belong to the type rather than to an instance of the type. You can call a static method directly on the type that [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) returns. This example uses the [`toString`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Integer.html#toString(int)) static method from the [`java.lang.Integer`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Integer.html) type.

```python
import jpy

_JInteger = jpy.get_type("java.lang.Integer")

# toString(int) is static, so it can be called directly from the type
int_as_string = _JInteger.toString(123)

# verify that the returned object is really a string
print(int_as_string)
print(type(int_as_string))
```

### Create objects and call instance methods

To create an instance of a Java type, call the type that [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) returns like a function. This invokes one of the type's constructors. The code block below creates a [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) with its `String` constructor.

```python test-set=3
import jpy

_JBigDecimal = jpy.get_type("java.math.BigDecimal")

# calling the type invokes the BigDecimal(String) constructor
decimal_instance = _JBigDecimal("123.456")
```

Unlike static methods, which belong to the type, instance methods belong to an _instance_ and operate on the data stored in that object.

`decimal_instance` is a Java object and provides many instance methods for operating on the value `123.456`. The [Javadoc for `java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) details all of the available methods. This example uses the [`toString`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html#toString()), [`compareTo`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html#compareTo(java.math.BigDecimal)), and [`equals`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html#equals(java.lang.Object)) instance methods.

```python test-set=3
# the toString method returns the decimal represented as a string
decimal_as_string = decimal_instance.toString()
print(decimal_as_string)
print(type(decimal_as_string))

# compareTo returns a negative number if the decimal is less than the argument, 0 if equal, and a positive number if greater
less_than_or_greater_than = decimal_instance.compareTo(_JBigDecimal("654.321"))
print(less_than_or_greater_than)
print(type(less_than_or_greater_than))

# the equals method returns True if the argument is a BigDecimal with the same value and scale, and False otherwise
are_decimals_equal = decimal_instance.equals(_JBigDecimal("123.456"))
print(are_decimals_equal)
print(type(are_decimals_equal))
```

### Return type conversion

[`jpy`](/core/pydoc/code/jpy.html) converts some Java return values to Python values. It converts a value when the method declares one of the types below as its return type, or declares `java.lang.Object` and returns one of them:

- A primitive or a boxed primitive, such as `java.lang.Integer` or `java.lang.Float`, becomes the matching Python `bool`, `int`, or `float`. A `char` or `java.lang.Character` becomes an `int` holding the char's numeric (UTF-16) value, not a `str`.
- A `java.lang.String` becomes a Python `str`.
- `null` becomes `None`.

For example, `_JInteger.valueOf(4)` returns a Python `int`, not a `java.lang.Integer` that you can call instance methods on. The next example shows the same conversion for a `double`: `Math.random` returns a Java `double`, which comes back as a Python `float`.

```python
import jpy

# get the Java Math class
_JMath = jpy.get_type("java.lang.Math")

# generate random float with random method
float_return = _JMath.random()

# verify that the returned type is correct
print(type(float_return))
```

When a Java method returns an object that [`jpy`](/core/pydoc/code/jpy.html) can't convert to a Python type, [`jpy`](/core/pydoc/code/jpy.html) wraps the result in a Python object that you can use like any other.

```python
import jpy

# get Deephaven date-time utils
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

# create a Java Instant using parseInstant
java_instant = _JDateTimeUtils.parseInstant("2023-06-30T17:00:00Z")

# the return type is a Java Instant, so jpy returns it wrapped in a jpy object
print(type(java_instant))
```

### Argument type conversion

When you call a Java method with Python arguments, such as `_JInteger.toString(123)`, [`jpy`](/core/pydoc/code/jpy.html) auto-converts the Python objects to Java objects of the appropriate types. Here is another example of this conversion.

```python
import jpy

_JBigInteger = jpy.get_type("java.math.BigInteger")

# create a java.math.BigInteger with the static valueOf(long) method, called with a Python object
big_int_instance = _JBigInteger.valueOf(4)

# call the pow(int) instance method, also called with a Python object
big_int_squared = big_int_instance.pow(2)
print(big_int_squared.toString())
```

Here, `4` and `2` are Python objects, so [`jpy`](/core/pydoc/code/jpy.html) must convert them to Java values before calling methods.

[`jpy`](/core/pydoc/code/jpy.html) auto-converts these Python arguments:

- `int` and `bool` values passed to any numeric, `char`, or `boolean` primitive parameter, or to its boxed type, such as `java.lang.Integer`.
- `float` values passed to `float` or `double` parameters, or to `java.lang.Float` or `java.lang.Double` parameters. A `float` is never converted to an integer type such as `int` or `long`.
- `str` values passed to `String` parameters.
- `int`, `float`, `bool`, and `str` values passed to `Object` parameters, or to another type that the matching Java type extends or implements, such as `Number` or `CharSequence`.
- `None` passed to any object parameter, which becomes `null`.
- Python sequences passed to Java array parameters.

[`jpy`](/core/pydoc/code/jpy.html) also passes most other numeric objects, including NumPy scalars such as `np.int32` and `np.float32`, to `float` and `double` parameters. It treats them differently from Python numbers:

- `np.float64` is a subclass of Python `float`, so it converts exactly like one.
- Other NumPy scalars, including NumPy integers, never convert to integer parameters such as `int` or `long`. For `float` and `double` parameters they rank below a Python `float`, so a method that has both overloads fails as ambiguous. See [Overload resolution errors](#overload-resolution-errors).

Convert NumPy scalars with `int()` or `float()` before passing them.

[`jpy`](/core/pydoc/code/jpy.html) doesn't convert other Python objects, such as a `dict`. The only parameter type that can receive them is `org.jpy.PyObject`, which accepts any Python object. To pass one of those objects to any other parameter type, first create a Java object of the parameter's type.

Passing such an object to a `java.lang.Object` parameter raises a `ValueError` rather than one of the [overload resolution errors](#overload-resolution-errors). For a `dict`, the message is `cannot convert a Python 'dict' to a Java 'java.lang.Object'`.

The [`dayOfWeek`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfWeek(java.time.LocalDate)) method from Deephaven's [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) takes a Java [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html). [`jpy`](/core/pydoc/code/jpy.html) can't convert any Python object to a `LocalDate`, so this example builds one with [`DateTimeUtils.parseLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)).

```python
import jpy

# want to use the dayOfWeek static method from DateTimeUtils
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

# create LocalDate argument to pass to dayOfWeek method and get result
j_local_date = _JDateTimeUtils.parseLocalDate("2021-05-04")
j_day_of_week = _JDateTimeUtils.dayOfWeek(j_local_date)
print(j_day_of_week)
```

> [!NOTE]
> This example is only illustrative. In practice, create the `LocalDate` with [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) from [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time), or call [`dayOfWeek`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfWeek(java.time.LocalDate)) directly in a [query string](./query-string-overview.md).

### Method overloading

Java allows method overloading, where multiple methods have the same name but different signatures. Python does not. When you call an overloaded Java method, [`jpy`](/core/pydoc/code/jpy.html) chooses the overload whose parameter types best match your arguments.

The best match is an _exact match_, where every argument has the Python type that fits its parameter best:

- A Python `int` is an exact match for any integer or `char` parameter, such as `int` or `long`.
- A Python `float` is an exact match for a `double` parameter. It also converts to a `float` parameter, but ranks lower there.
- A Python `bool` is an exact match for a `boolean` parameter.

This example calls Java's `max` method twice, once with two Python `int` values and once with two Python `float` values:

- For `int` arguments, both [`max(int, int)`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#max(int,int)) and `max(long, long)` are exact matches. [`jpy`](/core/pydoc/code/jpy.html) calls the first one it finds, and the result is a Python `int` either way.
- For `float` arguments, [`jpy`](/core/pydoc/code/jpy.html) calls [`max(double, double)`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#max(double,double)), because `double` is the exact match for a Python `float`. The result is a Python `float`.

```python
import jpy

# get the Java math library
_JMath = jpy.get_type("java.lang.Math")

# create int and float representations of 5 and 7
x_int, y_int = int(5), int(7)
x_float, y_float = float(5), float(7)

# call overloaded 'max' method on both types
max_int = _JMath.max(x_int, y_int)
max_float = _JMath.max(x_float, y_float)

# an integer overload returns an int
print(type(max_int))
# and the double overload returns a float
print(type(max_float))
```

[`jpy`](/core/pydoc/code/jpy.html) uses the same matching when you pass a mix of Python and Java arguments. Here's an example that calls two overloads of the [`minus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,long)) method with different combinations of Python and Java arguments.

```python
import jpy

# want to use the minus method to get time differences
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

# create the initial instant and two different types representing 12 hours
initial_instant = _JDateTimeUtils.parseInstant("2024-02-01T12:30:00Z")
diff_nanos = 12 * 60 * 60 * 1_000_000_000  # 12 hours in nanoseconds as a long
diff_duration = _JDateTimeUtils.parseDuration("PT12h")

# invoke two overloads; note that the first involves a Python conversion
res_1 = _JDateTimeUtils.minus(initial_instant, diff_nanos)
res_2 = _JDateTimeUtils.minus(initial_instant, diff_duration)

# verify that results are the same
print(res_1)
print(res_2)
```

### Overload resolution errors

If [`jpy`](/core/pydoc/code/jpy.html) cannot find an overload matching the argument types provided, it raises a "no matching Java method overloads found" error:

```
Type: <class 'RuntimeError'>
Value: no matching Java method overloads found
```

This error occurs when you pass the wrong number of arguments, pass an argument of the wrong type, or pass a Python value that [`jpy`](/core/pydoc/code/jpy.html) can't auto-convert to the parameter type. The following example makes one call of each kind and prints each error.

```python
import jpy

_JMath = jpy.get_type("java.lang.Math")

failing_calls = {
    # addExact takes two arguments, but is only given one
    "too few arguments": lambda: _JMath.addExact(5),
    # hypot takes two arguments, but is given three
    "too many arguments": lambda: _JMath.hypot(3, 4, 5),
    # abs has overloads for int, long, float, and double, not string
    "wrong type": lambda: _JMath.abs("hello!"),
    # the most subtle error
    # incrementExact accepts only an int or a long, and a Python float cannot be auto-converted to either
    "no conversion": lambda: _JMath.incrementExact(5.0),
}

for label, call in failing_calls.items():
    try:
        call()
    except RuntimeError as e:
        print(label, "->", e)
```

[`jpy`](/core/pydoc/code/jpy.html) raises an "ambiguous Java method call" error if more than one overload matches the arguments equally well and none of them is an exact match:

```
Type: <class 'RuntimeError'>
Value: ambiguous Java method call, too many matching method overloads found
```

This happens when you pass NumPy scalars, such as `np.int32` values, to a method that has both `float` and `double` overloads. The following example triggers the error, then fixes it by converting the arguments to Python `int` values first.

```python
import numpy as np
import jpy

_JMath = jpy.get_type("java.lang.Math")

try:
    # np.int32 matches the float and double overloads equally, so jpy can't choose between them
    _JMath.min(np.int32(4), np.int32(5))
except RuntimeError as e:
    print(e)

# converting to Python int first selects an integer overload
print(_JMath.min(int(np.int32(4)), int(np.int32(5))))
```

## Create Java arrays

[`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) creates a Java array from either a Python sequence of values or an integer that sets the array length. You don't need it to pass a Python list to a Java array parameter, because [`jpy`](/core/pydoc/code/jpy.html) converts the list for you. Use [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) when you need the Java array as an object of its own:

- To build nested arrays.
- To control the element type exactly.
- To create the array once and reuse it.

For one-dimensional arrays of Deephaven-supported types, [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) does the same job through Deephaven's Python API, so use it instead of [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) where it fits.

### One-dimensional arrays

To create a one-dimensional array with [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array), pass in the name of the type and a Python sequence (such as a list, tuple, or NumPy array) containing elements that can be converted to the target type.

```python test-set=4
import numpy as np
import jpy

# any Python sequence can be used, like tuples...
int_array = jpy.array("int", (1, 2, 3, 4, 5))

# lists...
bool_array = jpy.array("boolean", [True, False, True, False])

# or numpy arrays
double_array = jpy.array("double", np.array([4.5, 5.6, 6.7]))

# list() copies each Java array's elements into a Python list for printing
print(list(int_array), list(bool_array), list(double_array))
```

You can also pass the number of elements instead of a sequence. This yields an array of default values: zeros for numeric primitives, `false` for `boolean`, and `null` for objects.

```python test-set=4
# creates a zero-filled byte array of 100 elements
byte_array_100_elements = jpy.array("byte", 100)

# print the array's length and its first element
print(len(byte_array_100_elements), byte_array_100_elements[0])
```

If the sequence contains elements that cannot be converted to the requested Java type, the conversion fails.

```python test-set=4 should-fail
# 3.3 cannot be converted to an int
failed_int_array = jpy.array("int", [1, 2, 3.3, 4, 5])
```

[`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) also creates arrays of non-primitive types, such as [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) and [`java.lang.Float`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Float.html). Specify the fully qualified class name:

```python test-set=5
import jpy

string_array = jpy.array("java.lang.String", ["I", "am", "a", "string!"])
float_array = jpy.array("java.lang.Float", [1.2, 2.3, 3.4, 4.5])

print(list(string_array))
# java.lang.Float holds 32-bit values, so each one prints as its nearest 32-bit approximation
print(list(float_array))
```

### Nested arrays

Multi-dimensional arrays, or nested arrays, are common in Java. To create one, nest calls to [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array). The outer call can't take an element type name such as `"int"`, which the [One-dimensional arrays](#one-dimensional-arrays) examples use. The following example tries this with a nested array of integers and fails.

```python test-set=5 should-fail
# this won't work!
nested_int_array = jpy.array(
    "int",
    [  # this is the 'outer' array
        jpy.array("int", [1, 2, 3]),  # these are the 'inner' arrays
        jpy.array("int", [4, 5, 6]),
    ],
)
```

The outer call fails because it names `int` as its type, but its elements are `int` arrays. Name each call's type after what it holds:

- The innermost calls hold plain values, so they take the element type name, such as `"int"`.
- Every other call holds arrays, so it takes the name of the array type it holds.

Write an array type name by these rules:

- For primitive types, use the [_type signature_](https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html). The type signatures for each primitive type are:

  | Primitive Type | Type Signature |
  | -------------- | -------------- |
  | `boolean`      | `Z`            |
  | `byte`         | `B`            |
  | `char`         | `C`            |
  | `short`        | `S`            |
  | `int`          | `I`            |
  | `long`         | `J`            |
  | `float`        | `F`            |
  | `double`       | `D`            |

- For object types, use the fully qualified class name, prepended with `L` and ending with `;`. Write the class name with dots, as in `Ljava.lang.String;`.
- For an `n`-dimensional array, prepend `n-1` open square brackets.

To clarify these rules, here are some concrete examples.

#### Example 1: 2D `boolean` array

This example creates a 2D array of boolean values. Because the elements are primitives, use the type signature `Z` for `boolean`. Because the array is 2D, prepend one open square bracket: `[Z`.

```python test-set=5
bool_array_2d = jpy.array(
    "[Z",
    [
        jpy.array("boolean", [True, False, True, False]),
        jpy.array("boolean", [False, True, False]),
    ],
)

# print the number of inner arrays and the contents of the second one
print(len(bool_array_2d), list(bool_array_2d[1]))
```

#### Example 2: 3D `int` array

This example creates a 3D array of integer values. Because `int` is primitive, use the type signature `I`. Because the array is 3D, prepend two open square brackets: `[[I`.

```python test-set=5
int_array_3d = jpy.array(
    "[[I",
    [
        jpy.array(
            "[I",
            [
                jpy.array("int", [1, 2, 3, 4, 5]),
                jpy.array("int", [6, 7, 8]),
            ],
        ),
        jpy.array(
            "[I",
            [
                jpy.array("int", [9, 10, 11, 12]),
                jpy.array("int", [13, 14]),
            ],
        ),
    ],
)

# java.util.Arrays.deepToString prints the contents of a nested array
_JArrays = jpy.get_type("java.util.Arrays")
print(_JArrays.deepToString(int_array_3d))
```

#### Example 3: 2D `java.lang.String` array

This example creates a 2D array of `String` values. Because the elements are objects, prepend `L` to the class name and append `;`. Because the array is 2D, prepend one open square bracket.

```python test-set=5
string_array_nested = jpy.array(
    "[Ljava.lang.String;",
    [
        jpy.array("java.lang.String", ["I", "am", "a", "string!"]),
        jpy.array("java.lang.String", ["I", "am", "also", "a", "string!"]),
    ],
)

print(_JArrays.deepToString(string_array_nested))
```

#### Example 4: 3D `java.lang.String` array

Finally, this example creates a 3D array of `String` values. This needs `L` and `;`, plus two open square brackets.

```python test-set=5
string_array_nested_deeper = jpy.array(
    "[[Ljava.lang.String;",
    [
        jpy.array(
            "[Ljava.lang.String;",
            [
                jpy.array("java.lang.String", ["I", "am", "a", "string!"]),
                jpy.array("java.lang.String", ["I", "am", "also", "a", "string!"]),
            ],
        ),
        jpy.array(
            "[Ljava.lang.String;",
            [
                jpy.array("java.lang.String", ["I", "am", "a", "string", "too!"]),
                jpy.array(
                    "java.lang.String", ["I", "guess", "I", "am", "a", "string!"]
                ),
            ],
        ),
    ],
)

print(_JArrays.deepToString(string_array_nested_deeper))
```

## Use `jpy` with Deephaven

Because the Deephaven query engine is implemented in Java, Deephaven makes it easy to use Java objects directly. Deephaven's Python objects expose the Java objects they wrap, so you can pass Deephaven tables to Java methods you call through [`jpy`](/core/pydoc/code/jpy.html). In the other direction, Java objects you create with [`jpy`](/core/pydoc/code/jpy.html) can be used directly in [query strings](./query-string-overview.md).

### The `j_object` property

To access the Java objects underlying Deephaven's Python API, use the `j_object` property.

```python order=t,t_j_object
from deephaven import empty_table

# create a new table
t = empty_table(10).update("X = ii")

# get the underlying Java object with j_object
t_j_object = t.j_object
```

### Use Java libraries with Deephaven tables

Many use cases for [`jpy`](/core/pydoc/code/jpy.html) involve operating on Deephaven tables with Java libraries or methods that have not been wrapped in Python. Use [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) to get the desired class. Then, pass Deephaven tables to Java methods with the `j_object` property.

```python test-set=6 order=:log,t
from deephaven import empty_table
import jpy

t = empty_table(5).update(["X1 = i", "X2 = X1 + 2", "X3 = X1 + 3", "X4 = X1 + 4"])

# get the TableTools type
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")

# use the show method to print the first rows of the table; note the j_object in the argument
_JTableTools.show(t.j_object)
```

Java methods in Deephaven libraries often return new tables as Java objects. To use one with the rest of the Python API, wrap it with the [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) constructor from [`deephaven.table`](/core/pydoc/code/deephaven.table.html#module-deephaven.table).

```python test-set=6 order=new_t,merged_t
from deephaven.table import Table

new_t = empty_table(3).update(["X1 = 10", "X2 = 20", "X3 = 30", "X4 = 40"])

# merge tables with TableTools.merge; this is illustrative, because Deephaven's Python API already wraps merge
# _j_merged_t is a Java Deephaven table
_j_merged_t = _JTableTools.merge(t.j_object, new_t.j_object)

# wrap the result in a Python Deephaven table
merged_t = Table(_j_merged_t)
```

### Use Java values in query strings

Java objects created with [`jpy`](/core/pydoc/code/jpy.html) can be used directly in Deephaven's [query strings](./query-string-overview.md). For more on using Java objects and their methods in query strings, see [Java objects and classes in query strings](./java-classes.md).

```python order=t_meta,t
import jpy
from deephaven import empty_table

# create a string
_JString = jpy.get_type("java.lang.String")
string_instance = _JString("I am a string!")

t = empty_table(10).update("StringCol = string_instance")

# check the metadata of the table, confirming that the column is understood as a string
t_meta = t.meta_table
```

You can call a Java object's methods either outside a [query string](./query-string-overview.md) or inside one.

```python order=outside_calls,inside_calls
import jpy
from deephaven import empty_table

# get the URI type and use it to create a URL instance
_JURI = jpy.get_type("java.net.URI")
url_instance = _JURI.create("https://deephaven.io").toURL()

# call the methods outside of the query strings
protocol = url_instance.getProtocol()
host = url_instance.getHost()
port = url_instance.getDefaultPort()
uri = url_instance.toURI()

outside_calls = empty_table(1).update(
    [
        "URL = url_instance",
        "Protocol = protocol",
        "Host = host",
        "Port = port",
        "URI = uri",
    ]
)

# alternatively, call the methods inside of the query strings
inside_calls = empty_table(1).update(
    [
        "URL = url_instance",
        "Protocol = URL.getProtocol()",
        "Host = URL.getHost()",
        "Port = URL.getDefaultPort()",
        "URI = URL.toURI()",
    ]
)
```

### Compare Java and Python performance in query strings

Java values in [query strings](./query-string-overview.md) don't cross the [Python-Java boundary](../conceptual/python-java-boundary.md) when the query runs. This makes them faster than patterns that do cross it, such as calling a [Python function](./python-functions.md) from a query string. Here is an example that compares the efficiency of generating random numbers using a Python or a Java approach.

```python order=:log,python_gauss,java_gauss
import time
from deephaven import empty_table

### Python approach
import random

random.seed(12345)


def generate_gaussian() -> float:
    return random.gauss(0.0, 1.0)


python_start = time.time()
python_gauss = empty_table(10_000_000).update("X = generate_gaussian()")
python_end = time.time()

### Java approach
import jpy

_JRandom = jpy.get_type("java.util.Random")
rng_instance = _JRandom(12345)

java_start = time.time()
java_gauss = empty_table(10_000_000).update("X = rng_instance.nextGaussian()")
java_end = time.time()

### evaluate time difference
print("Python execution time: ", python_end - python_start)
print("Java execution time: ", java_end - java_start)
```

The Java version finishes faster because each row's `nextGaussian` call stays in Java, while the Python version runs Python code for every row.

## Tie it all together

This example uses [`ColumnsToRowsTransform`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnsToRowsTransform.html) to turn a table's value columns into labeled rows. It combines these pieces from earlier sections:

- [`jpy.get_type`](/core/pydoc/code/jpy.html#jpy.get_type) to get the Java class.
- A call to the static [`columnsToRows`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnsToRowsTransform.html#columnsToRows(io.deephaven.engine.table.Table,java.lang.String,java.lang.String,java.lang.String%5B%5D,java.lang.String%5B%5D)) method.
- [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) to build the `String[]` arguments.
- The `j_object` property to pass the table to Java.
- The [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) constructor to wrap the Java table that comes back.

```python order=transformed_t,t
import jpy
from deephaven import empty_table
from deephaven.table import Table

# create table where column names are 'labels', and entries are 'values'
t = empty_table(3).update(["Value1 = ii", "Value2 = ii + 3"])

# get the ColumnsToRowsTransform to turn this table's value columns into rows
_JColumnsToRowsTransform = jpy.get_type(
    "io.deephaven.engine.table.impl.util.ColumnsToRowsTransform"
)

# call the static columnsToRows method
_j_transformed_t = _JColumnsToRowsTransform.columnsToRows(
    # must use j_object to get the underlying Java object, otherwise method will not be found
    t.j_object,
    # labelColumn and valueColumn are String arguments, which jpy knows how to convert
    "LabelCol",
    "ValueCol",
    # labels and transposeColumns are String[] arguments
    # Python lists of str would also convert, but jpy.array creates the String[] arrays explicitly
    jpy.array("java.lang.String", ["Value1", "Value2"]),
    jpy.array("java.lang.String", ["Value1", "Value2"]),
)

# wrap the Java table in a Python Table, so that the Deephaven Python API can use it
transformed_t = Table(_j_transformed_t)
```

## Related documentation

- [Python-Java boundary](../conceptual/python-java-boundary.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Javadocs for Python users](./read-javadocs.md)
- [Python functions in query strings](./python-functions.md)
- [Query string overview](./query-string-overview.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [jpy GitHub](https://github.com/jpy-consortium/jpy)
- [jpy documentation](https://jpy.readthedocs.io/en/latest/)
- [Type signatures](https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html)
- [`jpy` Pydoc](/core/pydoc/code/jpy.html#module-jpy)
- [`deephaven.dtypes` Pydoc](/core/pydoc/code/deephaven.dtypes.html#module-deephaven.dtypes)
- [`deephaven.time` Pydoc](/core/pydoc/code/deephaven.time.html#module-deephaven.time)
- [`deephaven.table.Table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
