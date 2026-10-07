---
title: Formula validation configuration
---

When you work with Deephaven tables, you often create custom columns or filter data using expressions like:

- `myTable.update(myColumn = x + y * 2)`
- `myTable.where("Double.isFinite(Price)")`

These expressions get sent to the Deephaven server over gRPC and compiled into executable code. For security reasons, the server needs to validate that these expressions only call approved methods and functions. Script session code execution is not validated as the Deephaven engine does not parse script code. The console service can be disabled via [configuration](../how-to-guides/configuration/console-service.md#configuration).

This guide explains how to configure formula validation using two main approaches:

- Annotations (for your own code)
- Pointcut expressions (for external libraries)

## When does validation occur?

Formula validation happens when requests are sent over gRPC, such as those made by any of Deephaven's client APIs:

- Create calculated columns using [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), [`view`](../reference/table-operations/select/view.md), [`updateView`](../reference/table-operations/select/update-view.md), or [`lazyUpdate`](../reference/table-operations/select/lazy-update.md).
- Filter tables using [`where`](../reference/table-operations/filter/where.md) or [`whereIn`](../reference/table-operations/filter/where-in.md).
- Perform aggregations with formula-based operations like [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) or [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md).
- Use formula-based join conditions in operations like [`join`](../reference/table-operations/join/join.md), [`naturalJoin`](../reference/table-operations/join/natural-join.md), or [`rangeJoin`](../reference/table-operations/join/rangeJoin.md).
- Apply conditional logic in update operations like [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) with custom formulas.

Validation does **not** apply to:

- Code you write directly in the console/script editor.
- Python functions or Groovy closures in your session scope (these are blocked when validation is enabled).

## How validation works

When you write a formula, Deephaven:

1. Parses your expression to find all method calls.
2. Checks each method call against the configured rules.
3. Either permits or denies the expression based on those rules.

## Method 1: Annotations (for your own code)

Use this approach when you're writing your own Java classes and want to make specific methods available in Deephaven formulas.

Annotations are special markers you add to your Java code to indicate which methods should be allowed in formulas. The key annotation is [`@UserInvocationPermitted`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html).

### How annotations work

1. **Add the annotation** to your class or individual methods.
2. **Give it a name** using the `value` parameter (like a category name).
3. **Configure Deephaven** to recognize that category name.
4. **Optionally specify scope** to limit to static or instance methods only.

### Annotate entire classes

When you annotate a class, **all public methods** in that class become available:

```java
// This makes ALL public methods in this class available to formulas
@UserInvocationPermitted(value = "my_math_utils")
public class MathUtils {
    public static double square(double x) { return x * x; }
    public static double cube(double x) { return x * x * x; }
    // Both methods above are now available in formulas
}
```

### Limit annotations to static or instance methods

You can restrict which types of methods are allowed:

```java syntax
// Only STATIC methods are allowed from this class
@UserInvocationPermitted(value = "static_math_methods", classScope = UserInvocationPermitted.ScopeType.Static)
public class MathOperations {
    public String instanceMethod() {
        return "not allowed";  // This won't be available in formulas
    }

    public static double add(double a, double b) {
        return a + b;  // This WILL be available in formulas
    }
}

// Only INSTANCE methods are allowed from this class
@UserInvocationPermitted(value = "string_helpers", classScope = UserInvocationPermitted.ScopeType.Instance)
public class StringHelper {
    public String format(String input) {
        return input.toUpperCase();  // This WILL be available in formulas
    }

    public static String staticHelper() {
        return "not allowed";  // This won't be available in formulas
    }
}
```

### Annotate individual methods

For more precise control, annotate specific methods instead of entire classes:

```java syntax
public class MixedUtilities {
    @UserInvocationPermitted(value = "safe_math")
    public static double safeDivide(double a, double b) {
        return b != 0 ? a / b : 0;  // Available in formulas
    }

    @UserInvocationPermitted(value = "text_processing")
    public String cleanText(String input) {
        return input.trim().toLowerCase();  // Available in formulas
    }

    public void dangerousOperation() {
        // No annotation = NOT available in formulas
    }
}
```

### Best practices for annotations

When designing your annotations, follow these recommendations:

- **Use descriptive annotation values** that clearly indicate the method category (e.g., `math_operations`, `string_utilities`).
- **Group logically related methods** together rather than creating separate annotations for each method.
- **Avoid creating too many small annotation categories**, as this increases configuration complexity.
- **Consider the security implications** - only annotate methods that are safe for user formulas.
- **Group related methods under the same annotation value** to reduce configuration.

### Configuration

After adding annotations to your code, you must tell Deephaven to recognize them by setting configuration properties.

**Default configuration:**

Deephaven comes with pre-configured annotations for built-in functionality:

```properties
ColumnExpressionValidator.annotationSets.default=base,vector,function_library
```

**Adding your own annotations:**

- The property name format is: `ColumnExpressionValidator.annotationSets.<your_name>`.
- List all your annotation values separated by commas.

```properties
# Enable the annotation categories you defined
ColumnExpressionValidator.annotationSets.myapp=static_math_methods,string_helpers,safe_math,text_processing
```

## Method 2: Pointcut expressions (for external libraries)

Use this approach when you want to allow methods from libraries you don't control (like Java's built-in classes, third-party libraries, etc.).

Since you cannot add annotations to external code, you use pointcut expressions — patterns that match method signatures. The pattern syntax is adapted from AspectJ method patterns, and is a simple way to match one or more Java methods.

### Understanding pointcut patterns

A pointcut expression has three parts. Separate the class name from the method name with a space or `#` (`java.lang.String length()` and `java.lang.String#length()` are the same).

1. **Class name**: The full package and class name (e.g., `java.util.Collections`). A class in `java.lang` may omit its package (`String` is the same as `java.lang.String`). Write a nested class as `java.util.Map.Entry` or `java.util.Map$Entry`, and use `*..*` for every class. Only a name without dots or wildcards is taken to be in `java.lang`, so a nested `java.lang` class without its package must use `$`: `Thread$State` works, but `Thread.State` is taken as a fully qualified name and does not match `java.lang.Thread.State` (write that instead).
2. **Method name**: The specific method, a name with `*` wildcards such as `to*Case`, `<constructor>` for a constructor, or `*` for any method or constructor.
3. **Parameters**: The parameter types or `(..)` for any parameters. Write primitive types by their keyword (`int`, `double`). Other types must be fully qualified, except for types in `java.lang` such as `String`. Write an array parameter as `T[]` in any position, such as `java.util.Arrays fill(int[], int)`. The last parameter may instead be written `T...`, which is the same as `T[]`; `...` is rejected anywhere else.

### Pattern matching symbols

- `*` = A wildcard within a name. In a method name, or in one segment of a class or parameter type name, it matches any run of characters but never crosses a `.`: `to*Case` matches `toUpperCase` and `toLowerCase`, and `java.lang.*` matches every class in `java.lang`, but not arrays of them, which `java.lang.*[]` matches. In a class name, a segment also matches the binary name of a nested class, so `java.util.*` also matches `java.util.Map$Entry`. A parameter of only `*` matches any one parameter type, including primitive and array types, while a parameter of `*..*` matches any one type that is not an array (write `*..*[]` for a one-dimensional array), and a method name of only `*` matches constructors as well as methods.
- `..` = In a parameter list, match any number of parameters of any type; a parameter list may contain at most one `..`. In a class name, match any number of package or enclosing class names — `java.util..*` matches every class in `java.util` and its sub-packages, and `java.util..Entry` matches `java.util.Map.Entry`, while `java.util.*` does not include sub-packages.
- `;` = Separate multiple patterns in one property.

### Inherited methods

A pattern also matches every instance method that overrides a matching method. For example, `java.lang.Object toString()` permits `toString` on any class, and `java.lang.Number intValue()` permits `intValue` on `Integer`, `BigDecimal`, and every other subclass of `Number`. Static methods match only the class that declares them.

A class pattern matches only the methods and constructors that the class itself declares, plus overrides of those methods in subclasses. It does not match methods the class inherits without overriding: `java.lang.String *(..)` does not permit `getClass` on a `String`, because `Object` declares it and `String` does not override it.

### Common examples

```properties
# Allow any method or constructor declared by String, with any parameters
ColumnExpressionValidator.allowedMethods.strings=java.lang.String *(..)

# Allow only the length() method on String (no parameters)
ColumnExpressionValidator.allowedMethods.string_length=java.lang.String length()

# Allow specific methods on Integer class
ColumnExpressionValidator.allowedMethods.integers=java.lang.Integer valueOf(int);java.lang.Integer parseInt(java.lang.String)

# Allow the methods and constructors declared by several number classes, and overrides of those methods
ColumnExpressionValidator.allowedMethods.numbers=java.lang.Integer *(..);java.lang.Double *(..);java.math.BigDecimal *(..)
```

### Default Deephaven pointcut configuration

Deephaven comes pre-configured with safe methods from common Java classes:

```properties
# Methods and constructors declared by the primitive wrapper classes (Integer, Double, etc.)
ColumnExpressionValidator.allowedMethods.primitives=java.lang.Character *(..);java.lang.Byte *(..);java.lang.Short *(..);java.lang.Integer *(..);java.lang.Long *(..);java.lang.Float *(..);java.lang.Double *(..);java.lang.Boolean *(..)

# Methods and constructors declared by String (safe because strings are immutable)
ColumnExpressionValidator.allowedMethods.basic=java.lang.String *(..)

# Methods and constructors declared by the number and math classes, and overrides of those methods in subclasses
ColumnExpressionValidator.allowedMethods.numbers=java.math.BigInteger *(..);java.math.BigDecimal *(..);java.lang.Number *(..)

# Methods and constructors declared by the date and time classes
ColumnExpressionValidator.allowedMethods.time=java.time.Instant *(..);java.time.LocalTime *(..);java.time.LocalDate *(..);java.time.ZonedDateTime *(..)

# Common Object methods that are permitted
ColumnExpressionValidator.allowedMethods.toString=java.lang.Object toString()
ColumnExpressionValidator.allowedMethods.equals=java.lang.Object equals(java.lang.Object)
ColumnExpressionValidator.allowedMethods.hashCode=java.lang.Object hashCode()
```

## Backwards compatibility (Deephaven 0.39.x and earlier)

**For older versions only**: Prior to Deephaven 0.40.0, validation used a simple method name allowlist instead of the sophisticated system described above.

To revert to the old behavior (not recommended for new deployments):

```properties
ColumnExpressionValidator=method_name
```

> [!NOTE]
> The old system was less secure and flexible. The new annotation and pointcut system provides better security and control.

## Related documentation

- [Console service configuration](../how-to-guides/configuration/console-service.md#configuration)
- [How to select, view, and update data](../how-to-guides/use-select-view-update.md)
- [Formulas](../how-to-guides/formulas.md)
- [Java classes & objects](../how-to-guides/java-classes.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html)
