---
title: Formula validation configuration
---

When you work with Deephaven tables, you often create custom columns or filter data using [formula](../how-to-guides/formulas.md) expressions like:

- `myTable.update("MyColumn = X + Y * 2")`
- `myTable.where("Double.isFinite(Price)")`

Client APIs, such as the [Python client (`pydeephaven`)](/core/client-api/python/), the [Java client](../how-to-guides/java-client.md), and the [JS API](../how-to-guides/use-jsapi.md), send these expressions to the server over gRPC. The server compiles them into executable code. For security reasons, the server validates that these expressions call only approved methods and constructors.

This guide explains how to configure formula validation using two main approaches:

- Annotations, for methods in your own Java code.
- Pointcut expressions, for methods in libraries you don't control. A pointcut expression is a pattern that matches method signatures.

## When does validation occur?

Formula validation happens when any Deephaven client API sends a gRPC request to:

- Create calculated columns using [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), [`view`](../reference/table-operations/select/view.md), [`updateView`](../reference/table-operations/select/update-view.md), or [`lazyUpdate`](../reference/table-operations/select/lazy-update.md).
- Filter tables using [`where`](../reference/table-operations/filter/where.md) or structured filter conditions, such as the filters the [JS API](../how-to-guides/use-jsapi.md) builds.
- Perform [aggregations](./aggregations.md) with formula-based operations like [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) or [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md).
- Use formula-based [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) operations: [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md), [`CumCountWhere`](../reference/table-operations/update-by-operations/cum-count-where.md), and [`RollingCountWhere`](../reference/table-operations/update-by-operations/rolling-count-where.md).
- Create [rollup tables](../how-to-guides/rollup-table.md) with formula-based [aggregations](./aggregations.md).
- Filter [rollup tables](../how-to-guides/rollup-table.md) or [tree tables](../how-to-guides/tree-table.md).
- Add or format columns in [rollup tables](../how-to-guides/rollup-table.md).
- Group by column expressions in [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md) or [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md).

Validation does **not** apply to code you run in the console or script editor. To prevent that code from running, disable the [console service](../how-to-guides/configuration/console-service.md#configuration).

## How validation works

When a client sends a formula over gRPC, the server:

1. Parses your expression to find all method and constructor calls.
2. Checks each call against two rule sets that you enable in configuration:
   - [`@UserInvocationPermitted`](/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html) annotations on the called code. Each annotation lists one or more _annotation values_, which are names for groups of methods. The annotation counts only if at least one of its values is enabled.
   - Pointcut patterns that match method signatures.
3. Permits a call if either rule set allows it, and rejects the expression if any call is not permitted.

The validator always rejects a formula sent over gRPC that calls a Python function or Groovy closure from the [query scope](../how-to-guides/query-scope.md) by name, such as `myFunc(X)`. No annotation or pointcut setting permits these calls.

You enable annotation values and pointcut patterns with `ColumnExpressionValidator` properties, in a [configuration file](../how-to-guides/configuration/config-file.md) or as JVM flags. See [Configuration properties](../how-to-guides/configuration/configuration-properties.md) for how to set properties in each deployment.

## Permit your own code with annotations

Use this approach when you're writing your own Java classes and want to make specific methods available in Deephaven formulas.

An annotation is a marker in your Java code that identifies a method formulas may call. Deephaven checks for the [`@UserInvocationPermitted`](/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html) annotation.

### Steps to permit a method with an annotation

1. **Add the annotation** to a class or to individual methods and constructors.
2. **Choose an annotation value** with the `value` parameter. The value names a group of methods that you enable together.
3. **Optionally limit a class annotation** to static or instance methods with `classScope`.
4. **Deploy the class.** Put the compiled class on the server's classpath, as described in [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md). Formulas refer to the class by its fully qualified name, as described in [Java objects and classes in query strings](../how-to-guides/java-classes.md).
5. **Configure Deephaven** to recognize that annotation value. See [Enable annotation values](#enable-annotation-values).

### Annotate entire classes

When you annotate a class, **all public methods** that the class declares become available. The annotation is inherited, so methods declared in its subclasses become available too. When you annotate an interface, the instance methods that implement that interface's methods become available.

```java syntax
package com.example.formulas;

import io.deephaven.util.annotations.UserInvocationPermitted;

// This makes ALL public methods declared in this class available to formulas
@UserInvocationPermitted(value = "my_math_utils")
public class MathUtils {
    public static double square(double x) { return x * x; }
    public static double cube(double x) { return x * x * x; }
    // Both methods are available once my_math_utils is enabled
}
```

A class annotation doesn't cover constructors. Annotate each constructor that formulas may call, as described in [Annotate individual methods](#annotate-individual-methods).

### Annotate individual methods

For more precise control, annotate specific methods or constructors instead of entire classes:

```java syntax
package com.example.formulas;

import io.deephaven.util.annotations.UserInvocationPermitted;

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
        // No annotation, so not available in formulas
    }
}
```

### Limit annotations to static or instance methods

Set the `classScope` parameter to permit only the static methods or only the instance methods of an annotated class. This class permits only its static methods:

```java syntax
package com.example.formulas;

import io.deephaven.util.annotations.UserInvocationPermitted;

// Only STATIC methods are allowed from this class
@UserInvocationPermitted(value = "static_math_methods", classScope = UserInvocationPermitted.ScopeType.Static)
public class MathOperations {
    public String instanceMethod() {
        return "not allowed";  // Not available in formulas
    }

    public static double add(double a, double b) {
        return a + b;  // Available in formulas
    }
}
```

This class permits only its instance methods:

```java syntax
package com.example.formulas;

import io.deephaven.util.annotations.UserInvocationPermitted;

// Only INSTANCE methods are allowed from this class
@UserInvocationPermitted(value = "string_helpers", classScope = UserInvocationPermitted.ScopeType.Instance)
public class StringHelper {
    public String format(String input) {
        return input.toUpperCase();  // Available in formulas
    }

    public static String staticHelper() {
        return "not allowed";  // Not available in formulas
    }
}
```

### Enable annotation values

After adding annotations to your code, you must tell Deephaven to recognize them by setting configuration properties.

A property in the form `ColumnExpressionValidator.annotationSets.<label>` enables annotation values:

- `<label>` is a name you choose. It doesn't change which methods are permitted.
- The value is a comma-separated list of annotation values.

Formulas may call any method or constructor covered by an annotation that names a value from any of these properties.

The built-in `dh-defaults.prop` file enables three annotation values:

- `function_library` covers Deephaven's [built-in functions](../how-to-guides/built-in-functions.md).
- `vector` and `base` cover the methods of Deephaven's [vector and array types](../how-to-guides/work-with-arrays.md).

The default property is:

```properties
ColumnExpressionValidator.annotationSets.default=base,vector,function_library
```

A [configuration file](../how-to-guides/configuration/config-file.md) that doesn't include `dh-defaults.prop` gets none of these defaults.

To enable your own annotation values, add a property with a new label:

```properties
# Enable the annotation values you defined
ColumnExpressionValidator.annotationSets.myapp=my_math_utils,static_math_methods,string_helpers,safe_math,text_processing
```

A property that reuses a label replaces the earlier value, so redefining `annotationSets.default` without `base`, `vector`, and `function_library` turns off the built-in annotation values.

### Call a permitted method from a client

Once the class is on the server's classpath and its annotation value is enabled, a client can call the method by its fully qualified name. This [Python client](/core/client-api/python/) example calls `MathUtils.square` from [Annotate entire classes](#annotate-entire-classes), then sends a formula that nothing permits:

```python syntax
from pydeephaven import Session

# Add auth_type and auth_token to match your server's authentication
session = Session(host="localhost", port=10000)

source = session.empty_table(5).update(["X = i * 1.5"])

# Permitted: MathUtils is annotated with the enabled my_math_utils value
squares = source.update(["Y = com.example.formulas.MathUtils.square(X)"])

# Rejected: no annotation or pointcut pattern permits System.getenv
env = source.update(["Home = java.lang.System.getenv(`HOME`)"])
```

When a formula calls a method that isn't permitted, the client receives an error that gives only an error ID, and the server log records the reason under that ID. For the rejected formula above, the reason is:

```text
User expressions are not permitted to use static method getenv(java.lang.String) on class java.lang.System
```

A rejected constructor call produces a message that starts with `User expressions are not permitted to instantiate`, followed by the class and its parameter types.

### Best practices for annotations

When designing your annotations, follow these recommendations:

- **Annotate only methods that are safe for user formulas.** Once you enable its annotation value, an annotated method is callable by any client that sends a formula.
- **Use descriptive annotation values.** A value such as `math_operations` or `string_utilities` says what the methods do.
- **Group related methods under one annotation value.** Many small annotation values make configuration harder to manage.

## Permit library methods with pointcut expressions

Use this approach to permit methods from libraries you don't control, such as the JDK's built-in classes or third-party libraries.

Since you can't add annotations to external code, you use pointcut expressions instead. Deephaven reads these patterns with [OpenRewrite's method matcher](https://docs.openrewrite.org/reference/method-patterns#examples), which uses the pointcut syntax from the AspectJ language. One pattern can match one Java method or many.

### Understanding pointcut patterns

A pointcut expression has three parts:

1. **Class name**: The fully qualified class name, for example `java.lang.String`.
2. **Method name**: The specific method, `*` for any method or constructor, or `<constructor>` for constructors only.
3. **Parameters**: The parameter types or `(..)` for any parameters.

Write the class name, a space, then the method name followed by its parameters in parentheses. For example:

- `java.lang.Math max(int, int)` matches `Math.max` with two `int` parameters.
- `java.lang.Math *(..)` matches every method and constructor on `Math`.
- `java.math.MathContext <constructor>(int)` matches the `MathContext` constructor that takes an `int`.

A pattern that names `java.lang.Object` matches that method on every class, so `java.lang.Object toString()` permits every class's `toString` method. For any other class, a pattern matches a method only when the named class declares it. A subclass that inherits the method without overriding it still calls the named class's method, so the pattern matches. A subclass or implementation that overrides the method declares its own version, which the pattern doesn't match. For example, `java.util.List size()` permits `size` on a value whose type is `List`, but not on a value whose type is `ArrayList`, because `ArrayList` declares its own `size` method.

### Pattern matching symbols

Two symbols match more than one name or type, in more places than the examples above show:

- `*`: A wildcard in a class or method name (for example, `unmodifiable*`), or one parameter type in the argument list.
- `..`: Any number of parameters of any type in the argument list, or any subpackage in the class name.

### Enable pointcut patterns

A property in the form `ColumnExpressionValidator.allowedMethods.<label>=<patterns>` enables pointcut patterns:

- `<label>` is a name you choose. It doesn't change which methods are permitted.
- `<patterns>` is one or more patterns separated by `;`.

The built-in `dh-defaults.prop` file permits these methods and constructors from common Java classes:

```properties
# All methods and constructors on primitive wrapper classes (Integer, Double, etc.)
ColumnExpressionValidator.allowedMethods.primitives=java.lang.Character *(..);java.lang.Byte *(..);java.lang.Short *(..);java.lang.Integer *(..);java.lang.Long *(..);java.lang.Float *(..);java.lang.Double *(..);java.lang.Boolean *(..)

# All String methods and constructors (safe because strings are immutable)
ColumnExpressionValidator.allowedMethods.basic=java.lang.String *(..)

# Number and math classes
ColumnExpressionValidator.allowedMethods.numbers=java.math.BigInteger *(..);java.math.BigDecimal *(..);java.lang.Number *(..)

# Date and time classes
ColumnExpressionValidator.allowedMethods.time=java.time.Instant *(..);java.time.LocalTime *(..);java.time.LocalDate *(..);java.time.ZonedDateTime *(..)

# Common Object methods that are permitted
ColumnExpressionValidator.allowedMethods.toString=java.lang.Object toString()
ColumnExpressionValidator.allowedMethods.equals=java.lang.Object equals(java.lang.Object)
ColumnExpressionValidator.allowedMethods.hashCode=java.lang.Object hashCode()
```

As with annotation values, a configuration file that doesn't include `dh-defaults.prop` gets none of these defaults.

To permit more methods, add a property with a new label. A property that reuses a default label, such as `numbers`, replaces that default group.

Every method or constructor a pattern matches becomes callable by any client that sends a formula. See [Best practices for pointcut patterns](#best-practices-for-pointcut-patterns) for how to keep patterns narrow.

### Common examples

Each of these properties adds a new group of patterns:

```properties
# Allow any method or constructor on the Math class with any parameters
ColumnExpressionValidator.allowedMethods.math=java.lang.Math *(..)

# Allow size() on values typed as java.util.List (no parameters)
ColumnExpressionValidator.allowedMethods.list_size=java.util.List size()

# Allow specific methods on the StrictMath class
ColumnExpressionValidator.allowedMethods.strict_math=java.lang.StrictMath sqrt(double);java.lang.StrictMath hypot(double, double)

# Allow all methods and constructors on multiple time classes
ColumnExpressionValidator.allowedMethods.more_time=java.time.Duration *(..);java.time.Period *(..);java.time.LocalDateTime *(..)

# Allow the MathContext constructor that takes a precision
ColumnExpressionValidator.allowedMethods.math_context=java.math.MathContext <constructor>(int)
```

### Best practices for pointcut patterns

When writing pointcut patterns, follow these recommendations:

- **Prefer exact signatures for classes whose methods have side effects.** A pattern such as `*(..)` permits every method and constructor on the class, including ones you didn't review.
- **Never write a wildcard pattern on `java.lang.Object`.** Because every class is an `Object`, `java.lang.Object *(..)` permits every method and constructor of every class, which turns off validation.
- **Name the class that declares the method formulas call.** A pattern on an interface or superclass doesn't cover a subclass's override. To permit `size` on `ArrayList` values, name `java.util.ArrayList`.
- **Add a property with a new label rather than redefining a default one.** Redefining a default label, such as `numbers`, removes the patterns it held.

## Backwards compatibility (Deephaven 0.39.x and earlier)

Deephaven 0.39.x and earlier validated formulas against a simple allowlist of method names. Deephaven 0.40.0 replaced that allowlist with the [annotation](#permit-your-own-code-with-annotations) and [pointcut expression](#permit-library-methods-with-pointcut-expressions) rules on this page.

The method-name allowlist is less secure and less flexible than the annotation and pointcut rules. If you still need it, revert to the old behavior with:

```properties
ColumnExpressionValidator=method_name
```

With this setting, Deephaven ignores the `ColumnExpressionValidator.annotationSets` and `ColumnExpressionValidator.allowedMethods` properties, and the allowlist can't be changed.

## Related documentation

- [Aggregations in Deephaven](./aggregations.md)
- [Arrays](../how-to-guides/work-with-arrays.md)
- [Built-in query language functions](../how-to-guides/built-in-functions.md)
- [Configuration properties reference](../how-to-guides/configuration/configuration-properties.md)
- [Configure the Deephaven console service](../how-to-guides/configuration/console-service.md#configuration)
- [Create a Deephaven configuration file](../how-to-guides/configuration/config-file.md)
- [Create a hierarchical rollup table programmatically](../how-to-guides/rollup-table.md)
- [Create a hierarchical tree table](../how-to-guides/tree-table.md)
- [Extract data with a Java client](../how-to-guides/java-client.md)
- [Formulas](../how-to-guides/formulas.md)
- [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md)
- [Java objects and classes in query strings](../how-to-guides/java-classes.md)
- [Python client (`pydeephaven`)](/core/client-api/python/)
- [Query scope](../how-to-guides/query-scope.md)
- [Select and create columns](../how-to-guides/use-select-view-update.md)
- [Use the JS API](../how-to-guides/use-jsapi.md)
- [`AggCountWhere`](../reference/table-operations/group-and-aggregate/AggCountWhere.md)
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`CumCountWhere`](../reference/table-operations/update-by-operations/cum-count-where.md)
- [`headBy`](../reference/table-operations/group-and-aggregate/headBy.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`RollingCountWhere`](../reference/table-operations/update-by-operations/rolling-count-where.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`select`](../reference/table-operations/select/select.md)
- [`tailBy`](../reference/table-operations/group-and-aggregate/tailBy.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [OpenRewrite method patterns](https://docs.openrewrite.org/reference/method-patterns#examples)
- [`UserInvocationPermitted` Javadoc](/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html)
