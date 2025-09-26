---
title: Formula validation configuration
sidebar_label: Formula validation configuration
---

When a user creates a custom column or filters a table, an expression is sent to the Deephaven server over gRPC. When that expression is compiled into a dynamic formula, the server must validate that the requested operations are permitted. Script session code execution is not validated as the Deephaven engine does not parse script code. The console service can be disabled via [configuration](../how-to-guides/configuration/console-service.md#configuration).

When creating a dynamic formula, the Deephaven engine parses the expression and determines what methods invocations are used. When method validation is enabled, implicit calls are not permitted (e.g., calling a Python function or Groovy closure in your scope).

Each method invocation is then either permitted or denied based on configuration. There are two flexible ways to configure column expression validation:

- Annotations
- Pointcut expressions

# Annotations

When you control the source code of the class defining the methods you would like to call, the most flexible way to control access to those methods is using the [`@UserInvocationPermitted`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/annotations/UserInvocationPermitted.html) annotation. The `value` argument of the annotation is a string name used to identify permitted methods in the configuration properties. You can apply the annotation to a class, in which case all methods are permitted by default. You can limit the methods to either `Static` or `Instance` by specifying a `classScope` parameter to the annotation.

For example, in the following classes `AnnotatedStaticClass` class permits the `stm` method to be called and the `AnnotatedInstanceClass` allows the `im` method to be called.

```java
@UserInvocationPermitted(value = "example_static_methods", classScope = UserInvocationPermitted.ScopeType.Static)
public static class AnnotatedStaticClass {
    public String instance() {
        return "im";
    }

    public static String stm() {
        return "sm";
    }
}

@UserInvocationPermitted(value = "example_instance_methods", classScope = UserInvocationPermitted.ScopeType.Instance)
public static class AnnotatedInstanceClass {
    public String im() {
        return "im";
    }

    public static String stm() {
        return "sm";
    }
}
```

You may also annotate a single method:

```java
public static class AnnotatedMethods {
    @UserInvocationPermitted(value = "example_method_annotation1")
    public String im() {
        return "im";
    }

    @UserInvocationPermitted(value = "example_method_annotation2")
    public static String stm() {
        return "sm";
    }
}
```

The best practice is to group related methods into a single named set, to avoid excess configuration. In addition to annotating the classes, you must set a property of the form `ColumnExpressionValidator.annotationSets.<name>`. The following property permits the methods annotated in the examples above:

```properties
ColumnExpressionValidator.annotationSets.example=example_instance_methods,example_static_methods,example_method_annotation1,example_method_annotation2
```

The default configuration for Deephaven libraries is:

```properties
ColumnExpressionValidator.annotationSets.default=base,vector,function_library
```

# Pointcut expressions

If you do not control the class defining the methods you would like to call (like for those provided by the JDK), you can define properties to permit methods using pattern matching.

AspectJ Pointcut expressions, as defined by [OpenRewrite's method matcher](https://docs.openrewrite.org/reference/method-patterns#examples), are a simple way to match one or more Java methods. A pointcut expression consists of a fully qualified classname, a method name, and the method's arguments. `*` can be used to indicate a single wildcard (e.g., any method on a class or any one argument). `..` can be used to indicate any number of arguments of any type. For example, the pointcut `java.lang.String *(..)` permits any method on String, with any number of arguments. Each pointcut is expressed in a configuraiton property prefixed with `ColumnExpressionValidator.allowedMethods.` and then a descriptive name.

The default Deephaven pointcut configuration is:

```properties
ColumnExpressionValidator.allowedMethods.primitives=java.lang.Character *(..);java.lang.Byte *(..);java.lang.Short *(..);java.lang.Integer *(..);java.lang.Long *(..);java.lang.Float *(..);java.lang.Double *(..);java.lang.Boolean *(..)
# Permit any instance methods on immutable types (e.g. Strings; Instants; BigInteger; and Decimal)
ColumnExpressionValidator.allowedMethods.basic=java.lang.String *(..)
ColumnExpressionValidator.allowedMethods.numbers=java.math.BigInteger *(..);java.math.BigDecimal *(..);java.lang.Number *(..)
ColumnExpressionValidator.allowedMethods.time=java.time.Instant *(..);java.time.LocalTime *(..);java.time.LocalDate *(..);java.time.ZonedDateTime *(..)
# Permit converting any value to a String, comparing equality, or using a hashCode
ColumnExpressionValidator.allowedMethods.toString=java.lang.Object toString()
ColumnExpressionValidator.allowedMethods.equals=java.lang.Object equals(java.lang.Object)
ColumnExpressionValidator.allowedMethods.hashCode=java.lang.Object hashCode()
```

# Backwards Compatibility for 0.39.x and earlier

In versions of Deephaven prior to 0.40.0, validation on expressions was performed only using the name of the method in a hardcoded list. This behavior can be re-enabled by setting the property `ColumnExpressionValidator` to `method_name`.
