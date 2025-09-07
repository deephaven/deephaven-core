//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.annotations;

import java.lang.annotation.*;

/**
 * This annotation is used to permit users to invoke methods from formulas (e.g., select, view or where calls).
 *
 * <p>
 * The annotation may be specified on a method or a class. If specified on a class, then all methods of the class may be
 * invoked. Class annotations can be refined with the {@link #classScope()} attribute, to select only static or instance
 * methods. If specified on a method, that method may be invoked and the class scope is ignored.
 * </p>
 *
 * <p>
 * The annotated methods must belong to one or more sets. The list of permitted sets can be configured at runtime, to
 * make it simple to enable or disable a class of methods for users. If any of the sets defined by the method is
 * permitted, then the invocation is permitted.
 * </p>
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.CONSTRUCTOR})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface UserInvocationPermitted {
    enum ScopeType {
        StaticAndInstance, Static, Instance,
    }
    // String STATIC_CLASS_SCOPE = "static";
    // String INSTANCE_CLASS_SCOPE = "instance";

    /**
     * When applied to a class, the type of methods this annotation applies to. If {@link ScopeType#StaticAndInstance},
     * null, or not specified, then both static and instance methods are permitted. If {@link ScopeType#Static} is
     * specified, then only static methods are permitted (unless the instance method is otherwise annotated or
     * configured). If {@link ScopeType#Instance} is specified, then only instance methods are permitted (unless the
     * static method is otherwise annotated or configured).
     * 
     * @return the scope for this class annotation
     */
    ScopeType classScope() default ScopeType.StaticAndInstance;

    /**
     * An array of sets for configuring permitted methods.
     *
     * @return the sets of configured methods this belongs to.
     */
    String[] value();
}
