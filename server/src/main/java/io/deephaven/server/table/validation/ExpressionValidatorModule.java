//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import dagger.Module;
import dagger.Provides;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.engine.validation.MethodInvocationValidator;
import io.deephaven.server.table.validation.methodlist.MethodListInvocationValidator;
import io.deephaven.util.annotations.UserInvocationPermitted;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@Module
public class ExpressionValidatorModule {
    @Provides
    public ColumnExpressionValidator provideExpressionValidator() {
        final Configuration configuration = Configuration.getInstance();
        final String expressionValidator = configuration.getStringWithDefault("ColumnExpressionValidator", "parsed");
        if (expressionValidator.equals("method_name")) {
            return new MethodNameColumnExpressionValidator();
        } else if (expressionValidator.equals("parsed")) {
            return getParsingColumnExpressionValidatorFromConfiguration(configuration);
        } else {
            throw new IllegalArgumentException("Unsupported column expression: " + expressionValidator);
        }
    }

    /**
     * Create a {@link ParsingColumnExpressionValidator} based on configuration properties.
     *
     * <p>
     * There are five types of configuration, each indicated with a property prefix. The portion of a property name
     * after the prefix has no functional effect, but should be something to help document the type or reason for
     * permitting certain functions.
     * </p>
     *
     * <table>
     * <tr>
     * <th>Prefix</th>
     * <th>Description</th>
     * <th>Format</th>
     * </tr>
     * <tr>
     * <td>ColumnExpressionValidator.instanceTargets.</td>
     * <td>Classes for which all instance methods are permitted</td>
     * <td>A comma separated list of classes. Spaces are ignored. Classes should be specified as input to
     * {@link Class#forName(String)}.</td>
     * </tr>
     * <tr>
     * <td>ColumnExpressionValidator.staticTargets.</td>
     * <td>Classes for which all static methods are permitted</td>
     * <td>A comma separated list of classes. Spaces are ignored. Classes should be specified as input to
     * {@link Class#forName(String)}.</td>
     * </tr>
     * <tr>
     * <td>ColumnExpressionValidator.instanceMethods.</td>
     * <td>Permitted instance methods</td>
     * <td>A single instance method formatted as <code>class#method(class1, class2)</code>. See below.</td>
     * </tr>
     * <tr>
     * <td>ColumnExpressionValidator.staticMethods.</td>
     * <td>Permitted static methods</td>
     * <td>A single instance method formatted as <code>class#method(class1, class2)</code>. See below.</td>
     * </tr>
     * <tr>
     * <td>ColumnExpressionValidator.annotationSets.</td>
     * <td>Names of permitted annotation {@link UserInvocationPermitted#value()}</td>
     * <td>A comma separated list of strings. Spaces are ignored.</td>
     * </tr>
     * </table>
     *
     * <p>
     * Individual methods are specified as <code>class#method(class1, class2)</code>. The class and parameter types must
     * be valid input to {@link Class#forName(String)}.
     * </p>
     *
     * <p>
     * For instance methods, any overriding class's implementation of the method may be called. For example, if
     * <code>java.lang.Object#toString()</code> is permitted then any classes implementation of
     * {@link Object#toString()} is permitted. An overriding class may have parameters that are super types of the
     * specified method. For example if class <code>A</code> implements <code>add(java.lang.Integer)</code> is
     * permitted; then if <code>A</code> is a superclass of <code>B</code>, class <code>B</code>'s method
     * <code>add(java.lang.Number)</code> is also permitted.
     * </p>
     *
     * @param configuration the configuration instance to read properties from
     * @return a validator ready for use
     */
    @NotNull
    public static ParsingColumnExpressionValidator getParsingColumnExpressionValidatorFromConfiguration(
            final Configuration configuration) {
        final List<String> methodList = getMethodListFromConfiguration(configuration);

        final MethodInvocationValidator listValidator = new MethodListInvocationValidator(methodList);
        final MethodInvocationValidator annotationValidator = new AnnotationMethodInvocationValidator(
                getAnnotationSets(configuration, "ColumnExpressionValidator.annotationSets."));

        return new ParsingColumnExpressionValidator(List.of(new CachingMethodInvocationValidator(annotationValidator),
                new CachingMethodInvocationValidator(listValidator)));
    }

    /**
     * Create a list of pointcuts for the MethodListInvocationValidator based on configuration properties.
     *
     * <p>
     * For instance methods, any overriding class's implementation of the method may be called. For example, if
     * <code>java.lang.Object#toString()</code> is permitted then any classes implementation of
     * {@link Object#toString()} is permitted.
     * </p>
     *
     * @param configuration the configuration instance to read properties from
     * @return a MethodList object
     */
    public static List<String> getMethodListFromConfiguration(final Configuration configuration) {
        return configuration.getProperties("ColumnExpressionValidator.allowedMethods.").values().stream()
                .flatMap(x -> Arrays.stream(((String) x).split(";"))).map(String::trim)
                .collect(Collectors.toUnmodifiableList());
    }

    private static Set<String> getAnnotationSets(final Configuration configuration, final String prefix) {
        final Set<String> allowedSets = new HashSet<>();
        configuration.getProperties(prefix)
                .forEach((k, v) -> Arrays.stream(((String) v).split(",")).map(String::trim).forEach(allowedSets::add));
        return allowedSets;
    }
}
