/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.lang;

import com.github.javaparser.TokenRange;
import com.github.javaparser.ast.*;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.comments.BlockComment;
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.comments.LineComment;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.*;
import com.github.javaparser.ast.type.WildcardType;
import com.github.javaparser.ast.type.*;
import com.github.javaparser.ast.visitor.GenericVisitor;
import com.github.javaparser.ast.visitor.GenericVisitorAdapter;
import com.github.javaparser.ast.visitor.VoidVisitor;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.util.PyCallableWrapper;
import io.deephaven.engine.util.PyCallableWrapper.ColumnChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ConstantChunkArgument;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import io.deephaven.vector.*;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.lang.reflect.Type;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class QueryLanguageParser extends GenericVisitorAdapter<Class<?>, QueryLanguageParser.VisitArgs> {
    /**
     * Verify that the source code obtained from printing the AST is the same as the source code produced by the
     * original technique of writing code to a StringBuilder while visting nodes.
     */
    private static final boolean VERIFY_AST_CHANGES = true;

    private static final Logger log = LoggerFactory.getLogger(QueryLanguageParser.class);
    private final Collection<Package> packageImports;
    private final Collection<Class<?>> classImports;
    private final Collection<Class<?>> staticImports;
    private final Map<String, Class<?>> testOverrideClassLookups;

    private final Map<String, Class<?>> variables;
    private final Map<String, Class<?>[]> variableParameterizedTypes;

    private final HashSet<String> variablesUsed = new HashSet<>();

    // We need some class to represent null. We know for certain that this one won't be used...
    private static final Class<?> NULL_CLASS = QueryLanguageParser.class;

    private static final Set<String> simpleNameWhiteList = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("java.lang", Vector.class.getPackage().getName())));

    /**
     * The result of the QueryLanguageParser for the expression passed given to the constructor.
     */
    private final Result result;

    private final HashMap<Node, Class<?>> cachedTypes = new HashMap<>();
    private final boolean unboxArguments;

    /**
     * Create a QueryLanguageParser and parse the given {@code expression}. After construction, the
     * {@link QueryLanguageParser.Result result} of parsing the {@code expression} is available with the
     * {@link #getResult()}} method.
     *
     * @param expression The query language expression to parse
     * @param packageImports Wildcard package imports
     * @param classImports Individual class imports
     * @param staticImports Wildcard static imports. All static variables and methods for the given classes are
     *        imported.
     * @param variables A map of the names of scope variables to their types
     * @param variableParameterizedTypes A map of the names of scope variables to their parameterized types
     * @throws QueryLanguageParseException If any exception or error is encountered
     */
    public QueryLanguageParser(String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableParameterizedTypes) throws QueryLanguageParseException {
        this(expression, packageImports, classImports, staticImports, variables,
                variableParameterizedTypes, true);
    }

    /**
     * Create a QueryLanguageParser and parse the given {@code expression}. After construction, the
     * {@link QueryLanguageParser.Result result} of parsing the {@code expression} is available with the
     * {@link #getResult()}} method.
     *
     * @param expression The query language expression to parse
     * @param packageImports Wildcard package imports
     * @param classImports Individual class imports
     * @param staticImports Wildcard static imports. All static variables and methods for the given classes are
     *        imported.
     * @param variables A map of the names of scope variables to their types
     * @param variableParameterizedTypes A map of the names of scope variables to their parameterized types
     * @param unboxArguments If true it will unbox the query scope arguments
     * @throws QueryLanguageParseException If any exception or error is encountered
     */
    public QueryLanguageParser(String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableParameterizedTypes, boolean unboxArguments)
            throws QueryLanguageParseException {
        this(
                expression,
                packageImports,
                classImports,
                staticImports,
                null,
                variables,
                variableParameterizedTypes,
                unboxArguments,
                false);
    }

    QueryLanguageParser(String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> testOverrideClassLookups,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableParameterizedTypes) throws QueryLanguageParseException {
        this(expression, packageImports, classImports, staticImports, testOverrideClassLookups, variables,
                variableParameterizedTypes, true, false);
    }

    QueryLanguageParser(String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> testOverrideClassLookups,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableParameterizedTypes, boolean unboxArguments, final boolean verifyIdempotence)
            throws QueryLanguageParseException {
        this.packageImports = packageImports == null ? Collections.emptySet()
                : Require.notContainsNull(packageImports, "packageImports");
        this.classImports =
                classImports == null ? Collections.emptySet() : Require.notContainsNull(classImports, "classImports");
        this.staticImports = staticImports == null ? Collections.emptySet()
                : Require.notContainsNull(staticImports, "staticImports");
        this.testOverrideClassLookups = testOverrideClassLookups;
        this.variables = variables == null ? Collections.emptyMap() : variables;
        this.variableParameterizedTypes =
                variableParameterizedTypes == null ? Collections.emptyMap() : variableParameterizedTypes;
        this.unboxArguments = unboxArguments;

        // Convert backticks *before* converting single equals!
        // Backticks must be converted first in order to properly identify single-equals signs within
        // String and char literals, which should *not* be converted.
        expression = convertBackticks(expression);
        expression = convertSingleEquals(expression);

        final VisitArgs printer = VisitArgs.create();
        try {
            Expression expr = JavaExpressionParser.parseExpression(expression);
            WrapperNode wrapperNode = new WrapperNode(expr);
            expr.setParentNode(wrapperNode);

            Class<?> type = expr.accept(this, printer);

            if (type == NULL_CLASS) {
                type = Object.class;
            }

            final String printedSource = printer.builder.toString();

            if (VERIFY_AST_CHANGES) {
                final Node parsedNode = wrapperNode.getChildNodes().get(0);
                parsedNode.setParentNode(null); // LexicalPreservingPrinter crashes on WrapperNode.
                final String parserExpressionDumped = LexicalPreservingPrinter.print(parsedNode);

                // Compare the output from the LexicalPreservingPrinter (after modifying the AST) with the
                // output from the printer to ensure behavior is the same:
                if (!parserExpressionDumped.equals(printedSource)) {
                    throw new ParserVerificationFailure("Expression changed!\n" +
                            "    Orig result               : " + printedSource + ".\n" +
                            "    Printed parsed expression : " + parserExpressionDumped);
                }
            }

            if (verifyIdempotence) {
                try {
                    // make sure the parser has no problem reparsing its own output and makes no changes to it.
                    final QueryLanguageParser validationQueryLanguageParser = new QueryLanguageParser(printedSource,
                            packageImports, classImports, staticImports, testOverrideClassLookups, variables,
                            variableParameterizedTypes, false, false);

                    final String reparsedSource = validationQueryLanguageParser.result.source;
                    Assert.equals(
                            printedSource, "printedSource",
                            reparsedSource, "reparsedSource");
                    Assert.equals(
                            type,
                            "type",
                            validationQueryLanguageParser.result.type,
                            "validationQueryLanguageParser.result.type");
                } catch (Exception ex) {
                    throw new ParserVerificationFailure("Expression result failed reparse check", ex);
                }
            }

            result = new Result(type, printer.builder.toString(), variablesUsed);
        } catch (Throwable e) {
            // need to catch it and make a new one because it contains unserializable variables...
            final StringBuilder exceptionMessageBuilder = new StringBuilder(1024)
                    .append("\n\nHaving trouble with the following expression:\n")
                    .append("Full expression           : ")
                    .append(expression)
                    .append('\n')
                    .append("Expression having trouble : ")
                    .append(printer.builder)
                    .append('\n');

            final boolean VERBOSE_EXCEPTION_MESSAGES = verifyIdempotence || Configuration
                    .getInstance()
                    .getBooleanWithDefault("QueryLanguageParser.verboseExceptionMessages", false);

            if (VERBOSE_EXCEPTION_MESSAGES) { // include stack trace
                exceptionMessageBuilder
                        .append("Exception full stack trace: ")
                        .append(ExceptionUtils.getStackTrace(e))
                        .append('\n');
            } else {
                exceptionMessageBuilder
                        .append("Exception type            : ")
                        .append(e.getClass().getName())
                        .append('\n')
                        .append("Exception message         : ")
                        .append(e.getMessage())
                        .append('\n');
            }

            QueryLanguageParseException newException =
                    new QueryLanguageParseException(exceptionMessageBuilder.toString());
            newException.setStackTrace(e.getStackTrace());

            throw newException;
        }
    }

    public static final class QueryLanguageParseException extends Exception {
        private QueryLanguageParseException(String message) {
            super(message);
        }
    }

    /**
     * Retrieves the result of the parser, which includes the translated expression, its return type, and the variables
     * it uses.
     */
    public Result getResult() {
        return result;
    }

    /**
     * Convert single equals signs (the assignment operator) to double-equals signs (equality operator). The parser will
     * then replace the equality operator with an appropriate equality-checking methods. Assignments are not supported.
     * <p>
     * This method does not have any special handling for backticks; accordingly this method should be run <b>after</b>
     * {@link #convertBackticks(String)}.
     *
     * @param expression The expression to convert
     * @return The expression, with unescaped single-equals signs converted to the equality operator (double-equals)
     */
    public static String convertSingleEquals(String expression) {
        final int len = expression.length();
        boolean isInChar = false; // whether we are currently inside a char literal
        boolean isInStr = false; // whether we are currently inside a String literal
        boolean nextCharEscaped = false; // whether the next char is escaped.
        StringBuilder ret = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            char c = expression.charAt(i);
            char cBefore = i == 0 ? 0 : expression.charAt(i - 1);
            char cAfter = i == len - 1 ? 0 : expression.charAt(i + 1);

            if (nextCharEscaped) {
                nextCharEscaped = false;
            } else {
                if (!isInChar && c == '"') {
                    isInStr = !isInStr;

                } else if (!isInStr && c == '\'') {
                    isInChar = !isInChar;

                } else if ((isInStr || isInChar) && c == '\\') {
                    nextCharEscaped = true;
                }
            }

            ret.append(c);

            if (c == '=' && cBefore != '=' && cBefore != '<' && cBefore != '>' && cBefore != '!' && cAfter != '='
                    && !isInChar && !isInStr) {
                ret.append('=');
            }
        }
        return ret.toString();
    }

    /**
     * Convert backticks into double-quote characters, unless the backticks are already enclosed in double-quotes.
     * <p>
     * Also, within backticks, double-quotes are automatically re-escaped. For example, in the following string "`This
     * expression uses \"double quotes\"!`" The string will be converted to: "\"This expression uses \\\"double
     * quotes\\\"!\""
     *
     * @param expression The expression to convert
     * @return The expression, with backticks and double-quotes appropriately converted and escaped
     */
    public static String convertBackticks(String expression) {
        int len = expression.length();
        StringBuilder ret = new StringBuilder(len);

        boolean isInQuotes = false;
        boolean isInChar = false;
        boolean isInBackticks = false;
        boolean nextCharEscaped = false;

        for (int i = 0; i < len; i++) {
            char c = expression.charAt(i);

            if (nextCharEscaped) {
                ret.append(c);
                nextCharEscaped = false;
            } else {
                if (c == '`' && !isInQuotes && !isInChar) {
                    isInBackticks = !isInBackticks;
                    ret.append('"');
                } else {
                    if (c == '"' && !isInChar) {
                        if (isInBackticks)
                            ret.append('\\'); // Escape the quotes
                        else
                            isInQuotes = !isInQuotes;
                    } else if (c == '\'' && !isInBackticks && !isInQuotes) {
                        isInChar = !isInChar;
                    } else if ((isInQuotes || isInBackticks || isInChar) && c == '\\') {
                        nextCharEscaped = true;
                    }
                    ret.append(c);
                }
            }
        }
        return ret.toString();
    }

    private Class<?>[] printArguments(Expression[] arguments, VisitArgs printer) {
        ArrayList<Class<?>> types = new ArrayList<>();

        printer.append('(');
        for (int i = 0; i < arguments.length; i++) {
            types.add(arguments[i].accept(this, printer));

            if (i != arguments.length - 1) {
                printer.append(", ");
            }
        }
        printer.append(')');

        return types.toArray(new Class[0]);
    }

    static Class<?> binaryNumericPromotionType(Class<?> type1, Class<?> type2) {
        if (type1 == double.class || type2 == double.class) {
            return double.class;
        }

        if (type1 == Double.class || type2 == Double.class) {
            return double.class;
        }

        if (type1 == float.class || type2 == float.class) {
            return float.class;
        }

        if (type1 == Float.class || type2 == Float.class) {
            return float.class;
        }

        if (type1 == long.class || type2 == long.class) {
            return long.class;
        }

        if (type1 == Long.class || type2 == Long.class) {
            return long.class;
        }

        return int.class;
    }

    /**
     * Search for a class with the given {@code name}. This can be a fully-qualified name, or the simple name of an
     * imported class.
     *
     * @param name The name of the class to search for
     * @return The class, if it exists; otherwise, {@code null}.
     */
    private Class<?> findClass(String name) {
        if (testOverrideClassLookups != null) {
            final Class<?> testOverrideResult = testOverrideClassLookups.get(name);
            if (testOverrideResult != null)
                return testOverrideResult;
        }

        if (name.contains(".")) { // Fully-qualified class name
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ignored) {
            }
        } else { // Simple name
            for (Class<?> classImport : classImports) {
                if (name.equals(classImport.getSimpleName())) {
                    return classImport;
                }
            }
            for (Package packageImport : packageImports) {
                try {
                    return Class.forName(packageImport.getName() + '.' + name);
                } catch (ClassNotFoundException ignored) {
                }
            }
        }
        return null;
    }

    /**
     * Search for a nested class of a given name declared within a specified enclosing class
     *
     * @param enclosingClass The class to search within
     * @param nestedClassName The simple name of the nested class to search for
     * @return The nested class, if it exists; otherwise, {@code null}}
     */
    private Class<?> findNestedClass(Class<?> enclosingClass, String nestedClassName) {
        Map<String, Class<?>> m = Stream
                .of(enclosingClass.getDeclaredClasses())
                .filter((cls) -> nestedClassName.equals(cls.getSimpleName()))
                .collect(Collectors.toMap(Class::getSimpleName, Function.identity()));
        return m.get(nestedClassName);
    }

    private Method getMethod(final Class<?> scope, final String methodName, final Class<?>[] paramTypes,
            final Class<?>[][] parameterizedTypes) {
        final ArrayList<Method> acceptableMethods = new ArrayList<>();

        if (scope == null) {
            for (final Class<?> classImport : staticImports) {
                for (Method method : classImport.getDeclaredMethods()) {
                    possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, parameterizedTypes);
                }
            }
            // for Python function/Groovy closure call syntax without the explicit 'call' keyword, check if it is
            // defined in Query scope
            if (acceptableMethods.size() == 0) {
                // if the method name corresponds to an object in the query scope, and the type of that object
                // is something that could be potentially implicitly call()ed (e.g. PyCallableWrapepr/Closure),
                // then try to add its call() method to the acceptableMethods list.
                final Class<?> methodClass = variables.get(methodName);
                if (methodClass != null && isPotentialImplicitCall(methodClass)) {
                    for (Method method : methodClass.getMethods()) {
                        possiblyAddExecutable(acceptableMethods, method, "call", paramTypes, parameterizedTypes);
                    }
                }
                if (acceptableMethods.size() > 0) {
                    variablesUsed.add(methodName);
                }
            }
        } else {
            // Add the actual methods for the object (including PyObject's methods)
            for (final Method method : scope.getMethods()) {
                possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, parameterizedTypes);
            }

            if (acceptableMethods.isEmpty() && scope.equals(org.jpy.PyObject.class)) {
                // This is a Python method call; just assume it exists and wrap in PythonScopeJpyImpl.CallableWrapper
                for (Method method : PyCallableWrapper.class.getDeclaredMethods()) {
                    possiblyAddExecutable(acceptableMethods, method, "call", paramTypes, parameterizedTypes);
                }
            } else {
                // If 'scope' is an interface, we must explicitly consider the methods in Object
                if (scope.isInterface()) {
                    for (final Method method : Object.class.getMethods()) {
                        possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, parameterizedTypes);
                    }
                }
            }
        }

        if (acceptableMethods.size() == 0) {
            throw new ParserResolutionFailure("Cannot find method " + methodName + '(' + paramsTypesToString(paramTypes)
                    + ')' + (scope != null ? " in " + scope : ""));
        }

        Method bestMethod = null;
        for (final Method method : acceptableMethods) {
            if (bestMethod == null || isMoreSpecificMethod(bestMethod, method)) {
                bestMethod = method;
            }
        }

        return bestMethod;
    }

    private static boolean isPotentialImplicitCall(Class<?> methodClass) {
        return PyCallableWrapper.class.isAssignableFrom(methodClass) || methodClass == groovy.lang.Closure.class;
    }

    private Class<?> getMethodReturnType(Class<?> scope, String methodName, Class<?>[] paramTypes,
            Class<?>[][] parameterizedTypes) {
        return getMethod(scope, methodName, paramTypes, parameterizedTypes).getReturnType();
    }

    private Class<?> calculateMethodReturnTypeUsingGenerics(
            final Class<?> scope,
            final Expression scopeExpr,
            final Method method,
            final Class<?>[] paramTypes,
            final Class<?>[][] parameterizedTypes) {
        Type methodReturnType = method.getGenericReturnType();

        int arrayDimensions = 0;

        while (methodReturnType instanceof GenericArrayType) {
            methodReturnType = ((GenericArrayType) methodReturnType).getGenericComponentType();
            arrayDimensions++;
        }

        if (!(methodReturnType instanceof TypeVariable)) {
            return method.getReturnType();
        }

        final TypeVariable<?> genericReturnType = (TypeVariable<?>) methodReturnType;

        // if the method is being invoked on a variable (i.e. NameExpr) of a parameterized type, and we have
        // type arguments for that variable, then check whether the return type is one of the scope's type parameters.
        if (scopeExpr instanceof NameExpr) {
            Class<?>[] typeArguments = variableParameterizedTypes.get(((NameExpr) scopeExpr).getNameAsString());
            if (typeArguments != null) {
                final TypeVariable<? extends Class<?>>[] scopeTypeParameters = scope.getTypeParameters();
                for (int i = 0; i < scopeTypeParameters.length; i++) {
                    if (scopeTypeParameters[i].equals(genericReturnType)) {
                        return typeArguments[i];
                    }
                }
            }
        }


        // check for the generic type in a param

        final Type[] genericParameterTypes = method.getGenericParameterTypes();

        for (int i = 0; i < genericParameterTypes.length; i++) {
            Type genericParamType = genericParameterTypes[i];
            Class<?> paramType = paramTypes[i];

            while (genericParamType instanceof GenericArrayType) {
                genericParamType = ((GenericArrayType) genericParamType).getGenericComponentType();
            }

            while (paramType.isArray()) {
                paramType = paramType.getComponentType();
            }

            if (genericReturnType.equals(genericParamType)) {
                for (; arrayDimensions > 0; arrayDimensions--) {
                    paramType = Array.newInstance(paramType, 0).getClass();
                }

                return paramType;
            }

            if ((genericParamType instanceof ParameterizedType) && (parameterizedTypes[i] != null)) {
                Type[] methodParameterizedTypes = ((ParameterizedType) genericParamType).getActualTypeArguments();

                for (int j = 0; j < methodParameterizedTypes.length; j++) {
                    if (genericReturnType.equals(methodParameterizedTypes[j])) {
                        return parameterizedTypes[i][j];
                    }
                }
            }
        }

        // check if inheritance fixes the generic type
        final Class<?> fixedGenericType = extractGenericType(scope, method, genericReturnType);
        return fixedGenericType != null ? fixedGenericType : method.getReturnType();
    }

    private Class<?> extractGenericType(final Type type, final Method method, final TypeVariable<?> genericReturnType) {
        final Class<?> clazz;
        if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;
            final Type rawType = parameterizedType.getRawType();
            if (!(rawType instanceof Class<?>)) {
                return null;
            }
            clazz = (Class<?>) rawType;

            // check if this parameterized type matches the method's declaring class, then return the fixed type
            if (method.getDeclaringClass().equals(rawType)) {
                final Type[] typeArguments = parameterizedType.getActualTypeArguments();
                final TypeVariable<?>[] typeParameters = clazz.getTypeParameters();
                for (int ii = 0; ii < typeParameters.length; ++ii) {
                    if (typeParameters[ii].getName().equals(genericReturnType.getName())
                            && typeArguments[ii] instanceof Class<?>) {
                        return (Class<?>) typeArguments[ii];
                    }
                }
            }
        } else if (type instanceof Class<?>) {
            clazz = (Class<?>) type;
        } else {
            return null;
        }

        // check for generic interfaces
        for (final Type superInterfaceType : clazz.getGenericInterfaces()) {
            final Class<?> returnType = extractGenericType(superInterfaceType, method, genericReturnType);
            if (returnType != null) {
                return returnType;
            }
        }

        // check super type
        final Type superType = clazz.getGenericSuperclass();
        return (superType != null) ? extractGenericType(superType, method, genericReturnType) : null;
    }

    @SuppressWarnings({"ConstantConditions"})
    private Constructor<?> getConstructor(final Class<?> scope, final Class<?>[] paramTypes,
            final Class<?>[][] parameterizedTypes) {
        final ArrayList<Constructor<?>> acceptableConstructors = new ArrayList<>();

        for (final Constructor<?> constructor : scope.getConstructors()) {
            possiblyAddExecutable(acceptableConstructors, constructor, scope.getName(), paramTypes, parameterizedTypes);
        }

        if (acceptableConstructors.size() == 0) {
            throw new ParserResolutionFailure("Cannot find constructor for " + scope.getName() + '('
                    + paramsTypesToString(paramTypes) + ')' + (scope != null ? " in " + scope : ""));
        }

        Constructor<?> bestConstructor = null;

        for (final Constructor<?> constructor : acceptableConstructors) {
            if (bestConstructor == null || isMoreSpecificConstructor(bestConstructor, constructor)) {
                bestConstructor = constructor;
            }
        }

        return bestConstructor;
    }

    private String paramsTypesToString(Class<?>[] paramTypes) {
        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < paramTypes.length; i++) {
            buf.append(paramTypes[i].getName());

            if (i != paramTypes.length - 1) {
                buf.append(", ");
            }
        }

        return buf.toString();
    }

    private static <EXECUTABLE_TYPE extends Executable> void possiblyAddExecutable(
            final List<EXECUTABLE_TYPE> accepted,
            final EXECUTABLE_TYPE candidate,
            final String name, final Class<?>[] paramTypes,
            final Class<?>[][] parameterizedTypes) {
        if (!candidate.getName().equals(name)) {
            return;
        }

        final Class<?>[] candidateParamTypes = candidate.getParameterTypes();

        if (candidate.isVarArgs() ? candidateParamTypes.length > paramTypes.length + 1
                : candidateParamTypes.length != paramTypes.length) {
            return;
        }

        int lengthWithoutVarArg = candidate.isVarArgs() ? candidateParamTypes.length - 1 : candidateParamTypes.length;
        for (int i = 0; i < lengthWithoutVarArg; i++) {
            Class<?> paramType = paramTypes[i];

            if (isTypedVector(paramType) && candidateParamTypes[i].isArray()) {
                paramType = convertVector(paramType, parameterizedTypes[i] == null ? null : parameterizedTypes[i][0]);
            }

            if (!canAssignType(candidateParamTypes[i], paramType)) {
                return;
            }
        }

        // If the paramTypes includes 1+ varArgs check the classes match -- no need to check if there are 0 varArgs
        if (candidate.isVarArgs() && paramTypes.length >= candidateParamTypes.length) {
            Class<?> paramType = paramTypes[candidateParamTypes.length - 1];
            Class<?> candidateParamType = candidateParamTypes[candidateParamTypes.length - 1];

            Assert.eqTrue(candidateParamType.isArray(), "candidateParamType.isArray()");

            if (isTypedVector(paramType)) {
                paramType = convertVector(paramType, parameterizedTypes[candidateParamTypes.length - 1] == null ? null
                        : parameterizedTypes[candidateParamTypes.length - 1][0]);
            }

            boolean canAssignVarArgs = candidateParamTypes.length == paramTypes.length && paramType.isArray()
                    && canAssignType(candidateParamType, paramType);

            boolean canAssignParamsToVarArgs = true;
            final Class<?> lastClass = candidateParamType.getComponentType();

            for (int i = candidateParamTypes.length - 1; i < paramTypes.length; i++) {
                paramType = paramTypes[i];

                if (isTypedVector(paramType) && lastClass.isArray()) {
                    paramType = convertVector(paramType,
                            parameterizedTypes[i] == null ? null : parameterizedTypes[i][0]);
                }

                if (!canAssignType(lastClass, paramType)) {
                    canAssignParamsToVarArgs = false;
                    break;
                }
            }

            if (!canAssignVarArgs && !canAssignParamsToVarArgs) {
                return;
            }
        }

        accepted.add(candidate);
    }

    private static boolean canAssignType(final Class<?> candidateParamType, final Class<?> paramType) {
        if (dhqlIsAssignableFrom(candidateParamType, paramType)) {
            return true;
        }

        final Class<?> maybePrimitive = ClassUtils.wrapperToPrimitive(paramType);
        if (maybePrimitive != null && dhqlIsAssignableFrom(candidateParamType, maybePrimitive)) {
            return true;
        }

        return maybePrimitive != null && candidateParamType.isPrimitive()
                && isWideningPrimitiveConversion(maybePrimitive, candidateParamType);
    }

    private static boolean isMoreSpecificConstructor(final Constructor<?> c1, final Constructor<?> c2) {
        final Boolean executableResult = isMoreSpecificExecutable(c1, c2);
        if (executableResult == null) {
            throw new IllegalStateException("Ambiguous comparison between constructors " + c1 + " and " + c2);
        }
        return executableResult;
    }

    /**
     * Is m2 more specific than m1?
     *
     * @param m1 the current best method
     * @param m2 the method that might be better
     *
     * @return true if m2 is more specific than m1
     */
    private static boolean isMoreSpecificMethod(final Method m1, final Method m2) {
        final Boolean executableResult = isMoreSpecificExecutable(m1, m2);
        // NB: executableResult can be null in cases where an override narrows its return type
        return executableResult == null ? dhqlIsAssignableFrom(m1.getReturnType(), m2.getReturnType())
                : executableResult;
    }

    private static <EXECUTABLE_TYPE extends Executable> Boolean isMoreSpecificExecutable(
            final EXECUTABLE_TYPE e1, final EXECUTABLE_TYPE e2) {

        // var args (variable arity) methods always go after fixed arity methods when determining the proper overload
        // https://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.12.2
        if (e1.isVarArgs() && !e2.isVarArgs()) {
            return true;
        }
        if (e2.isVarArgs() && !e1.isVarArgs()) {
            return false;
        }

        final Class<?>[] e1ParamTypes = e1.getParameterTypes();
        final Class<?>[] e2ParamTypes = e2.getParameterTypes();

        if (e1.isVarArgs() && e2.isVarArgs()) {
            e1ParamTypes[e1ParamTypes.length - 1] = e1ParamTypes[e1ParamTypes.length - 1].getComponentType();
            e2ParamTypes[e2ParamTypes.length - 1] = e2ParamTypes[e2ParamTypes.length - 1].getComponentType();
        }

        for (int i = 0; i < e1ParamTypes.length; i++) {
            if (!canAssignType(e1ParamTypes[i], e2ParamTypes[i]) && !isTypedVector(e2ParamTypes[i])) {
                return false;
            }
        }

        if (!Arrays.equals(e1ParamTypes, e2ParamTypes)) {
            // this means that e2 params are more specific
            return true;
        }

        return null;
    }

    /**
     * This checks whether {@code classA} is assignable from {@code classB} <b>for operations in the query engine</b>.
     * <p>
     * In particular, it is distinct from {@link Class#isAssignableFrom} in its handling of numeric types — if
     * {@code classA} is a primitive or boxed type (except boolean/Boolean), the result is true if {@code classA} is the
     * {@link #binaryNumericPromotionType binary numeric promotion type} of {@code classB}. For example,
     * {@code Double.TYPE.isAssignableFrom(Integer.TYPE)} and {@code Double.TYPE.isAssignableFrom(Integer.class)} are
     * both {@code false}, but {@code QueryLanguageParser.dhqlIsAssignableFrom(Double.TYPE, Integer.TYPE)} and
     * {@code QueryLanguageParser.dhqlIsAssignableFrom(Double.TYPE, Integer.class)} are both {@code true}.
     * <p>
     * 
     * @param classA The potential target type for a reference/primitive value of {@code classB}
     * @param classB The class whose assignability to {@code classA} should be checked
     * @return {@code true} if it's acceptable to cast {@code classB} to {@code classA}
     */
    static boolean dhqlIsAssignableFrom(Class<?> classA, Class<?> classB) {
        if (classA == classB) {
            return true;
        }

        if ((classA.isPrimitive() && classA != boolean.class) && classB.isPrimitive() && classB != boolean.class) {
            return classA == binaryNumericPromotionType(classA, classB);
        } else if (!classA.isPrimitive() && classB == NULL_CLASS) {
            return true;
        } else {
            classA = io.deephaven.util.type.TypeUtils.getBoxedType(classA);
            classB = io.deephaven.util.type.TypeUtils.getBoxedType(classB);

            return classA.isAssignableFrom(classB);
        }
    }

    private Class<?>[][] getParameterizedTypes(Expression... expressions) {
        final Class<?>[][] parameterizedTypes = new Class[expressions.length][];

        for (int i = 0; i < expressions.length; i++) {
            if ((expressions[i] instanceof NameExpr)) {
                parameterizedTypes[i] = variableParameterizedTypes.get(((NameExpr) expressions[i]).getNameAsString());
            }
        }

        return parameterizedTypes;
    }

    private static Class<?> convertVector(Class<?> type, Class<?> parameterizedType) {
        if (ObjectVector.class.isAssignableFrom(type)) {
            return Array.newInstance(parameterizedType == null ? Object.class : parameterizedType, 0).getClass();
        }
        if (IntVector.class.isAssignableFrom(type)) {
            return int[].class;
        }
        // noinspection deprecation
        if (BooleanVector.class.isAssignableFrom(type)) {
            return Boolean[].class;
        }
        if (DoubleVector.class.isAssignableFrom(type)) {
            return double[].class;
        }
        if (CharVector.class.isAssignableFrom(type)) {
            return char[].class;
        }
        if (ByteVector.class.isAssignableFrom(type)) {
            return byte[].class;
        }
        if (ShortVector.class.isAssignableFrom(type)) {
            return short[].class;
        }
        if (LongVector.class.isAssignableFrom(type)) {
            return long[].class;
        }
        if (FloatVector.class.isAssignableFrom(type)) {
            return float[].class;
        }
        throw new IllegalStateException("Unknown Vector type : " + type);
    }


    /**
     * Replaces {@code origExpr} with {@code newExpr} in {@code parentNode}. Throws an exception if {@code origExpr}
     * could not be found in the parent.
     * 
     * @param parentNode The parent node in which a child should be replaced
     * @param origExpr The original expression to find & replace in the {@code parentNode}
     * @param newExpr The new expression to put in the place of the {@code origExpr}
     */
    private static void replaceChildExpression(Node parentNode, Expression origExpr, Expression newExpr) {
        if (parentNode.replace(origExpr, newExpr)) {
            final Node newExprParent = newExpr.getParentNode().orElse(null);
            Assert.eq(newExprParent, "newExprParent", parentNode, "parentNode");
            return;
        }
        throw new IllegalStateException(
                "Could not replace expression within parent of type " + parentNode.getClass().getSimpleName());
    }

    private Class<?> getTypeWithCaching(Node n) {
        if (!cachedTypes.containsKey(n)) {
            Class<?> r = n.accept(this, VisitArgs.WITHOUT_STRING_BUILDER);
            cachedTypes.putIfAbsent(n, r);
        }
        return cachedTypes.get(n);
    }

    static String getOperatorSymbol(BinaryExpr.Operator op) {
        return op.asString();
    }

    static String getOperatorName(BinaryExpr.Operator op) {
        switch (op) {
            case OR:
                return "or";
            case AND:
                return "and";
            case BINARY_OR:
                return "binaryOr";
            case BINARY_AND:
                return "binaryAnd";
            case XOR:
                return "xor";
            case EQUALS:
                return "eq";
            case NOT_EQUALS:
                return "notEquals";
            case LESS:
                return "less";
            case GREATER:
                return "greater";
            case LESS_EQUALS:
                return "lessEquals";
            case GREATER_EQUALS:
                return "greaterEquals";
            case LEFT_SHIFT:
                return "leftShift";
            case SIGNED_RIGHT_SHIFT:
                return "signedRightShift";
            case UNSIGNED_RIGHT_SHIFT:
                return "unsignedRightShift";
            case PLUS:
                return "plus";
            case MINUS:
                return "minus";
            case MULTIPLY:
                return "multiply";
            case DIVIDE:
                return "divide";
            case REMAINDER:
                return "remainder";
            default:
                throw new IllegalArgumentException("Could not find operator name for op " + op.name());
        }
    }

    static boolean isNonFPNumber(Class<?> type) {
        type = io.deephaven.util.type.TypeUtils.getUnboxedType(type);

        // noinspection SimplifiableIfStatement
        if (type == null) {
            return false;
        }

        return type == int.class || type == long.class || type == byte.class || type == short.class
                || type == char.class;
    }

    public static boolean isTypedVector(Class<?> type) {
        return Vector.class.isAssignableFrom(type) && Vector.class != type;
    }

    /**
     * Converts the provided argument {@code expressions} for the given {@code executable} so that the expressions whose
     * types (expressionTypes) do not match the corresponding declared argument types ({@code argumentTypes}) may still
     * be used as arguments.
     * <p>
     * Conversions include casts & unwrapping of Vectors to Java arrays.
     *
     * @param executable The executable (method) to be called
     * @param argumentTypes The argument types of {@code executable}
     * @param expressionTypes The types of the {@code expressions} to be passed as arguments
     * @param parameterizedTypes The actual type arguments corresponding to the expressions
     * @param expressions The actual expressions
     * @return An array of new expressions that maintain the 'meaning' of the input {@code expressions} but are
     *         appropriate to pass to {@code executable}
     */
    private Expression[] convertParameters(final Executable executable,
            final Class<?>[] argumentTypes, final Class<?>[] expressionTypes,
            final Class<?>[][] parameterizedTypes, Expression[] expressions) {
        final int nArgs = argumentTypes.length; // Number of declared arguments
        for (int ai = 0; ai < (executable.isVarArgs() ? nArgs - 1 : nArgs); ai++) {
            if (argumentTypes[ai] != expressionTypes[ai] && argumentTypes[ai].isPrimitive()
                    && expressionTypes[ai].isPrimitive()) {
                // replace node in its parent, otherwise setArguments() will clear the parent of 'expressions[ai]'
                expressions[ai].replace(new DummyExpr());
                expressions[ai] = new CastExpr(
                        new PrimitiveType(PrimitiveType.Primitive
                                .valueOf(argumentTypes[ai].getSimpleName().toUpperCase())),
                        expressions[ai]);
            } else if (unboxArguments && argumentTypes[ai].isPrimitive() && !expressionTypes[ai].isPrimitive()) {
                expressions[ai] = new MethodCallExpr(expressions[ai],
                        argumentTypes[ai].getSimpleName() + "Value", new NodeList<>());
            } else if (argumentTypes[ai].isArray() && isTypedVector(expressionTypes[ai])) {
                expressions[ai] = new MethodCallExpr(new NameExpr("VectorConversions"), "nullSafeVectorToArray",
                        new NodeList<>(expressions[ai]));
                expressionTypes[ai] = convertVector(expressionTypes[ai],
                        parameterizedTypes[ai] == null ? null : parameterizedTypes[ai][0]);
            }
        }

        if (executable.isVarArgs()) {
            Class<?> varArgType = argumentTypes[nArgs - 1].getComponentType();

            boolean allExpressionTypesArePrimitive = true;

            final int nArgExpressions = expressionTypes.length;
            final int lastArgIndex = expressions.length - 1;
            // If there's only one arg expression provided, and it's a Vector, and the varArgType
            // *isn't* Vector, then convert the Vector to a Java array
            if (nArgExpressions == nArgs
                    && varArgType != expressionTypes[lastArgIndex]
                    && isTypedVector(expressionTypes[lastArgIndex])) {
                expressions[lastArgIndex] =
                        new MethodCallExpr(new NameExpr("VectorConversions"), "nullSafeVectorToArray",
                                new NodeList<>(expressions[lastArgIndex]));
                expressionTypes[lastArgIndex] = convertVector(expressionTypes[lastArgIndex],
                        parameterizedTypes[lastArgIndex] == null ? null : parameterizedTypes[lastArgIndex][0]);
                allExpressionTypesArePrimitive = false;
            } else if (nArgExpressions == nArgs
                    && dhqlIsAssignableFrom(argumentTypes[lastArgIndex], expressionTypes[lastArgIndex])) {
                allExpressionTypesArePrimitive = false;
            } else {
                for (int ei = nArgs - 1; ei < nArgExpressions; ei++) {
                    // iterate over the vararg argument expressions
                    if (varArgType == expressionTypes[ei]) {
                        continue;
                    }

                    if (varArgType.isPrimitive() && expressionTypes[ei].isPrimitive()) {
                        // cast primitives to the appropriate type
                        expressions[ei] = new CastExpr(
                                new PrimitiveType(PrimitiveType.Primitive
                                        .valueOf(varArgType.getSimpleName().toUpperCase())),
                                expressions[ei]);
                    } else if (unboxArguments && varArgType.isPrimitive() && !expressionTypes[ei].isPrimitive()) {
                        expressions[ei] = new MethodCallExpr(expressions[ei],
                                varArgType.getSimpleName() + "Value", new NodeList<>());
                    } else if (!expressionTypes[ei].isPrimitive()) {
                        allExpressionTypesArePrimitive = false;
                    }
                }
            }

            if (varArgType.isPrimitive() && allExpressionTypesArePrimitive) {
                // there are ambiguous oddities with primitive varargs, so if it's primitive let's just box it ourselves
                Expression[] temp = new Expression[nArgs];
                Expression[] varArgExpressions = new Expression[nArgExpressions - nArgs + 1];
                System.arraycopy(expressions, 0, temp, 0, temp.length - 1);
                System.arraycopy(expressions, nArgs - 1, varArgExpressions, 0,
                        varArgExpressions.length);

                NodeList<ArrayCreationLevel> levels = new NodeList<>(new ArrayCreationLevel());
                temp[temp.length - 1] = new ArrayCreationExpr(
                        new PrimitiveType(
                                PrimitiveType.Primitive.valueOf(varArgType.getSimpleName().toUpperCase())),
                        levels, new ArrayInitializerExpr(new NodeList<>(varArgExpressions)));

                expressions = temp;
            }
        }

        return expressions;
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<?> visit(NameExpr n, VisitArgs printer) {
        /*
         * JLS on how to resolve names: https://docs.oracle.com/javase/specs/jls/se8/html/jls-6.html#jls-6.5
         *
         * Our parser doesn't work exactly this way (some cases are not relevant, and the work is split between this
         * class and the parser library), but the behavior should be consistent with the spec.
         *
         * What matters here: 1) If it's a simple name (i.e. not a qualified name; doesn't contain a '.'), then 1. Check
         * whether it's in the scope 2. If it's not in the scope, see if it's a static import 3. If it's not a static
         * import, then it's not a situation the QueryLanguageParser has to worry about. 2) Qualified names -- we just
         * throw them to 'findClass()'. Many details are not relevant here. For example, field access is handled by a
         * different method: visit(FieldAccessExpr, StringBuilder).
         */
        printer.append(n.getNameAsString());

        Class<?> ret = variables.get(n.getNameAsString());

        if (ret != null) {
            variablesUsed.add(n.getNameAsString());

            return ret;
        }

        // We don't support static imports of individual fields/methods -- have to check among
        // *all* members of a class.
        for (Class<?> classImport : staticImports) {
            try {
                ret = classImport.getField(n.getNameAsString()).getType();
                return ret;
            } catch (NoSuchFieldException ignored) {
            }
        }

        ret = findClass(n.getNameAsString());

        if (ret != null) {
            return ret;
        }

        throw new ParserResolutionFailure("Cannot find variable or class " + n.getName());
    }

    @Override
    public Class<?> visit(PrimitiveType n, VisitArgs printer) {
        switch (n.getType()) {
            case BOOLEAN:
                printer.append("boolean");
                return boolean.class;
            case BYTE:
                printer.append("byte");
                return byte.class;
            case CHAR:
                printer.append("char");
                return char.class;
            case DOUBLE:
                printer.append("double");
                return double.class;
            case FLOAT:
                printer.append("float");
                return float.class;
            case INT:
                printer.append("int");
                return int.class;
            case LONG:
                printer.append("long");
                return long.class;
            case SHORT:
                printer.append("short");
                return short.class;
        }

        throw new IllegalStateException("Unknown primitive type : " + n.getType());
    }

    @Override
    public Class<?> visit(ArrayAccessExpr n, VisitArgs printer) {
        final Runnable nothingPrintedAssertion = getNothingPrintedAssertion(printer);

        /*
         * ArrayAccessExprs are permitted even when the 'array' is not really an array. The main use of this is for
         * Vectors, such as:
         *
         * t.view("Date", "Price").updateView("Return=Price/Price_[i-1]").
         *
         * The "Price_[i-1]" is translated to "Price.get(i-1)". But we do this generically, not just for Vectors. As an
         * example, this works (column Blah will be set to "hello"):
         *
         * map = new HashMap(); map.put("a", "hello") t = emptyTable(1).update("Blah=map[`a`]")
         *
         * As of July 2017, I don't know if anyone uses this, or if it has ever been advertised.
         */

        if (n.getName() instanceof LiteralExpr) { // a sanity check
            throw new IllegalStateException("Invalid expression: indexing into literal value");
        }

        Class<?> type = n.getName().accept(this, VisitArgs.WITHOUT_STRING_BUILDER);

        if (type.isArray()) {
            n.getName().accept(this, printer);
            printer.append('[');
            n.getIndex().accept(this, printer);
            printer.append(']');

            return type.getComponentType();
        } else {
            // An array access on something that's not an array should be replaced with a MethodCallExpr calling get()
            final Node origParent = n.getParentNode().orElseThrow();
            final MethodCallExpr newExpr = new MethodCallExpr(n.getName(), "get", NodeList.nodeList(n.getIndex()));
            replaceChildExpression(origParent, n, newExpr);
            nothingPrintedAssertion.run();
            return newExpr.accept(this, printer);
        }
    }

    @Override
    public Class<?> visit(final BinaryExpr n, VisitArgs printer) {
        final Runnable assertNothingPrinted = getNothingPrintedAssertion(printer);

        BinaryExpr.Operator op = n.getOperator();
        final Node origParent = n.getParentNode().orElseThrow();

        Class<?> lhType = getTypeWithCaching(n.getLeft());
        Class<?> rhType = getTypeWithCaching(n.getRight());

        final Expression leftExpr = n.getLeft();
        final Expression rightExpr = n.getRight();

        if ((lhType == String.class || rhType == String.class) && op == BinaryExpr.Operator.PLUS) {

            leftExpr.accept(this, printer);
            printer.append(' ').append(getOperatorSymbol(op)).append(' ');
            rightExpr.accept(this, printer);

            return String.class;
        }

        if (op == BinaryExpr.Operator.OR || op == BinaryExpr.Operator.AND) {

            leftExpr.accept(this, printer);
            printer.append(' ').append(getOperatorSymbol(op)).append(' ');
            rightExpr.accept(this, printer);

            return boolean.class;
        }

        // "Replace "x!=y" with "!(x==y)")
        if (op == BinaryExpr.Operator.NOT_EQUALS) {
            // Update the AST -- create an EQUALS BinaryExpr, then invert its result.
            final BinaryExpr equalsExpr = new BinaryExpr(n.getLeft(), n.getRight(), BinaryExpr.Operator.EQUALS);

            final UnaryExpr newExpr = new UnaryExpr(equalsExpr, UnaryExpr.Operator.LOGICAL_COMPLEMENT);
            newExpr.setComment(QueryLanguageParserComment.BINARY_OP_NEQ_CONVERSION_FLAG);
            replaceChildExpression(origParent, n, newExpr);


            assertNothingPrinted.run();
            return newExpr.accept(this, printer);
        }

        if (op == BinaryExpr.Operator.EQUALS) {
            // TODO: it's bit weird to use isNull() here as it's not part of QueryLanguageFunctionUtils.
            if (leftExpr.isNullLiteralExpr()) {
                if (printer.hasStringBuilder()) {
                    final MethodCallExpr isNullExpr = new MethodCallExpr("isNull", rightExpr);
                    replaceChildExpression(origParent, n, isNullExpr);
                    return isNullExpr.accept(this, printer);
                }
                return boolean.class;
            }
            if (rightExpr.isNullLiteralExpr()) {
                if (printer.hasStringBuilder()) {
                    final MethodCallExpr isNullExpr = new MethodCallExpr("isNull", leftExpr);
                    replaceChildExpression(origParent, n, isNullExpr);
                    return isNullExpr.accept(this, printer);
                }
                return boolean.class;
            }
        }

        boolean isArray = lhType.isArray() || rhType.isArray() || isTypedVector(lhType) || isTypedVector(rhType);

        String methodName = getOperatorName(op) + (isArray ? "Array" : "");

        if (printer.hasStringBuilder()) {
            final MethodCallExpr binaryOpOverloadMethod = new MethodCallExpr(methodName, n.getLeft(), n.getRight());
            replaceChildExpression(origParent, n, binaryOpOverloadMethod); // Replace the BinaryExpr with a method call,
                                                                           // e.g. "x==y" --> "eq(x, y)"

            assertNothingPrinted.run();
            return binaryOpOverloadMethod.accept(this, printer);
        }

        // printer.append(methodName + '(');
        // n.getLeft().accept(this, printer);
        // printer.append(',');
        // n.getRight().accept(this, printer);
        // printer.append(')');

        return getMethodReturnType(null, methodName, new Class[] {lhType, rhType},
                getParameterizedTypes(n.getLeft(), n.getRight()));
    }

    /**
     * Creates a Procedure that can be used to assert that nothing has been written to the given {@code printer} since
     * this method was called. This is used as a sanity check before calling
     * {@link Node#accept(com.github.javaparser.ast.visitor.GenericVisitor, Object)} on a newly-created
     * {@link Expression}.
     * <p>
     * The initial length of the {@code printer} is captured when the {@code Runnable} is created; an invocation of
     * {@link Runnable#run()} will throw an exception if its length has changed since then.
     *
     * @param printer The printer whose initial length should be captured
     */
    @NotNull
    private Runnable getNothingPrintedAssertion(VisitArgs printer) {
        return new Runnable() {
            final int initialLen = !printer.hasStringBuilder() ? -1 : printer.builder.length();

            @Override
            public void run() {
                if (printer.hasStringBuilder()) {
                    if (printer.builder.length() - initialLen != 0) {
                        throw new IllegalStateException(
                                "printer.builder.length() - initialLen != 0 --> printer.builder.length() = "
                                        + printer.builder.length() + "; initialLen = " + initialLen);
                    }
                }
            }
        };
    }


    @Override
    public Class<?> visit(UnaryExpr n, VisitArgs printer) {
        final Runnable nothingPrintedAssertion = getNothingPrintedAssertion(printer);
        final String opName;

        if (n.getOperator() == UnaryExpr.Operator.LOGICAL_COMPLEMENT) {
            opName = "not";
        } else if (n.getOperator() == UnaryExpr.Operator.MINUS) {
            opName = "negate";
        } else {
            throw new UnsupportedOperationException("Unary operation (" + n.getOperator().name() + ") not supported");
        }

        final Class<?> ret = getTypeWithCaching(n.getExpression());

        final boolean isNonequalOpOverload =
                n.getComment().orElse(null) == QueryLanguageParserComment.BINARY_OP_NEQ_CONVERSION_FLAG;
        if (isNonequalOpOverload) {
            Assert.equals(ret, "ret", boolean.class, "boolean.class");
        }

        if (boolean.class.equals(ret)) {
            // primitive booleans don't need a 'not()' method; they can't be null anyway
            printer.append('!');

            final Class<?> retForCheck = n.getExpression().accept(this, printer);
            Assert.equals(retForCheck, "retForCheck", ret, "ret");

            if (isNonequalOpOverload && printer.hasStringBuilder()) {
                // sanity checks -- the inner expression *must* be a BinaryExpr (for ==), and it must be replaced in
                // this UnaryExpr with a MethodCallExpr (for "eq()" or possibly "isNull()").
                Assert.instanceOf(n.getExpression(), "n.getExpression()", MethodCallExpr.class);
                final MethodCallExpr methodCall = (MethodCallExpr) n.getExpression();
                final String methodName = methodCall.getNameAsString();
                if (!"eq".equals(methodName) && !"isNull".equals(methodName)) {
                    throw new IllegalStateException(
                            "Unexpected node with BINARY_OP_NEQ_CONVERSION_FLAG: " + methodCall);
                }
            }

            return ret;
        }

        final Node origParent = n.getParentNode().orElseThrow();
        final MethodCallExpr unaryOpOverloadMethod = new MethodCallExpr(opName, n.getExpression());
        replaceChildExpression(origParent, n, unaryOpOverloadMethod);

        nothingPrintedAssertion.run();
        return unaryOpOverloadMethod.accept(this, printer);
    }

    @Override
    public Class<?> visit(CastExpr n, VisitArgs printer) {
        final Runnable nothingPrintedAssertion = getNothingPrintedAssertion(printer);

        final Class<?> ret = n.getType().accept(this, VisitArgs.WITHOUT_STRING_BUILDER); // the target type
        final Expression origExprToCast = n.getExpression();

        // retrieve the expression's type. (ignore the StringBuilder; just need the type.)
        final Class<?> exprType = origExprToCast.accept(this, VisitArgs.WITHOUT_STRING_BUILDER);

        final boolean fromPrimitive = exprType.isPrimitive();
        final boolean fromBoxedType = TypeUtils.isBoxedType(exprType);
        final Class<?> unboxedExprType = !fromBoxedType ? null : TypeUtils.getUnboxedType(exprType);

        final boolean toPrimitive = ret.isPrimitive();
        final boolean isUnboxingAndWidening;

        /*
         * Here are the rules for casting: https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.5
         *
         * First, we should ensure the cast does not violate the Java Language Specification.
         */
        if (toPrimitive) { // Casting to a primitive
            /*
             * booleans can only be cast to booleans, and only booleans can be cast to booleans. See table 5.5-A at the
             * link above.
             *
             * The JLS also places restrictions on conversions from boxed types to primitives (again, see table 5.5-A).
             */
            if (fromPrimitive && (ret.equals(boolean.class) ^ exprType.equals(boolean.class))) {
                throw new IncompatibleTypesException(
                        "Incompatible types; " + exprType.getName() + " cannot be converted to " + ret.getName());
            }

            // Now check validity if we're converting from a boxed type:
            if (fromBoxedType) {
                isUnboxingAndWidening = isWideningPrimitiveConversion(unboxedExprType, ret);
                // Unboxing and Identity conversions are always OK
                if (!ret.equals(unboxedExprType) &&
                /*
                 * Boolean is the only boxed type that can be cast to boolean, and boolean is the only primitive type to
                 * which Boolean can be cast:
                 */
                        (boolean.class.equals(ret) ^ Boolean.class.equals(exprType)
                                // Only Character can be cast to char:
                                || char.class.equals(ret) && !Character.class.equals(exprType)
                                // Other than that, only widening conversions are allowed:
                                || !isUnboxingAndWidening)) {
                    throw new IncompatibleTypesException(
                            "Incompatible types; " + exprType.getName() + " cannot be converted to " + ret.getName());
                }
            } else {
                isUnboxingAndWidening = false;
            }
        }
        /*
         * When casting primitives to boxed types, only boxing conversions are allowed When casting boxed types to boxed
         * types, only the identity conversion is allowed
         */
        else {
            if (io.deephaven.util.type.TypeUtils.isBoxedType(ret) && (fromPrimitive || fromBoxedType)
                    && !(ret.equals(io.deephaven.util.type.TypeUtils.getBoxedType(exprType)))) {
                throw new IncompatibleTypesException(
                        "Incompatible types; " + exprType.getName() + " cannot be converted to " + ret.getName());
            }
            isUnboxingAndWidening = false;
        }

        /*
         * Now actually print the cast. For casts to primitives (except boolean), we use special null-safe functions
         * (e.g. intCast()) to perform the cast.
         *
         * There is no "booleanCast()" function. However, we do try to cast to String and Boolean from PyObjects.
         *
         * There are also no special functions for the identity conversion -- e.g. "intCast(int)"
         */
        final boolean isPyUpgrade =
                ((ret.equals(boolean.class) || ret.equals(Boolean.class) || ret.equals(String.class))
                        && exprType.equals(PyObject.class));

        final boolean isPyCast = exprType != NULL_CLASS && dhqlIsAssignableFrom(PyObject.class, exprType);

        // check if cast is redundant (e.g. identity conversion or simple unboxing)
        final boolean isRedundantCast = ret.isAssignableFrom(exprType);

        // determine whether to use a straight java cast or a method call
        final boolean useCastFunction =
                (toPrimitive && !isRedundantCast && !ret.equals(boolean.class) && !ret.equals(exprType)) || isPyUpgrade;
        if (useCastFunction) {
            // Casting to a primitive, except booleans and the identity conversion.
            // Replace with a function (e.g. 'intCast(blah)' or 'doPyBooleanCast(blah)')

            /*
             * When unboxing to a wider type, do an unboxing conversion followed by a widening conversion. See table
             * 5.5-A in the JLS, at the link above.
             */
            if (isUnboxingAndWidening) {
                // Replacing the original expression with an unboxing conversion. (Then we will cast that.)
                // e.g. in "(long) myInteger", replace "myInteger" with "intCast(myInteger)", the final expression
                // will be "longCast(intCast(myInteger))".
                Assert.neqNull(unboxedExprType, "unboxedExprType");

                final String unboxingMethodName = unboxedExprType.getSimpleName() + "Cast";
                final MethodCallExpr unboxingCastExpr = new MethodCallExpr(unboxingMethodName, origExprToCast);
                unboxingCastExpr.setComment(QueryLanguageParserComment.NO_PARAMETER_REWRITING_FLAG);
                // Set the unboxing MethodCallExpr as the target of the original CastExpr 'n'.
                // Next, the CastExpr is replaced with another MethodCallExpr.
                n.setExpression(unboxingCastExpr);
                origExprToCast.setParentNode(unboxingCastExpr);

                Class<?> unboxingMethodCallReturnType = unboxingCastExpr.accept(this, VisitArgs.WITHOUT_STRING_BUILDER);
                Assert.equals(unboxingMethodCallReturnType, "unboxingMethodCallReturnType", unboxedExprType,
                        "unboxedExprType"); // verify type is as expected
            }

            // Note that the handling of unboxing/widening may have changed the expression-to-cast.
            final Expression finalExprToCast = n.getExpression();


            final String castMethodName;
            if (isPyCast) {
                if (!toPrimitive) {
                    // e.g. "doStringPyCast"
                    castMethodName = "do" + ret.getSimpleName() + "PyCast";
                } else {
                    // e.g. "intPyCast"
                    castMethodName = ret.getSimpleName() + "PyCast";
                }
            } else {
                // e.g. "intCast"
                castMethodName = ret.getSimpleName() + "Cast";
            }


            final Node origParent = n.getParentNode().orElseThrow();
            final MethodCallExpr primitiveCastExpr = new MethodCallExpr(castMethodName, finalExprToCast);
            if (ret.equals(unboxedExprType)) {
                // tell parser not to rewrite this method call (only needed when unboxing)
                primitiveCastExpr.setComment(QueryLanguageParserComment.NO_PARAMETER_REWRITING_FLAG);
            }
            replaceChildExpression(origParent, n, primitiveCastExpr);
            nothingPrintedAssertion.run();
            Class<?> primitiveCastMethodCallReturnType = primitiveCastExpr.accept(this, printer);
            // verify type is as expected:
            Assert.equals(primitiveCastMethodCallReturnType, "primitiveCastMethodCallReturnType", ret, "ret");
            return primitiveCastMethodCallReturnType;
        }

        /*
         * @formatter:off
         *
         * If we're not using an internal cast function, then the cast is either:
         *
         * - to a reference type
         * - to a primitive boolean
         * - a redundant primitive cast
         *
         * So, we will use a straight cast (e.g. '(int) blah') not a function (e.g. 'intCast(blah)')
         *
         * @formatter:on
         */

        // If the expression is anything more complex than a simple name or literal, then enclose it in parentheses to
        // ensure the order of operations is not altered.
        if (!isAssociativitySafeExpression(origExprToCast)) {
            // TODO: unclear whether this matters with AST modifications/pretty printing,
            // but it's required for compatibility w/ original 'printer' method.
            EnclosedExpr newExpr = new EnclosedExpr(origExprToCast);
            nothingPrintedAssertion.run();
            n.setExpression(newExpr);
            origExprToCast.setParentNode(newExpr);
        }

        if (printer.hasStringBuilder()) {
            /* Print the cast normally - "(targetType) (expression)" */
            printer.append('(');
            if (ret.getPackage() != null && simpleNameWhiteList.contains(ret.getPackage().getName())) {
                printer.append(ret.getSimpleName());
            } else {
                printer.append(ret.getCanonicalName());
            }
            printer.append(')');
            printer.append(' ');

            n.getExpression().accept(this, printer);
        }

        return ret;
    }

    /**
     * Returns true if you can put an "!" in front of an expression to invert the whole thing instead of part. So
     * {@code true} for {@code myMethod("some", "args"} or {@code myVar}, {@code false} for {@code a&&b}. (Because
     * {@code !myMethod("some", "args")} or {@code !myVar} work fine, but {@code !a&&b} is very different from
     * {@code !(a&&b)}.)
     * 
     * @param expr
     * @return {@code true} if prepending a unary operator will apply the operator to the entire expression instead of
     *         just its first term.
     */
    private static boolean isAssociativitySafeExpression(Expression expr) {
        return (expr instanceof NameExpr) || (expr instanceof LiteralExpr) || (expr instanceof EnclosedExpr)
                || (expr instanceof MethodCallExpr);
    }

    /**
     * Checks whether the conversion from {@code original} to {@code target} is a widening primitive conversion. The
     * arguments must be primitive types (not boxed types).
     * <p>
     * This method return false if {@code original} and {@code target} represent the same type, as such a conversion is
     * the identity conversion, not a widening conversion.
     * <p>
     * See <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.1.2">the JLS</a> for more info.
     *
     * @param original The type to convert <b>from</b>.
     * @param target The type to convert <b>to</b>.
     * @return {@code true} if a conversion from {@code original} to {@code target} is a widening conversion; otherwise,
     *         {@code false}.
     */
    static boolean isWideningPrimitiveConversion(Class<?> original, Class<?> target) {
        if (original == null || !original.isPrimitive() || target == null || !target.isPrimitive()
                || original.equals(void.class) || target.equals(void.class)) {
            throw new IllegalArgumentException("Arguments must be a primitive type (excluding void)!");
        }

        LanguageParserPrimitiveType originalEnum = LanguageParserPrimitiveType.getPrimitiveType(original);

        switch (originalEnum) {
            case BytePrimitive:
                if (target == short.class)
                    return true;
            case ShortPrimitive:
            case CharPrimitive:
                if (target == int.class)
                    return true;
            case IntPrimitive:
                if (target == long.class)
                    return true;
            case LongPrimitive:
                if (target == float.class)
                    return true;
            case FloatPrimitive:
                if (target == double.class)
                    return true;
        }
        return false;
    }

    private enum LanguageParserPrimitiveType {
        // Including "Enum" (or really, any differentiating string) in these names is important. They're used
        // in a switch() statement, which apparently does not support qualified names. And we can't use
        // names that conflict with java.lang's boxed types.

        BytePrimitive(byte.class), ShortPrimitive(short.class), CharPrimitive(char.class), IntPrimitive(
                int.class), LongPrimitive(long.class), FloatPrimitive(
                        float.class), DoublePrimitive(double.class), BooleanPrimitive(boolean.class);

        private final Class<?> primitiveClass;

        LanguageParserPrimitiveType(Class<?> clazz) {
            primitiveClass = clazz;
        }

        private Class<?> getPrimitiveClass() {
            return primitiveClass;
        }

        private static final Map<Class<?>, LanguageParserPrimitiveType> primitiveClassToEnumMap = Stream
                .of(LanguageParserPrimitiveType.values())
                .collect(Collectors.toMap(LanguageParserPrimitiveType::getPrimitiveClass, Function.identity()));

        private static LanguageParserPrimitiveType getPrimitiveType(Class<?> original) {
            if (!original.isPrimitive()) {
                throw new IllegalArgumentException("Class " + original.getName() + " is not a primitive type");
            } else if (original.equals(void.class)) {
                throw new IllegalArgumentException("Void is not supported!");
            }

            LanguageParserPrimitiveType primitiveType = primitiveClassToEnumMap.get(original);
            Assert.neqNull(primitiveType, "primitiveType");
            return primitiveType;
        }
    }

    @Override
    public Class<?> visit(ClassOrInterfaceType n, VisitArgs printer) {
        Class<?> ret;
        final ClassOrInterfaceType scope = n.getScope().orElse(null);
        final String className = n.getNameAsString();

        // Note that we must pass a class name *excluding* generics to findClass()
        final String fullClassName = (scope != null ? scope.toString() + '.' : "") + className;

        // First and foremost: is 'n' a class name?
        if ((ret = findClass(fullClassName)) != null) {
            // n.toString() provides the full expression, *including* generics
            printer.append(n.toString());
            return ret;
        }

        // If not, 'className' should be a nested class of the scope type.
        if (scope != null) {
            Class<?> scopeClass = scope.accept(this, printer);
            if (scopeClass != null) {
                ret = findNestedClass(scopeClass, className);
                if (ret != null) {
                    printer.append('.');
                    printer.append(className);
                    return ret;
                }
            }
        }

        throw new ParserResolutionFailure("Cannot find class : " + className);
    }

    @Override
    public Class<?> visit(ArrayType n, VisitArgs printer) {
        Class<?> ret = n.getElementType().accept(this, printer);

        for (int i = 0; i < n.getArrayLevel(); i++) {
            printer.append("[]");
        }

        for (int i = 0; i < n.getArrayLevel(); i++) {
            ret = Array.newInstance(ret, 0).getClass();
        }

        return ret;
    }

    @Override
    public Class<?> visit(ConditionalExpr n, VisitArgs printer) {
        Class<?> classA = getTypeWithCaching(n.getThenExpr());
        Class<?> classB = getTypeWithCaching(n.getElseExpr());

        if (classA == NULL_CLASS && io.deephaven.util.type.TypeUtils.getUnboxedType(classB) != null) {
            n.setThenExpr(new NameExpr("NULL_" + io.deephaven.util.type.TypeUtils.getUnboxedType(classB)
                    .getSimpleName().toUpperCase()));
            classA = n.getThenExpr().accept(this, VisitArgs.WITHOUT_STRING_BUILDER);
        } else if (classB == NULL_CLASS && io.deephaven.util.type.TypeUtils.getUnboxedType(classA) != null) {
            n.setElseExpr(new NameExpr("NULL_" + TypeUtils.getUnboxedType(classA).getSimpleName().toUpperCase()));
            classB = n.getElseExpr().accept(this, VisitArgs.WITHOUT_STRING_BUILDER);
        }

        if (classA == boolean.class && classB == Boolean.class) {
            // a little hacky, but this handles the null case where it unboxes. very weird stuff
            final Expression uncastExpr = n.getThenExpr();
            final CastExpr castExpr = new CastExpr(new ClassOrInterfaceType("Boolean"), uncastExpr);
            n.setThenExpr(castExpr);
            // fix parent in uncastExpr (it is cleared when it is replaced with the CastExpr)
            uncastExpr.setParentNode(castExpr);
        }

        if (classA == Boolean.class && classB == boolean.class) {
            // a little hacky, but this handles the null case where it unboxes. very weird stuff
            final Expression uncastExpr = n.getElseExpr();
            final CastExpr castExpr = new CastExpr(new ClassOrInterfaceType("Boolean"), uncastExpr);
            n.setElseExpr(castExpr);
            // fix parent in uncastExpr (it is cleared when it is replaced with the CastExpr)
            uncastExpr.setParentNode(castExpr);
        }

        if (printer.hasStringBuilder()) {
            n.getCondition().accept(this, printer);
        }

        printer.append(" ? ");
        classA = n.getThenExpr().accept(this, printer);
        printer.append(" : ");
        classB = n.getElseExpr().accept(this, printer);

        boolean isAssignableFromA = dhqlIsAssignableFrom(classA, classB);
        boolean isAssignableFromB = dhqlIsAssignableFrom(classB, classA);

        if (isAssignableFromA && isAssignableFromB) {
            return classA.isPrimitive() ? classA : classB;
        } else if (isAssignableFromA) {
            return classA;
        } else if (isAssignableFromB) {
            return classB;
        }

        throw new IncompatibleTypesException(
                "Incompatible types in condition operation not supported : " + classA + ' ' + classB);
    }

    @Override
    public Class<?> visit(EnclosedExpr n, VisitArgs printer) {
        printer.append('(');
        Class<?> ret = n.getInner().accept(this, printer);
        printer.append(')');

        return ret;
    }

    @Override
    public Class<?> visit(FieldAccessExpr n, VisitArgs printer) {
        Class<?> ret; // the result type of this FieldAccessExpr (i.e. the type of the field)
        String exprString = n.toString();
        if ((ret = findClass(exprString)) != null) {
            // Before we do anything: just see if the entire expression is just a class name.
            printer.append(exprString);
            return ret;
        }

        Expression scopeExpr = n.getScope();
        String scopeName = scopeExpr.toString();
        String fieldName = n.getNameAsString();

        // A class name can also come through as a FieldAccessExpr (*not*, as one might expect,
        // as QualifiedNameExpr or ClassOrInterfaceType).
        // So if this FieldAccessExpr is:
        // com.a.b.TheClass.field
        // then the scope -- "com.a.b.TheClass" -- is itself a FieldAccessExpr.
        //
        // Thus we can use scopeExpr.accept() to find the scope type if the scope is anything other than a class,
        // but we would recurse and eventually fail if the scope name actually is a class. Instead, we must
        // manually check whether the scope is a class.
        Class<?> scopeType;
        if (scopeExpr instanceof FieldAccessExpr
                && (scopeType = findClass(scopeName)) != null) {
            // 'scope' was a class, and we found it - print 'scopeType' ourselves
            printer.append(scopeName);
        } else { // 'scope' was *not* a class; call accept() on it to print it and find its type.
            try {
                // The incoming VisitArgs might have a "casting context", meaning that it wants us to cast to
                // the proper type at the end. But we have a scope, and that scope needs to be evaluated in
                // a non-casting context. So we provide that here.
                scopeType = scopeExpr.accept(this, printer.cloneWithCastingContext(null));
            } catch (RuntimeException e) {
                throw new ParserResolutionFailure("Cannot resolve scope." +
                        "\n    Expression : " + exprString +
                        "\n    Scope      : " + scopeExpr +
                        "\n    Field Name : " + fieldName, e);
            }
            Assert.neqNull(scopeType, "scopeType");
        }

        if (scopeType.isArray() && fieldName.equals("length")) {
            // We need special handling for arrays -- see the note in the javadocs for Class.getField():
            // "If this Class object represents an array type, then this method
            // does not find the length field of the array type."
            ret = Integer.TYPE;
        } else {
            // If it's not an array, first check whether the 'field' is actually just the name of
            // a nested class.
            ret = findNestedClass(scopeType, fieldName);

            // If it wasn't a nested class, then it should be an actual field.
            if (ret == null) {
                try {
                    // For Python object, the type of the field is PyObject by default, the actual data type if
                    // primitive will only be known at runtime
                    if (scopeType == PyObject.class) {
                        ret = PyObject.class;
                    } else {
                        ret = scopeType.getField(fieldName).getType();
                    }
                } catch (NoSuchFieldException e) {
                    // And if we still can't find the field, we have a problem.
                    throw new ParserResolutionFailure("Cannot resolve field name." +
                            "\n    Expression : " + exprString +
                            "\n    Scope      : " + scopeExpr +
                            "\n    Scope Type : " + scopeType.getCanonicalName() +
                            "\n    Field Name : " + fieldName, e);
                }
            }
        }

        if (ret == PyObject.class) {
            // This is a direct field access on a Python object which is wrapped in PyObject.class
            // and must be accessed through PyObject.getAttribute() method
            printer.append('.').append("getAttribute(\"" + n.getNameAsString() + "\"");

            final NodeList<Expression> getAttributeArgs = new NodeList<>();
            getAttributeArgs.add(new StringLiteralExpr(n.getNameAsString()));

            if (printer.pythonCastContext != null) {
                // The to-be-cast expr is a Python object field accessor
                final String clsName = printer.pythonCastContext.getSimpleName();
                printer.append(", " + clsName + ".class");

                final ClassExpr targetType =
                        new ClassExpr(new ClassOrInterfaceType(null, printer.pythonCastContext.getSimpleName()));
                getAttributeArgs.add(targetType);

                // Let's advertise to the caller the cast context type
                ret = printer.pythonCastContext;
            }
            printer.append(')');

            // replace the field access with the getAttribute() call
            // we do not need to visit() (since we already visited the scope)
            final MethodCallExpr pyGetAttributeMethodCall =
                    new MethodCallExpr(n.getScope(), "getAttribute", getAttributeArgs);
            replaceChildExpression(
                    n.getParentNode().orElseThrow(),
                    n,
                    pyGetAttributeMethodCall);
        } else {
            printer.append('.').append(n.getNameAsString());
        }
        return ret;
    }

    // ---------- LITERALS: ----------

    @Override
    public Class<?> visit(CharLiteralExpr n, VisitArgs printer) {
        printer.append('\'');
        printer.append(n.getValue());
        printer.append('\'');

        return char.class;
    }

    @Override
    public Class<?> visit(DoubleLiteralExpr n, VisitArgs printer) {
        String value = n.getValue();
        printer.append(value);
        if (value.charAt(value.length() - 1) == 'f') {
            return float.class;
        }

        return double.class;
    }

    @Override
    public Class<?> visit(IntegerLiteralExpr n, VisitArgs printer) {
        final Runnable nothingPrintedAssertion = getNothingPrintedAssertion(printer);
        String origValue = n.getValue();

        /*
         * In java, you can't compile if your code contains an integer literal that's too big to fit in an int. You'd
         * need to add an "L" to the end, to indicate that it's a long.
         *
         * But in the engine, we assume you don't mind extra precision and just want your query to work, so when an
         * 'integer' literal is too big to fit in an int, we automatically add on the "L" to promote the literal from an
         * int to a long.
         *
         * Also, note that the 'x' and 'b' for hexadecimal/binary literals are _not_ case-sensitive.
         */

        // First, we need to remove underscores from the value before we can parse it.
        final String value = origValue.chars()
                .filter((c) -> c != '_')
                .collect(StringBuilder::new, (sb, c) -> sb.append((char) c), StringBuilder::append)
                .toString();

        long longValue;
        String prefix = value.length() > 2 ? value.substring(0, 2) : null;
        if ("0x".equalsIgnoreCase(prefix)) { // hexadecimal literal
            longValue = Long.parseLong(value.substring(2), 16);
        } else if ("0b".equalsIgnoreCase(prefix)) { // binary literal
            // If a literal has 32 bits, the 32nd (i.e. MSB) is *not* taken as the sign bit!
            // This follows from the fact that Integer.parseInt(str, 2) will only parse an 'str' up to 31 chars long.
            longValue = Long.parseLong(value.substring(2), 2);
        } else { // regular numeric literal
            longValue = Long.parseLong(value);
        }

        if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
            LongLiteralExpr newExpr = new LongLiteralExpr(origValue + 'L');
            replaceChildExpression(n.getParentNode().orElseThrow(), n, newExpr);
            nothingPrintedAssertion.run();
            return newExpr.accept(this, printer);
        }

        printer.append(origValue);

        return int.class;
    }

    @Override
    public Class<?> visit(LongLiteralExpr n, VisitArgs printer) {
        printer.append(n.getValue());

        return long.class;
    }

    @Override
    public Class<?> visit(StringLiteralExpr n, VisitArgs printer) {
        printer.append('"');
        printer.append(n.getValue());
        printer.append('"');

        return String.class;
    }

    @Override
    public Class<?> visit(BooleanLiteralExpr n, VisitArgs printer) {
        printer.append(String.valueOf(n.getValue()));

        return boolean.class;
    }

    @Override
    public Class<?> visit(NullLiteralExpr n, VisitArgs printer) {
        printer.append("null");

        return NULL_CLASS;
    }

    // ---------- MISC: ----------

    @Override
    public Class<?> visit(MethodCallExpr n, VisitArgs printer) {

        final VisitArgs scopePrinter = VisitArgs.create();

        Class<?> scope = n.getScope().map(sc -> {
            Class<?> result = sc.accept(this, scopePrinter);
            scopePrinter.append('.');
            return result;
        }).orElse(null);

        Expression[] expressions = n.getArguments() == null ? new Expression[0]
                : n.getArguments().toArray(new Expression[0]);

        Class<?>[] expressionTypes = printArguments(expressions, VisitArgs.WITHOUT_STRING_BUILDER);

        Class<?>[][] parameterizedTypes = getParameterizedTypes(expressions);

        final String methodName = n.getNameAsString();
        Method method = getMethod(scope, methodName, expressionTypes, parameterizedTypes);


        // TODO: we can swap out method calls for better ones here, e.g. python functions to Java ones, or
        // replacing boxed stuff with primitives (e.g. DateTime methods), etc.
        // if(methodToOptimizerMap.containsKey(method)) {
        // methodToOptimizerMap.get(method).apply(n, printer);
        // }

        Class<?>[] argumentTypes = method.getParameterTypes();

        // now do some parameter conversions...
        if (n.getComment().orElse(null) != QueryLanguageParserComment.NO_PARAMETER_REWRITING_FLAG) {
            expressions = convertParameters(method, argumentTypes, expressionTypes, parameterizedTypes, expressions);
        }
        n.setArguments(NodeList.nodeList(expressions));

        if (isPotentialImplicitCall(method.getDeclaringClass())) {
            if (scope == null) { // python func call or Groovy closure call
                /*
                 * @formatter:off
                 * python func call
                 * 1. the func is defined at the main module level and already wrapped in CallableWrapper
                 * 2. the func will be called via CallableWrapper.call() method
                 * @formatter:on
                 */
                Assert.eqZero(scopePrinter.builder.length(), "scopePrinter.builder.length()");

                final MethodCallExpr callMethodCall =
                        new MethodCallExpr(n.getNameAsExpression(), "call", n.getArguments());

                replaceChildExpression(
                        n.getParentNode().orElseThrow(),
                        n,
                        callMethodCall);

                return callMethodCall.accept(this, printer);
            } else {
                /*
                 * @formatter:off
                 * python method call
                 * 1. need to reference the method with PyObject.getAttribute();
                 * 2. wrap the method reference in CallableWrapper()
                 * 3. the method will be called via CallableWrapper.call()
                 *
                 * So we are going from:
                 * 'myPyObj.myPyMethod(a, b, c)'
                 * to:
                 * '(new io.deephaven.engine.util.PyCallableWrapper(myPyObj.getAttribute("myPyMethod"))).call(a, b, c)'
                 *
                 * @formatter:on
                 */


                final boolean isExplicitCall = methodName.equals("call")
                        || PyObject.class.isAssignableFrom(scope) && methodName.equals("getAttribute");
                if (isExplicitCall) {
                    printer.append(scopePrinter);
                    printer.append(methodName);
                } else {
                    final MethodCallExpr getAttributeCall = new MethodCallExpr(
                            n.getScope().orElseThrow(),
                            "getAttribute",
                            NodeList.nodeList(new StringLiteralExpr(methodName)));


                    final ObjectCreationExpr newPyCallableExpr = new ObjectCreationExpr(
                            null,
                            new ClassOrInterfaceType("io.deephaven.engine.util.PyCallableWrapper"),
                            NodeList.nodeList(getAttributeCall));

                    final MethodCallExpr callMethodCall = new MethodCallExpr(
                            new EnclosedExpr(newPyCallableExpr),
                            "call",
                            n.getArguments());

                    // Replace the original method call we were visit()ing with the new one
                    replaceChildExpression(
                            n.getParentNode().orElseThrow(),
                            n,
                            callMethodCall);

                    // Determine return type by visiting the new method call
                    return callMethodCall.accept(this, printer);
                }
            }
        } else { // Groovy or Java method call (or explicit python call)
            printer.append(scopePrinter);
            printer.append(methodName);
        }

        Class<?>[] argTypes = printArguments(expressions, printer);

        // Python function call vectorization
        if (PyCallableWrapper.class.equals(scope)) {
            vectorizePythonCallable(n, scope, expressions, argTypes);
        }

        return calculateMethodReturnTypeUsingGenerics(scope, n.getScope().orElse(null), method, expressionTypes,
                parameterizedTypes);
    }

    private void vectorizePythonCallable(MethodCallExpr n, Class<?> scopeType, Expression[] argExpressions,
            Class<?>[] argTypes) {
        final String invokedMethodName = n.getNameAsString();

        // assertions related to when the parser should even attempt vectorization:
        Assert.equals(scopeType, "scopeType", PyCallableWrapper.class, "PyCallableWrapper.class");
        Assert.equals(invokedMethodName, "invokedMethodName", "call");

        final String pyMethodName = n.getScope().orElseThrow().toString();

        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        final Object paramValueRaw = queryScope.readParamValue(pyMethodName, null);
        if (paramValueRaw == null) {
            throw new IllegalStateException("Resolved Python function name " + pyMethodName + " not found");
        }
        if (!(paramValueRaw instanceof PyCallableWrapper)) {
            throw new IllegalStateException("Resolved Python function name " + pyMethodName + " not callable");
        }
        final PyCallableWrapper pyCallableWrapper = (PyCallableWrapper) paramValueRaw;

        prepareVectorization(n, argExpressions, pyCallableWrapper);
        if (pyCallableWrapper.isVectorizable()) {
            prepareVectorizationArgs(n, queryScope, argExpressions, argTypes, pyCallableWrapper);
        }
    }

    private void prepareVectorization(MethodCallExpr n, Expression[] expressions, PyCallableWrapper pyCallableWrapper) {
        try {
            checkVectorizability(n, expressions, pyCallableWrapper);
            pyCallableWrapper.setVectorizable(true);
            if (!pyCallableWrapper.isVectorized() && log.isDebugEnabled()) {
                log.debug().append("Python function call ").append(n.toString()).append(" is auto-vectorizable")
                        .endl();
            }
        } catch (RuntimeException ex) {
            // if the Callable is already vectorized (decorated with numba.vectorized or dh_vectorized),
            // then the exception is fatal, otherwise it is an un-vectorized Callable and will remain as such
            if (pyCallableWrapper.isVectorized()) {
                throw ex;
            }
            if (log.isDebugEnabled()) {
                log.debug().append("Python function call ").append(n.toString()).append(" is not auto-vectorizable:")
                        .append(ex.getMessage()).endl();
            }
        }
    }

    private void checkVectorizability(MethodCallExpr n, Expression[] expressions, PyCallableWrapper pyCallableWrapper) {

        pyCallableWrapper.parseSignature();

        // Python vectorized functions(numba, DH) return arrays of primitive/Object types. This will break the generated
        // expression evaluation code that expects singular values. This check makes sure that numba/dh vectorized
        // functions must be used alone as the entire expression.
        n.getParentNode().ifPresent(parent -> {
            if (parent.getClass() == CastExpr.class) {
                throw new PythonCallVectorizationFailure(
                        "The return values of Python vectorized function can't be cast: " + parent);
            }
            if (!WrapperNode.class.equals(parent.getClass())) {
                throw new PythonCallVectorizationFailure(
                        "Python vectorized function can't be used in another expression: " + parent);
            }
        });

        for (int i = 0; i < expressions.length; i++) {
            if (!(expressions[i] instanceof NameExpr) && !(expressions[i] instanceof LiteralExpr)) {
                throw new PythonCallVectorizationFailure("Invalid argument at index " + i
                        + ": Python vectorized function arguments can only be columns, variables, and constants: "
                        + expressions[i]);
            }
        }
    }

    private void prepareVectorizationArgs(MethodCallExpr n, QueryScope queryScope, Expression[] expressions,
            Class<?>[] argTypes,
            PyCallableWrapper pyCallableWrapper) {
        List<Class<?>> paramTypes = pyCallableWrapper.getParamTypes();
        if (paramTypes.size() != expressions.length) {
            throw new PythonCallVectorizationFailure("Python function argument count mismatch: " + n + " "
                    + paramTypes.size() + " vs. " + expressions.length);
        }

        pyCallableWrapper.initializeChunkArguments();
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i] instanceof LiteralExpr) {
                addLiteralArg(expressions[i], argTypes[i], pyCallableWrapper);
            } else if (expressions[i] instanceof NameExpr) {
                String name = expressions[i].asNameExpr().getNameAsString();
                try {
                    Object param = queryScope.readParamValue(name);
                    pyCallableWrapper.addChunkArgument(new ConstantChunkArgument(param, argTypes[i]));
                } catch (QueryScope.MissingVariableException ex) {
                    // A column name or one of the special variables
                    pyCallableWrapper.addChunkArgument(new ColumnChunkArgument(name, argTypes[i]));
                }
            } else {
                throw new IllegalStateException("Vectorizability check failed: " + n);
            }

            if (!isSafelyCoerceable(argTypes[i], paramTypes.get(i))) {
                throw new PythonCallVectorizationFailure("Python vectorized function argument type mismatch: " + n + " "
                        + argTypes[i].getSimpleName() + " -> " + paramTypes.get(i).getSimpleName());
            }
        }
    }

    private void addLiteralArg(Expression expression, Class<?> argType, PyCallableWrapper pyCallableWrapper) {
        if (argType == long.class) {
            pyCallableWrapper
                    .addChunkArgument(new ConstantChunkArgument(((LongLiteralExpr) expression).asNumber().longValue(),
                            long.class));
        } else if (argType == int.class) {
            pyCallableWrapper.addChunkArgument(
                    new ConstantChunkArgument(((IntegerLiteralExpr) expression).asNumber().intValue(), int.class));
        } else if (argType == boolean.class) {
            pyCallableWrapper.addChunkArgument(
                    new ConstantChunkArgument(((BooleanLiteralExpr) expression).getValue(), boolean.class));
        } else if (argType == String.class) {
            pyCallableWrapper.addChunkArgument(
                    new ConstantChunkArgument(((StringLiteralExpr) expression).getValue(), String.class));
        } else if (argType == float.class) {
            pyCallableWrapper.addChunkArgument(
                    new ConstantChunkArgument((Float.parseFloat(((DoubleLiteralExpr) expression).getValue())),
                            float.class));
        } else if (argType == double.class) {
            pyCallableWrapper.addChunkArgument(
                    new ConstantChunkArgument(((DoubleLiteralExpr) expression).asDouble(), double.class));
        } else if (argType == NULL_CLASS) {
            pyCallableWrapper.addChunkArgument(new ConstantChunkArgument(null, NULL_CLASS));
        } else if (argType == char.class) {
            pyCallableWrapper
                    .addChunkArgument(new ConstantChunkArgument(((CharLiteralExpr) expression).getValue(), char.class));
        } else {
            throw new IllegalStateException("Unrecognized literal expression type: " + argType);
        }
    }

    private static boolean isSafelyCoerceable(Class<?> expressionType, Class<?> aClass) {
        // TODO (core#709): numba does appear to check for type coercing at runtime, though no explicit rules exist
        // also the dh_vectorize is type-blind for now and simply calls the wrapped function with the provided data.
        return true;
    }

    @Override
    public Class<?> visit(ExpressionStmt n, VisitArgs printer) {
        Class<?> ret = n.getExpression().accept(this, printer);
        printer.append(';');
        return ret;
    }

    @Override
    public Class<?> visit(ObjectCreationExpr n, VisitArgs printer) {
        printer.append("new ");

        Class<?> ret = n.getType().accept(this, printer);

        Expression[] expressions = n.getArguments() == null ? new Expression[0]
                : n.getArguments().toArray(new Expression[0]);

        Class<?>[] expressionTypes = printArguments(expressions, VisitArgs.WITHOUT_STRING_BUILDER);

        Class<?>[][] parameterizedTypes = getParameterizedTypes(expressions);

        Constructor<?> constructor = getConstructor(ret, expressionTypes, parameterizedTypes);

        Class<?>[] argumentTypes = constructor.getParameterTypes();

        // now do some parameter conversions...

        expressions = convertParameters(constructor, argumentTypes, expressionTypes, parameterizedTypes, expressions);
        n.setArguments(NodeList.nodeList(expressions));

        if (printer.hasStringBuilder()) {
            printArguments(expressions, printer);
        }

        return ret;
    }

    @Override
    public Class<?> visit(ArrayCreationExpr n, VisitArgs printer) {
        printer.append("new ");

        Class<?> ret = n.getElementType().accept(this, printer);

        for (ArrayCreationLevel dim : n.getLevels()) {
            printer.append('[');
            dim.accept(this, printer);
            printer.append(']');

            ret = Array.newInstance(ret, 0).getClass();
        }

        n.getInitializer().ifPresent(init -> {
            printer.append(' ');
            init.accept(this, printer);
        });
        return ret;
    }

    @Override
    public Class<?> visit(ArrayInitializerExpr n, VisitArgs printer) {
        printer.append('{');
        if (n.getValues() != null) {
            printer.append(' ');
            for (Iterator<Expression> i = n.getValues().iterator(); i.hasNext();) {
                Expression expr = i.next();
                expr.accept(this, printer);
                if (i.hasNext()) {
                    printer.append(", ");
                }
            }
            printer.append(' ');
        }
        printer.append('}');

        return null;
    }

    @Override
    public Class<?> visit(ClassExpr n, VisitArgs printer) {
        Class<?> type = n.getType().accept(this, printer);
        printer.append(".class");
        // noinspection ClassGetClass
        return type.getClass();
    }

    // ---------- METHOD REFERENCES: ----------

    @Override
    public Class<?> visit(TypeExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("TypeExpr Operation not supported");
        // return n.getType().accept(this, printer);
    }

    @Override
    public Class<?> visit(MethodReferenceExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("MethodReferenceExpr Operation not supported");

        // Expression scope = n.getScope();
        // Class scopeType = scope.accept(this, printer);
        //
        // String methodName = n.getIdentifier();
        //
        // /*
        // NOTE: I believe the big problem here is knowing how many arguments to expect the referenced method to take.
        // Seems like we'll have to search parent nodes to find the context in which this method reference is used
        // */
        //
        // Method[] possibleReferredMethods = Stream
        // .of(scopeType.getMethods())
        // .filter((method) -> method.getName().equals(methodName))
        // .toArray(Method[]::new);
        //
        // if(n.getParentNode() instanceof MethodCallExpr) {
        // MethodCallExpr parent = (MethodCallExpr) n.getParentNode();
        // int argIndex = parent.getArgs().indexOf(n);
        //
        // Class parentScope = (Class) parent.getData();
        //
        // // Possible methods to which this MethodReferenceExpr is an argument
        // Method[] candidateCalledMethods =
        // // Get all possible methods
        // (parentScope == null
        // ? staticImports.stream().map(Class::getDeclaredMethods).map(Stream::of).flatMap(Function.identity())
        // : Stream.of(parentScope.getMethods())
        // )
        // .filter((m) -> m.getParameterCount() == parent.getArgs().size()) // filter based on argument count
        // .filter((m) -> m.getName().equals(methodName)) // filter based on name
        // .toArray(Method[]::new);
        //
        // StringBuilder tempPrinter = new StringBuilder();
        // new MethodCallExpr(parent.getScope(), parent).ac
        // // .filter((m) -> m.getParameterTypes()[argIndex].dhqlIsAssignableFrom())
        //
        // // so, have to find out what kind of argument we need,
        // // and what kind of return types [scope]::[methodName] could possibly
        // // provide.
        // }
        //
        //
        //
        // // Also...what to do with the type parameters? n.getTypeParameters()?
        //
        // /* If the method identifier is 'new' (and the scope type has a public constructor),
        // then the return type is an instance of this object */
        //
        // if(methodName.equals("new")) {
        // if (Stream
        // .of(scopeType.getConstructors())
        // .filter((c) -> (c.getModifiers() | Modifier.PUBLIC) > 0)
        // .count() > 0) {
        // printer.append("::");
        // printer.append("new");
        // return scopeType;
        // }
        // throw new RuntimeException("No public constructor available: " + n);
        // }
        //
        // Method m = Stream.of(scopeType.getMethods())
        // .filter((method) -> method.getName().equals(methodName))
        // .collect(Collectors.toMap(Method::getName, Function.identity()))
        // .get(methodName);
        //
        // if(m == null) {
        // throw new RuntimeException("Could not find method \"" + methodName + "\": " + n.toString());
        // } else {
        // printer.append("::");
        // printer.append(n.getIdentifier());
        // return m.getReturnType();
        // }
    }

    // ---------- UNSUPPORTED: ----------

    @Override
    public Class<?> visit(AnnotationDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("AnnotationDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(AnnotationMemberDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("AnnotationMemberDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(AssertStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("AssertStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(AssignExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("AssignExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(BlockComment n, VisitArgs printer) {
        throw new UnsupportedOperationException("BlockComment Operation not supported!");
    }

    @Override
    public Class<?> visit(BlockStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("BlockStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(BreakStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("BreakStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(CatchClause n, VisitArgs printer) {
        throw new UnsupportedOperationException("CatchClause Operation not supported!");
    }

    @Override
    public Class<?> visit(ClassOrInterfaceDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("ClassOrInterfaceDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(CompilationUnit n, VisitArgs printer) {
        throw new UnsupportedOperationException("CompilationUnit Operation not supported!");
    }

    @Override
    public Class<?> visit(ConstructorDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("ConstructorDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(ContinueStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("ContinueStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(DoStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("DoStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(EmptyStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("EmptyStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(EnumConstantDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("EnumConstantDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(EnumDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("EnumDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(ExplicitConstructorInvocationStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("ExplicitConstructorInvocationStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(FieldDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("FieldDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(ForStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("ForStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(IfStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("IfStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(ImportDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("ImportDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(InitializerDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("InitializerDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(InstanceOfExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("InstanceOfExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(JavadocComment n, VisitArgs printer) {
        throw new UnsupportedOperationException("JavadocComment Operation not supported!");
    }

    @Override
    public Class<?> visit(LabeledStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("LabeledStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(LambdaExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("LambdaExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(LineComment n, VisitArgs printer) {
        throw new UnsupportedOperationException("LineComment Operation not supported!");
    }

    @Override
    public Class<?> visit(MarkerAnnotationExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("MarkerAnnotationExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(MemberValuePair n, VisitArgs printer) {
        throw new UnsupportedOperationException("MemberValuePair Operation not supported!");
    }

    @Override
    public Class<?> visit(MethodDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("MethodDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(NormalAnnotationExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("NormalAnnotationExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(PackageDeclaration n, VisitArgs printer) {
        throw new UnsupportedOperationException("PackageDeclaration Operation not supported!");
    }

    @Override
    public Class<?> visit(Parameter n, VisitArgs printer) {
        throw new UnsupportedOperationException("Parameter Operation not supported!");
    }

    @Override
    public Class<?> visit(ReturnStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("ReturnStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(SingleMemberAnnotationExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("SingleMemberAnnotationExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(SuperExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("SuperExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(SwitchStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("SwitchStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(SynchronizedStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("SynchronizedStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(ThisExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("ThisExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(ThrowStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("ThrowStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(TryStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("TryStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(TypeParameter n, VisitArgs printer) {
        throw new UnsupportedOperationException("TypeParameter Operation not supported!");
    }

    @Override
    public Class<?> visit(VariableDeclarationExpr n, VisitArgs printer) {
        throw new UnsupportedOperationException("VariableDeclarationExpr Operation not supported!");
    }

    @Override
    public Class<?> visit(VariableDeclarator n, VisitArgs printer) {
        throw new UnsupportedOperationException("VariableDeclarator Operation not supported!");
    }

    @Override
    public Class<?> visit(VoidType n, VisitArgs printer) {
        throw new UnsupportedOperationException("VoidType Operation not supported!");
    }

    @Override
    public Class<?> visit(WhileStmt n, VisitArgs printer) {
        throw new UnsupportedOperationException("WhileStmt Operation not supported!");
    }

    @Override
    public Class<?> visit(WildcardType n, VisitArgs printer) {
        throw new UnsupportedOperationException("WildcardType Operation not supported!");
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    public static class Result {
        private final Class<?> type;
        private final String source;
        private final HashSet<String> variablesUsed;

        Result(Class<?> type, String source, HashSet<String> variablesUsed) {
            this.type = type;
            this.source = source;
            this.variablesUsed = variablesUsed;
        }

        public Class<?> getType() {
            return type;
        }

        public String getConvertedExpression() {
            return source;
        }

        public HashSet<String> getVariablesUsed() {
            return variablesUsed;
        }
    }

    public static class VisitArgs {
        public static VisitArgs WITHOUT_STRING_BUILDER = new VisitArgs(null, null);

        public static VisitArgs create() {
            return new VisitArgs(new StringBuilder(), null);
        }

        public VisitArgs cloneWithCastingContext(Class<?> pythonCastContext) {
            return new VisitArgs(builder, pythonCastContext);
        }

        /**
         * Underlying StringBuilder or 'null' if we don't need a buffer (i.e. if we are just running the visitor pattern
         * to calculate a type and don't care about side effects.
         */
        private final StringBuilder builder;
        private final Class<?> pythonCastContext;

        private VisitArgs(StringBuilder builder, Class<?> pythonCastContext) {
            this.builder = builder;
            this.pythonCastContext = pythonCastContext;
        }

        public boolean hasStringBuilder() {
            return builder != null;
        }

        /**
         * Convenience method: forwards argument to 'builder' if 'builder' is not null
         */
        public VisitArgs append(String s) {
            if (hasStringBuilder()) {
                builder.append(s);
            }
            return this;
        }

        /**
         * Convenience method: forwards argument to 'builder' if 'builder' is not null
         */
        public VisitArgs append(char c) {
            if (hasStringBuilder()) {
                builder.append(c);
            }
            return this;
        }

        /**
         * Convenience method: forwards argument to 'builder' if 'builder' is not null
         */
        public VisitArgs append(VisitArgs va) {
            if (hasStringBuilder() && va.hasStringBuilder()) {
                builder.append(va.builder);
            }
            return this;
        }
    }

    /**
     * These are comments that are used as internal flags to control the parser's behavior. (Comments are not supported
     * within DHQL expressions anyway.)
     */
    private static class QueryLanguageParserComment extends Comment {

        /**
         * This "comment" is a flag used to indicate that a logical complement expr (e.g. "!(a==b)") has been inserted
         * by the parser and should *NOT* be replaced with an operator overload. In other words, it's used to make sure
         * "a!=b" is transformed first into "!(a==b)" and then into "!(equals(a, b))", rather than being transformed
         * into "not(equals(a, b))".
         */
        private static final QueryLanguageParserComment BINARY_OP_NEQ_CONVERSION_FLAG =
                new QueryLanguageParserComment("NODE NEEDS LOGICAL COMPLEMENT");

        /**
         * This "comment" is a flag used to indicate that {@link #convertParameters} should be skipped for a method
         * call. This is used to ensure that {@code (int) myInteger} is rewritten to {@code intCast(myInteger)} and not
         * {@code intCast(myInteger.longValue()}, which is what normally would happen because the parser will prefer a
         * method call with primitive arguments over a method call with reference arguments when possible.
         */
        private static final QueryLanguageParserComment NO_PARAMETER_REWRITING_FLAG =
                new QueryLanguageParserComment("ARGUMENTS MUST NOT BE MODIFIED");

        private QueryLanguageParserComment(String content) {
            super(content);
        }

        @Override
        public <R, A> R accept(GenericVisitor<R, A> v, A arg) {
            return null;
        }

        @Override
        public <A> void accept(VoidVisitor<A> v, A arg) {}
    }

    private static class WrapperNode extends Node {

        Node wrappedNode;

        protected WrapperNode(Expression origExpr) {
            super(origExpr.getTokenRange().orElse(null));
            wrappedNode = origExpr;
        }

        @Override
        public <R, A> R accept(GenericVisitor<R, A> v, A arg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A> void accept(VoidVisitor<A> v, A arg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean replace(Node node, Node replacementNode) {
            if (wrappedNode != node) {
                return false;
            }

            wrappedNode.setParentNode(null);
            wrappedNode = replacementNode;
            replacementNode.setParentNode(this);

            setTokenRange(replacementNode.getTokenRange().orElse(null));

            return true;
        }
    }

    private static class DummyExpr extends Expression {
        protected DummyExpr() {
            super(TokenRange.INVALID);
        }

        @Override
        public <R, A> R accept(GenericVisitor<R, A> v, A arg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A> void accept(VoidVisitor<A> v, A arg) {
            throw new UnsupportedOperationException();
        }
    }

    private static class ParserVerificationFailure extends RuntimeException {
        public ParserVerificationFailure(String message) {
            super(message);
        }

        public ParserVerificationFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class ParserResolutionFailure extends RuntimeException {
        public ParserResolutionFailure(String message) {
            super(message);
        }

        public ParserResolutionFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class IncompatibleTypesException extends RuntimeException {
        public IncompatibleTypesException(String message) {
            super(message);
        }
    }

    private static class PythonCallVectorizationFailure extends RuntimeException {
        public PythonCallVectorizationFailure(String message) {
            super(message);
        }
    }
}
