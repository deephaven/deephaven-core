//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.TokenRange;
import com.github.javaparser.ast.*;
import com.github.javaparser.ast.body.AnnotationDeclaration;
import com.github.javaparser.ast.body.AnnotationMemberDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.EnumConstantDeclaration;
import com.github.javaparser.ast.body.EnumDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.InitializerDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.comments.BlockComment;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.comments.LineComment;
import com.github.javaparser.ast.expr.ArrayAccessExpr;
import com.github.javaparser.ast.expr.ArrayCreationExpr;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.BooleanLiteralExpr;
import com.github.javaparser.ast.expr.CastExpr;
import com.github.javaparser.ast.expr.CharLiteralExpr;
import com.github.javaparser.ast.expr.ClassExpr;
import com.github.javaparser.ast.expr.ConditionalExpr;
import com.github.javaparser.ast.expr.DoubleLiteralExpr;
import com.github.javaparser.ast.expr.EnclosedExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.InstanceOfExpr;
import com.github.javaparser.ast.expr.IntegerLiteralExpr;
import com.github.javaparser.ast.expr.LambdaExpr;
import com.github.javaparser.ast.expr.LiteralExpr;
import com.github.javaparser.ast.expr.LongLiteralExpr;
import com.github.javaparser.ast.expr.MarkerAnnotationExpr;
import com.github.javaparser.ast.expr.MemberValuePair;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.MethodReferenceExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.NormalAnnotationExpr;
import com.github.javaparser.ast.expr.NullLiteralExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.SingleMemberAnnotationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.expr.SuperExpr;
import com.github.javaparser.ast.expr.ThisExpr;
import com.github.javaparser.ast.expr.TypeExpr;
import com.github.javaparser.ast.expr.UnaryExpr;
import com.github.javaparser.ast.expr.VariableDeclarationExpr;
import com.github.javaparser.ast.stmt.AssertStmt;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.BreakStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ContinueStmt;
import com.github.javaparser.ast.stmt.DoStmt;
import com.github.javaparser.ast.stmt.EmptyStmt;
import com.github.javaparser.ast.stmt.ExplicitConstructorInvocationStmt;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.ForStmt;
import com.github.javaparser.ast.stmt.IfStmt;
import com.github.javaparser.ast.stmt.LabeledStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.stmt.SwitchStmt;
import com.github.javaparser.ast.stmt.SynchronizedStmt;
import com.github.javaparser.ast.stmt.ThrowStmt;
import com.github.javaparser.ast.stmt.TryStmt;
import com.github.javaparser.ast.stmt.WhileStmt;
import com.github.javaparser.ast.type.ArrayType;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.PrimitiveType;
import com.github.javaparser.ast.type.TypeParameter;
import com.github.javaparser.ast.type.VoidType;
import com.github.javaparser.ast.type.WildcardType;
import com.github.javaparser.ast.visitor.GenericVisitorAdapter;
import com.github.javaparser.ast.visitor.GenericVisitor;
import com.github.javaparser.ast.visitor.VoidVisitor;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;
import groovy.lang.Closure;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.ShiftedColumnsFactory;
import io.deephaven.engine.util.PyCallableWrapper;
import io.deephaven.engine.util.PyCallableWrapper.ColumnChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ConstantChunkArgument;
import io.deephaven.engine.util.PyCallableWrapperJpyImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.TimeLiteralReplacedExpression;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.jpy.PyObject;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class QueryLanguageParser extends GenericVisitorAdapter<Class<?>, QueryLanguageParser.VisitArgs> {
    /**
     * Verify that the source code obtained from printing the AST is the same as the source code produced by the
     * original technique of writing code to a StringBuilder while visiting nodes.
     */
    private static final boolean VERIFY_AST_CHANGES = true;

    private static final Logger log = LoggerFactory.getLogger(QueryLanguageParser.class);
    private static final String GET_ATTRIBUTE_METHOD_NAME = "getAttribute";
    private final Collection<Package> packageImports;
    private final Collection<Class<?>> classImports;
    private final Collection<Class<?>> staticImports;

    private final Map<String, Class<?>> variables;
    private final Map<String, Class<?>[]> variableTypeArguments;
    private final Map<String, Object> queryScopeVariables;
    private final Set<String> columnVariables;

    private final HashSet<String> variablesUsed = new HashSet<>();

    private final Map<String, Class<?>> nameLookupCache = new HashMap<>();

    private final Map<String, Class<?>> staticImportLookupCache = new HashMap<>();

    // We need some class to represent null. We know for certain that this one won't be used...
    public static final Class<?> NULL_CLASS = QueryLanguageParser.class;

    /**
     * The result of the QueryLanguageParser for the expression passed given to the constructor.
     */
    private final Result result;

    private final HashMap<Node, Class<?>> cachedTypes = new HashMap<>();
    private final boolean unboxArguments;
    private static final Method PY_CALLABLE_WRAPPER_CALL_METHOD;

    static {
        try {
            PY_CALLABLE_WRAPPER_CALL_METHOD = PyCallableWrapper.class.getMethod("call", Object[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not locate PyCallableWrapper.call() method", e);
        }
    }


    @NotNull
    private final String pyCallableWrapperImplName;

    /**
     * Create a QueryLanguageParser and parse the given {@code expression}. After construction, the
     * {@link QueryLanguageParser.Result result} of parsing the {@code expression} is available with the
     * {@link #getResult()}} method.
     * <p>
     * Note that the provided Collections and Maps must not be mutated concurrently with or after construction.
     *
     * @param expression The query language expression to parse
     * @param packageImports Wildcard package imports
     * @param classImports Individual class imports
     * @param staticImports Wildcard static imports. All static variables and methods for the given classes are
     *        imported.
     * @param variables A map of the names of scope variables to their types
     * @param variableTypeArguments A map of the names of scope variables to their type arguments
     * @param queryScopeVariables A map of the names of query scope variables to their values
     * @param columnVariables A set of column variable names
     * @param unboxArguments If true it will unbox the query scope arguments
     * @param timeConversionResult The result of converting time literals in the expression
     * @throws QueryLanguageParseException If any exception or error is encountered
     */
    public QueryLanguageParser(
            String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableTypeArguments,
            @Nullable Map<String, Object> queryScopeVariables,
            @Nullable Set<String> columnVariables,
            boolean unboxArguments,
            @Nullable TimeLiteralReplacedExpression timeConversionResult) throws QueryLanguageParseException {
        this(
                expression,
                packageImports,
                classImports,
                staticImports,
                variables,
                variableTypeArguments,
                queryScopeVariables,
                columnVariables,
                unboxArguments,
                false,
                PyCallableWrapperJpyImpl.class.getName(),
                timeConversionResult);
    }

    /**
     * Create a QueryLanguageParser and parse the given {@code expression}. After construction, the
     * {@link QueryLanguageParser.Result result} of parsing the {@code expression} is available with the
     * {@link #getResult()}} method.
     * <p>
     * Note that the provided Collections and Maps must not be mutated concurrently with or after construction.
     *
     * @param expression The query language expression to parse
     * @param packageImports Wildcard package imports
     * @param classImports Individual class imports
     * @param staticImports Wildcard static imports. All static variables and methods for the given classes are
     *        imported.
     * @param variables A map of the names of scope variables to their types
     * @param variableTypeArguments A map of the names of scope variables to their type arguments
     * @throws QueryLanguageParseException If any exception or error is encountered
     */
    @TestUseOnly
    QueryLanguageParser(
            String expression,
            Collection<Package> packageImports,
            Collection<Class<?>> classImports,
            Collection<Class<?>> staticImports,
            Map<String, Class<?>> variables,
            Map<String, Class<?>[]> variableTypeArguments) throws QueryLanguageParseException {
        this(expression, packageImports, classImports, staticImports, variables,
                variableTypeArguments, null, null, true, null);
    }

    /**
     * Create a QueryLanguageParser and parse the given {@code expression}. After construction, the
     * {@link QueryLanguageParser.Result result} of parsing the {@code expression} is available with the
     * {@link #getResult()}} method.
     * <p>
     * Note that the provided Collections and Maps must not be mutated concurrently with or after construction.
     *
     * @param expression The query language expression to parse
     * @param packageImports Wildcard package imports
     * @param classImports Individual class imports
     * @param staticImports Wildcard static imports. All static variables and methods for the given classes are
     *        imported.
     * @param variables A map of the names of scope variables to their types
     * @param variableTypeArguments A map of the names of scope variables to their type arguments
     * @param queryScopeVariables A map of the names of query scope variables to their values
     * @param columnVariables A set of column variable names
     * @param unboxArguments If true it will unbox the query scope arguments
     * @param verifyIdempotence If true, the parser will verify that the result expression will not mutate when parsed
     * @param pyCallableWrapperImplName The name of the PyCallableWrapper implementation to use
     * @param timeConversionResult The result of converting time literals in the expression
     * @throws QueryLanguageParseException If any exception or error is encountered
     */
    @VisibleForTesting
    QueryLanguageParser(
            String expression,
            final Collection<Package> packageImports,
            final Collection<Class<?>> classImports,
            final Collection<Class<?>> staticImports,
            final Map<String, Class<?>> variables,
            final Map<String, Class<?>[]> variableTypeArguments,
            @Nullable final Map<String, Object> queryScopeVariables,
            @Nullable final Set<String> columnVariables,
            final boolean unboxArguments,
            final boolean verifyIdempotence,
            @NotNull final String pyCallableWrapperImplName,
            @Nullable final TimeLiteralReplacedExpression timeConversionResult) throws QueryLanguageParseException {
        this.packageImports = packageImports == null ? Collections.emptySet() : Set.copyOf(packageImports);
        this.classImports = classImports == null ? Collections.emptySet() : Set.copyOf(classImports);
        this.staticImports = staticImports == null ? Collections.emptySet() : Set.copyOf(staticImports);
        this.variables = variables == null ? Collections.emptyMap() : variables;
        this.variableTypeArguments = variableTypeArguments == null ? Collections.emptyMap() : variableTypeArguments;
        this.queryScopeVariables = queryScopeVariables == null ? new HashMap<>() : queryScopeVariables;
        this.columnVariables = columnVariables == null ? Collections.emptySet() : columnVariables;
        this.unboxArguments = unboxArguments;

        Assert.nonempty(pyCallableWrapperImplName, "pyCallableWrapperImplName");
        this.pyCallableWrapperImplName = pyCallableWrapperImplName;

        // Convert backticks *before* converting single equals!
        // Backticks must be converted first in order to properly identify single-equals signs within
        // String and char literals, which should *not* be converted.
        expression = convertBackticks(expression);
        expression = convertSingleEquals(expression);

        final VisitArgs printer = VisitArgs.create();
        try {
            final Expression expr = JavaExpressionParser.parseExpression(expression);
            final boolean isConstantValueExpression = JavaExpressionParser.isConstantValueExpression(expr);
            final Pair<String, Map<Long, List<MatchPair>>> formulaShiftColPair =
                    ShiftedColumnsFactory.getShiftToColPairsMap(expr);

            WrapperNode wrapperNode = new WrapperNode(expr);
            expr.setParentNode(wrapperNode);

            Class<?> type = expr.accept(this, printer);

            if (type == null) {
                throw new IllegalStateException("Parser returned null type!");
            }

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
                            "    Original printer result : " + printedSource + ".\n" +
                            "    Printed AST expression  : " + parserExpressionDumped);
                }
            }

            if (verifyIdempotence) {
                try {
                    // make sure the parser has no problem reparsing its own output and makes no changes to it.
                    final QueryLanguageParser validationQueryLanguageParser = new QueryLanguageParser(
                            printedSource, packageImports, classImports, staticImports, variables,
                            variableTypeArguments, queryScopeVariables, columnVariables, false, false,
                            pyCallableWrapperImplName, timeConversionResult);

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
                    throw new ParserVerificationFailure(
                            "Failed idempotence check: expression result modified on reparse", ex);
                }
            }

            result = new Result(type, printer.builder.toString(), variablesUsed, this.queryScopeVariables,
                    isConstantValueExpression, formulaShiftColPair, timeConversionResult);
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
            types.add(arguments[i].accept(this, printer.cloneWithCastingContext(null)));

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
        if (name.contains(".")) { // Fully-qualified class name
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ignored) {
            }
        } else { // Simple name

            // check whether 'name' is an imported class:
            for (Class<?> classImport : classImports) {
                if (name.equals(classImport.getSimpleName())) {
                    return classImport;
                }
            }

            // check whether 'name' is a static member of a static-imported class:
            final Class<?> potentialStaticImportedClass = lookupStaticImport(name);
            if (potentialStaticImportedClass != null) {
                if (!name.equals(potentialStaticImportedClass.getSimpleName())) {
                    // Make sure we actually got back a class matching 'name', not some other static
                    // member that happens to be imported and match 'name'.
                    final Class<?> declaringClass = potentialStaticImportedClass.getDeclaringClass();
                    final String declaringClassName = declaringClass == null ? "null" : declaringClass.getName();
                    throw new ParserResolutionFailure(
                            "Expected class named \"" + name + "\" but found " + potentialStaticImportedClass.getName()
                                    + " instead. Declaring class: " + declaringClassName);
                }
                return potentialStaticImportedClass;
            }

            // check whether 'name' is the name of a class in an imported package:
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
            final Class<?>[][] typeArguments) {
        final ArrayList<Method> acceptableMethods = new ArrayList<>();

        if (scope == null) {
            for (final Class<?> classImport : staticImports) {
                for (Method method : classImport.getDeclaredMethods()) {
                    if (Modifier.isStatic(method.getModifiers())) {
                        possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, typeArguments);
                    }
                }
            }
            // for Python function/Groovy closure call syntax without the explicit 'call' keyword, check if it is
            // defined in Query scope
            if (acceptableMethods.isEmpty()) {
                // if the method name corresponds to an object in the query scope, and the type of that object
                // is something that could be potentially implicitly call()ed (e.g. PyCallableWrapper/Closure),
                // then try to add its call() method to the acceptableMethods list.
                final Class<?> methodClass = variables.get(methodName);
                if (methodClass != null && isPotentialImplicitCall(methodName, methodClass)) {
                    for (Method method : methodClass.getMethods()) {
                        possiblyAddExecutable(acceptableMethods, method, "call", paramTypes, typeArguments);
                    }
                }
                if (!acceptableMethods.isEmpty()) {
                    variablesUsed.add(methodName);
                }
            }
        } else {
            // Add the actual methods for the object (including PyObject's methods)
            for (final Method method : scope.getMethods()) {
                possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, typeArguments);
            }

            if (acceptableMethods.isEmpty()
                    && (scope.equals(org.jpy.PyObject.class) || PyCallableWrapper.class.isAssignableFrom(scope))) {
                // This is a Python method call; just assume it exists and wrap in a PyCallableWrapper
                possiblyAddExecutable(acceptableMethods, PY_CALLABLE_WRAPPER_CALL_METHOD, "call", paramTypes,
                        typeArguments);
            } else {
                // If 'scope' is an interface, we must explicitly consider the methods in Object
                if (scope.isInterface()) {
                    for (final Method method : Object.class.getMethods()) {
                        possiblyAddExecutable(acceptableMethods, method, methodName, paramTypes, typeArguments);
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
            if (bestMethod == null || isMoreSpecificMethod(bestMethod, method, paramTypes)) {
                bestMethod = method;
            }
        }

        return bestMethod;
    }

    private static boolean isPotentialImplicitCall(final String methodName, final Class<?> methodClass) {
        final boolean isPotentialPythonImplicitCall =
                PyCallableWrapper.class.isAssignableFrom(methodClass) && !GET_ATTRIBUTE_METHOD_NAME.equals(methodName);
        final boolean isPotentialGroovyImplicitCall = methodClass == Closure.class && !"call".equals(methodName);
        return isPotentialPythonImplicitCall || isPotentialGroovyImplicitCall;
    }

    private Class<?> getMethodReturnType(Class<?> scope, String methodName, Class<?>[] paramTypes,
            Class<?>[][] typeArguments) {
        return getMethod(scope, methodName, paramTypes, typeArguments).getReturnType();
    }

    /**
     * Creates an array type from {@code type}, if necessary.
     * 
     * @param type The initial type.
     * @param arrayDimensions The number of array dimensions to add.
     * @return Either an array type for {@code type} (if {@code arrayDimensions != 0}), or {@code type} itself.
     */
    private static Class<?> createArrayType(Class<?> type, int arrayDimensions) {
        for (; arrayDimensions > 0; arrayDimensions--) {
            type = Array.newInstance(type, 0).getClass();
        }
        return type;
    }

    private Class<?> calculateMethodReturnTypeUsingGenerics(
            final Class<?> scope,
            final Expression scopeExpr,
            final Method method,
            final Class<?>[] argumentTypes,
            final Class<?>[][] typeArgumentsForMethodArguments) {
        final Type methodReturnTypePossibleArray = method.getGenericReturnType();

        // Compute the base method return type (after stripping off array dimensions)
        final int arrayDimensions;
        Type methodReturnType = methodReturnTypePossibleArray;
        {
            int arrayDimensionsTmp = 0;
            while (methodReturnType instanceof GenericArrayType) {
                methodReturnType = ((GenericArrayType) methodReturnType).getGenericComponentType();
                arrayDimensionsTmp++;
            }
            arrayDimensions = arrayDimensionsTmp;
        }

        if (!(methodReturnType instanceof TypeVariable)) {
            return method.getReturnType();
        }

        final TypeVariable<?> genericReturnType = (TypeVariable<?>) methodReturnType;

        // if the method is being invoked on a variable (i.e. NameExpr) of a parameterized type, and we have
        // type arguments for that variable, then check whether the return type is one of the scope's type parameters.
        if (scopeExpr instanceof NameExpr) {
            Class<?>[] typeArguments = variableTypeArguments.get(((NameExpr) scopeExpr).getNameAsString());
            if (typeArguments != null) {
                final TypeVariable<? extends Class<?>>[] scopeTypeParameters = scope.getTypeParameters();
                for (int i = 0; i < scopeTypeParameters.length; i++) {
                    if (scopeTypeParameters[i].equals(genericReturnType)) {
                        final Class<?> typeArgument = typeArguments[i];
                        // use the type from the type arg if available; otherwise return first generic bound
                        if (typeArgument != null) {
                            return createArrayType(typeArgument, arrayDimensions);
                        }
                        final Type[] typeParamBounds = scopeTypeParameters[i].getBounds();
                        Assert.neqNull(typeParamBounds, "typeParamBounds");
                        Assert.gtZero(typeParamBounds.length, "typeParamBounds.length");
                        final Type genericTypeBound = typeParamBounds[0];
                        if (!(genericTypeBound instanceof Class<?>)) {
                            throw new IllegalStateException("Unexpected type: " + genericTypeBound);
                        }

                        return createArrayType((Class<?>) genericTypeBound, arrayDimensions);
                    }
                }
            }
        }


        // check for the generic type in a param

        final Type[] genericParameterTypes = method.getGenericParameterTypes();

        for (int i = 0; i < genericParameterTypes.length; i++) {
            Type genericParamType = genericParameterTypes[i];
            Class<?> argType = argumentTypes[i];

            while (genericParamType instanceof GenericArrayType) {
                genericParamType = ((GenericArrayType) genericParamType).getGenericComponentType();
            }

            while (argType.isArray()) {
                argType = argType.getComponentType();
            }

            // Check whether the return type can be captured from a method argument.
            // e.g. calling `T someMethod(T myArg)` with `someMethod("my string")` should return String.class.
            if (genericReturnType.equals(genericParamType)) {
                return createArrayType(argType, arrayDimensions);
            }

            // Check whether the return type can be inferred from a type argument of a method argument.
            // e.g. calling `<T> T getSomeItem(Set<T> arg)` with `getSomeItem(Collections.singleton("my string"))`
            // should return String.class.
            if ((genericParamType instanceof ParameterizedType) && (typeArgumentsForMethodArguments[i] != null)) {
                Type[] methodParameterizedTypes = ((ParameterizedType) genericParamType).getActualTypeArguments();

                for (int j = 0; j < methodParameterizedTypes.length; j++) {
                    if (genericReturnType.equals(methodParameterizedTypes[j])) {
                        Class<?> returnType = typeArgumentsForMethodArguments[i][j];
                        return createArrayType(returnType, arrayDimensions);
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
            if (bestConstructor == null || isMoreSpecificConstructor(bestConstructor, constructor, paramTypes)) {
                bestConstructor = constructor;
            }
        }

        return bestConstructor;
    }

    private static String paramsTypesToString(Class<?>[] paramTypes) {
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
            final Class<?>[][] typeArguments) {
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
                paramType = convertVector(paramType, typeArguments[i] == null ? null : typeArguments[i][0]);
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
                paramType = convertVector(paramType, typeArguments[candidateParamTypes.length - 1] == null ? null
                        : typeArguments[candidateParamTypes.length - 1][0]);
            }

            boolean canAssignVarArgs = candidateParamTypes.length == paramTypes.length && paramType.isArray()
                    && canAssignType(candidateParamType, paramType);

            boolean canAssignParamsToVarArgs = true;
            final Class<?> lastClass = candidateParamType.getComponentType();

            for (int i = candidateParamTypes.length - 1; i < paramTypes.length; i++) {
                paramType = paramTypes[i];

                if (isTypedVector(paramType) && lastClass.isArray()) {
                    paramType = convertVector(paramType,
                            typeArguments[i] == null ? null : typeArguments[i][0]);
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

    /**
     * Checks whether {@code candidateParamType} is assignable from {@code paramType}.
     *
     * @see #dhqlIsAssignableFrom
     */
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

    private static boolean isMoreSpecificConstructor(final Constructor<?> c1, final Constructor<?> c2,
            Class<?>[] argExprTypes) {
        final Boolean executableResult = isMoreSpecificExecutable(c1, c2, argExprTypes);
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
    private static boolean isMoreSpecificMethod(final Method m1, final Method m2, Class<?>[] argExprTypes) {
        final Boolean executableResult = isMoreSpecificExecutable(m1, m2, argExprTypes);
        // NB: executableResult can be null in cases where an override narrows its return type
        return executableResult == null ? dhqlIsAssignableFrom(m1.getReturnType(), m2.getReturnType())
                : executableResult;
    }

    /**
     * Check whether {@code e2} is more specific than {@code e1}.
     *
     * @param e1 The current best-choice executable.
     * @param e2 A possible better-matching execuable.
     * @param argExprTypes The argument types. (Used as a tiebreaker between primitive and boxed parameters.)
     * @param <EXECUTABLE_TYPE> The kind of executable ({@link Method} vs {@link Constructor}).
     * @return {@code true} if {@code e2} is more specific than {@code e1}, {@code false} if {@code e1} is more specific
     *         than {@code e2}, and {@code null} if no determination could be made.
     */
    private static <EXECUTABLE_TYPE extends Executable> Boolean isMoreSpecificExecutable(
            final EXECUTABLE_TYPE e1, final EXECUTABLE_TYPE e2, Class<?>[] argExprTypes) {

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

        // note that at this point, e1.isVarArgs() == e2.isVarArgs()
        final boolean bothExecutablesAreVarargs = e1.isVarArgs();
        if (bothExecutablesAreVarargs) {
            e1ParamTypes[e1ParamTypes.length - 1] = e1ParamTypes[e1ParamTypes.length - 1].getComponentType();
            e2ParamTypes[e2ParamTypes.length - 1] = e2ParamTypes[e2ParamTypes.length - 1].getComponentType();
        }

        for (int i = 0; i < e1ParamTypes.length; i++) {
            final Class<?> e1ParamType = e1ParamTypes[i];
            final Class<?> e2ParamType = e2ParamTypes[i];

            // if the e1 param type is not a superclass of the e2 param type, then the e2 param is not more specific
            // to the provided args. (Note that the executables passed here are only ones that match the provided
            // arguments.)
            // For example, let's say we have myMethod(Collection), myMethod(List), and myMethod(ArrayList), and
            // the actual method call is myMethod(someLinkedList). Both myMethod(Collection) and myMethod(List) will
            // match. Since Collection is assignable from List, we know that Collection is a superclass if List, and
            // therefore is a less-specific match than List.
            if (!canAssignType(e1ParamType, e2ParamType) && !isTypedVector(e2ParamType)) {
                return false;
            }

            // If there are overloads for both primitive and boxed parameters, the one that matches the provided
            // arguments should be considered more specific.
            final Class<?> argExprType = argExprTypes[i];
            if (argExprType.isPrimitive() && e1ParamType.isPrimitive() && !e2ParamType.isPrimitive()) {
                return false;
            }
            if (TypeUtils.isBoxedType(argExprType) && TypeUtils.isBoxedType(e1ParamType)
                    && !TypeUtils.isBoxedType(e2ParamType)) {
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

    /**
     * Gets the type arguments corresponding to the {@code expressions}, if there are any declared.
     *
     * @param expressions The expressions to check type arguments for.
     * @return An array with the same length as {@code expressions},
     */
    private Class<?>[][] getTypeArguments(Expression... expressions) {
        final Class<?>[][] typeArgs = new Class[expressions.length][];

        for (int i = 0; i < expressions.length; i++) {
            if ((expressions[i] instanceof NameExpr)) {
                typeArgs[i] = variableTypeArguments.get(((NameExpr) expressions[i]).getNameAsString());
            }
        }

        return typeArgs;
    }

    /**
     * Returns the array type corresponding to a vector.
     *
     * @param type The original vector type.
     * @param typeArgument The type argument (for ObjectVectors, when applicable), otherwise {code null}
     * @return An appropriate array type corresponding to the vector type.
     */
    private static Class<?> convertVector(Class<?> type, Class<?> typeArgument) {
        if (ObjectVector.class.isAssignableFrom(type)) {
            return Array.newInstance(typeArgument == null ? Object.class : typeArgument, 0).getClass();
        }
        if (IntVector.class.isAssignableFrom(type)) {
            return int[].class;
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

    public static String getOperatorSymbol(BinaryExpr.Operator op) {
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
                throw new IllegalStateException(
                        "NOT_EQUALS must be converted to '!eq(a, b)'or kept as the operator 'a != b'");
            case LESS:
                return "less";
            case GREATER:
                return "greater";
            case LESS_EQUALS:
                return "lessEquals";
            case GREATER_EQUALS:
                return "greaterEquals";
            case LEFT_SHIFT:
                throw new UnsupportedOperationException("leftShift (<<) is not supported.");
            case SIGNED_RIGHT_SHIFT:
                throw new UnsupportedOperationException("signedRightShift (>>) is not supported.");
            case UNSIGNED_RIGHT_SHIFT:
                throw new UnsupportedOperationException("unsignedRightShift (>>>) is not supported.");
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
     * @param typeArguments The actual type arguments corresponding to the expressions
     * @param expressions The actual expressions
     * @return An array of new expressions that maintain the 'meaning' of the input {@code expressions} but are
     *         appropriate to pass to {@code executable}
     */
    private Expression[] convertParameters(final Executable executable,
            final Class<?>[] argumentTypes, final Class<?>[] expressionTypes,
            final Class<?>[][] typeArguments, Expression[] expressions) {
        // For Python callables, all Vector columns should be converted into arrays
        if (PyCallableWrapper.class.isAssignableFrom(executable.getDeclaringClass())) {
            for (int ei = 0; ei < expressionTypes.length; ei++) {
                if (isTypedVector(expressionTypes[ei])) {
                    // replace node in its parent, otherwise setArguments() will clear the parent of 'expressions[ei]'
                    expressions[ei].replace(new DummyExpr());

                    expressions[ei] =
                            new MethodCallExpr(new NameExpr("VectorConversions"), "nullSafeVectorToArray",
                                    new NodeList<>(expressions[ei]));
                    // To prevent JPY from unpacking a non-primitive array to make sure the result array is treated
                    // as a single argument
                    if (ObjectVector.class.isAssignableFrom(expressionTypes[ei])) {
                        expressions[ei] = new CastExpr(
                                StaticJavaParser.parseClassOrInterfaceType("java.lang.Object"),
                                expressions[ei]);
                        expressionTypes[ei] = Object.class;
                    } else {
                        expressionTypes[ei] = convertVector(expressionTypes[ei],
                                typeArguments[ei] == null ? null : typeArguments[ei][0]);
                    }
                }
            }
        }



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
                // note: the setArguments() bug does not arise here in practice, since nodes representing boxed types
                // are not replaced by the parser, but it is better to insert the DummyExpr anyway as future-proofing.
                expressions[ai].replace(new DummyExpr());
                if (expressionTypes[ai] == NULL_CLASS) {
                    expressions[ai] = new FieldAccessExpr(new NameExpr("io.deephaven.util.QueryConstants"),
                            "NULL_" + argumentTypes[ai].getSimpleName().toUpperCase());
                } else {
                    expressions[ai] = new MethodCallExpr(expressions[ai],
                            argumentTypes[ai].getSimpleName() + "Value", new NodeList<>());
                }
            } else if (argumentTypes[ai].isArray() && isTypedVector(expressionTypes[ai])) {
                // note: the setArguments() bug does not arise here in practice, since nodes representing a Vector
                // are not replaced by the parser, but it is better to insert the DummyExpr anyway as future-proofing.
                expressions[ai].replace(new DummyExpr());
                expressions[ai] = new MethodCallExpr(new NameExpr("VectorConversions"), "nullSafeVectorToArray",
                        new NodeList<>(expressions[ai]));
                expressionTypes[ai] = convertVector(expressionTypes[ai],
                        typeArguments[ai] == null ? null : typeArguments[ai][0]);
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
                // note: the setArguments() bug does not arise here in practice, since nodes representing a Vector
                // are not replaced by the parser, but it is better to insert the DummyExpr anyway as future-proofing.
                expressions[lastArgIndex].replace(new DummyExpr());
                expressions[lastArgIndex] =
                        new MethodCallExpr(new NameExpr("VectorConversions"), "nullSafeVectorToArray",
                                new NodeList<>(expressions[lastArgIndex]));
                expressionTypes[lastArgIndex] = convertVector(expressionTypes[lastArgIndex],
                        typeArguments[lastArgIndex] == null ? null : typeArguments[lastArgIndex][0]);
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
                        // replace node in its parent, otherwise setArguments() will clear the parent of
                        // 'expressions[ei]'
                        expressions[ei].replace(new DummyExpr());
                        expressions[ei] = new CastExpr(
                                new PrimitiveType(PrimitiveType.Primitive
                                        .valueOf(varArgType.getSimpleName().toUpperCase())),
                                expressions[ei]);
                    } else if (unboxArguments && varArgType.isPrimitive() && !expressionTypes[ei].isPrimitive()) {
                        // replace node in its parent, otherwise setArguments() will clear the parent of
                        // 'expressions[ei]'
                        expressions[ei].replace(new DummyExpr());
                        expressions[ei] = new MethodCallExpr(expressions[ei],
                                varArgType.getSimpleName() + "Value", new NodeList<>());
                    } else if (!expressionTypes[ei].isPrimitive()) {
                        allExpressionTypesArePrimitive = false;
                    }
                }
            }

            if ((TypeUtils.isBoxedType(varArgType) || varArgType.isPrimitive()) && allExpressionTypesArePrimitive) {
                // method invocation is ambiguous when both boxed and primitive versions of the method exist
                Expression[] temp = new Expression[nArgs];
                Expression[] varArgExpressions = new Expression[nArgExpressions - nArgs + 1];
                System.arraycopy(expressions, 0, temp, 0, temp.length - 1);
                System.arraycopy(expressions, nArgs - 1, varArgExpressions, 0,
                        varArgExpressions.length);

                for (Expression expr : varArgExpressions) {
                    // replace node in its parent, otherwise setArguments() will clear the parent of 'expr''
                    expr.replace(new DummyExpr());
                }

                NodeList<ArrayCreationLevel> levels = new NodeList<>(new ArrayCreationLevel());
                com.github.javaparser.ast.type.Type elementType;
                if (varArgType.isPrimitive()) {
                    // wrap the vararg argument expressions in a primitive array:
                    elementType = new PrimitiveType(PrimitiveType.Primitive.valueOf(
                            varArgType.getSimpleName().toUpperCase()));
                } else {
                    // wrap the vararg argument expressions in a boxed array:
                    elementType = StaticJavaParser.parseClassOrInterfaceType(varArgType.getSimpleName());
                }
                temp[temp.length - 1] = new ArrayCreationExpr(
                        elementType, levels, new ArrayInitializerExpr(new NodeList<>(varArgExpressions)));

                expressions = temp;
            }
        }

        return expressions;
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<?> visit(NameExpr n, VisitArgs printer) {
        final String nameStr = n.getNameAsString();
        printer.append(nameStr);

        return nameLookupCache.computeIfAbsent(nameStr, this::resolveName);
    }

    private Class<?> resolveName(final String nameStr) {
        /*
        @formatter:off
        JLS on how to resolve names: https://docs.oracle.com/javase/specs/jls/se11/html/jls-6.html#jls-6.5

        The QueryLanguageParser doesn't work exactly this way (some cases are not relevant, and the work
        is split between this class and the JavaParser library), but the behavior should be consistent with the spec.

        Situations where this method will be called include:
        - resolving a variable name (e.g. 'myObject')
        - resolving a statically-imported field or class name (e.g. 'MyStaticNestedClass', 'myStaticVar')
        - resolving a method invocation target (e.g. 'someName.myMethod()')

        There is
        1)  Simple names (i.e. name does not contain a '.'):
                1. Check whether it's in the scope.
                2. If it's not in the scope, see if it's a static import.
                3. If it's not a static import, assume it's a class and look it up with 'findClass()'.
        2)  Qualified names: just look up the name immediately with 'findClass()'

        @formatter:on
         */
        Class<?> ret;
        final boolean isSimpleName = nameStr.indexOf('.') < 0;
        if (isSimpleName) {
            ret = variables.get(nameStr);
            if (ret != null) {
                variablesUsed.add(nameStr);
                return ret;
            }

            ret = lookupStaticImport(nameStr);
            if (ret != null) {
                return ret;
            }
        }

        // If we have not returned a result yet, then 'n' must be a class name. (Or something invalid.)
        ret = findClass(nameStr);
        if (ret != null) {
            return ret;
        }

        throw new ParserResolutionFailure("Cannot find variable or class " + nameStr);
    }

    /**
     * Check whether {@code name} is in scope from a static import. Results are cached.
     *
     * @param name The name to look up.
     * @return The type of {@code name}, if {@code name} is imported by a static import.
     */
    @Nullable
    private Class<?> lookupStaticImport(@NotNull final String name) {
        return staticImportLookupCache.computeIfAbsent(name, nameForLookup -> {
            for (final Class<?> staticImportedClass : staticImports) {
                // We don't support static imports of individual fields/methods -- have to check among
                // *all* members of a class.
                try {
                    final Field field = staticImportedClass.getField(nameForLookup);
                    if (Modifier.isStatic(field.getModifiers())) {
                        return field.getType();
                    }
                } catch (NoSuchFieldException ignored) {
                }

                for (final Class<?> nestedClass : staticImportedClass.getDeclaredClasses()) {
                    if (Modifier.isStatic(nestedClass.getModifiers())
                            && nestedClass.getSimpleName().equals(nameForLookup)) {
                        return nestedClass;
                    }
                }
            }
            return null;
        });
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

        // Now get the updated left/right expressions. (We must retrieve these after running getTypeWithCaching(),
        // since the original left/right nodes in the BinaryExpr may have been replaced while traversing the tree.)
        final Expression leftExpr = n.getLeft();
        final Expression rightExpr = n.getRight();

        if ((lhType == String.class || rhType == String.class) && op == BinaryExpr.Operator.PLUS) {

            leftExpr.accept(this, printer);
            printer.append(' ').append(getOperatorSymbol(op)).append(' ');
            rightExpr.accept(this, printer);

            return String.class;
        }

        // For boolean operations that don't involve Deephaven's ordering guarantees or null handling,
        // the operation can be printed as-is instead of using QueryLanguageFunctionUtil methods.
        final boolean isPrimitiveBooleanOperation =
                boolean.class.equals(lhType) && boolean.class.equals(rhType) && (op == BinaryExpr.Operator.EQUALS
                        || op == BinaryExpr.Operator.NOT_EQUALS
                        || op == BinaryExpr.Operator.BINARY_OR
                        || op == BinaryExpr.Operator.BINARY_AND
                        || op == BinaryExpr.Operator.XOR);
        if (op == BinaryExpr.Operator.OR || op == BinaryExpr.Operator.AND || isPrimitiveBooleanOperation) {

            leftExpr.accept(this, printer);
            printer.append(' ').append(getOperatorSymbol(op)).append(' ');
            rightExpr.accept(this, printer);

            return boolean.class;
        }

        // Replace `x!=y` with `!(x==y)`.
        // (QueryLanguageFunctionUtils only has `eq()` methods, not `neq()`, since `!eq()` is sufficient.)
        if (op == BinaryExpr.Operator.NOT_EQUALS) {
            // Update the AST -- create an EQUALS BinaryExpr, then invert its result.
            final BinaryExpr equalsExpr = new BinaryExpr(n.getLeft(), n.getRight(), BinaryExpr.Operator.EQUALS);

            final UnaryExpr newExpr = new UnaryExpr(equalsExpr, UnaryExpr.Operator.LOGICAL_COMPLEMENT);
            newExpr.setData(QueryLanguageParserDataKeys.BINARY_OP_NEQ_CONVERSION_FLAG, true);
            replaceChildExpression(origParent, n, newExpr);


            assertNothingPrinted.run();
            return newExpr.accept(this, printer);
        }

        if (op == BinaryExpr.Operator.EQUALS) {
            // Note that `isNull()` is the only method used here that's not from QueryLanguageFunctionUtils.
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
                                                                           // e.g. `x==y` --> `eq(x, y)`

            assertNothingPrinted.run();
            return binaryOpOverloadMethod.accept(this, printer);
        }

        return getMethodReturnType(null, methodName, new Class[] {lhType, rhType},
                getTypeArguments(n.getLeft(), n.getRight()));
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
    private static Runnable getNothingPrintedAssertion(VisitArgs printer) {
        if (!printer.hasStringBuilder()) {
            return () -> {
            };
        }
        final int initialLen = printer.builder.length();
        return () -> Assert.eq(printer.builder.length(), "printer.builder.length()", initialLen, "initialLen");
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

        final boolean isNonequalOpOverload = n.containsData(QueryLanguageParserDataKeys.BINARY_OP_NEQ_CONVERSION_FLAG);
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

        // Descend into the new method call expression. This should not make further changes,
        // since the original expression was visited by getTypeWithCaching at the beginning of this method.
        final Class<?> result = unaryOpOverloadMethod.accept(this, printer);

        // Verify that the operator overload method returns the original expected type:
        Assert.equals(ret, "ret", result, "result");

        return ret;
    }

    @Override
    public Class<?> visit(CastExpr n, VisitArgs printer) {
        final Runnable nothingPrintedAssertion = getNothingPrintedAssertion(printer);

        // resolve the target type:
        final Class<?> ret = n.getType().accept(this, VisitArgs.WITHOUT_STRING_BUILDER);
        final String targetTypeSimpleName = ret.getSimpleName();
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
                unboxingCastExpr.setData(QueryLanguageParserDataKeys.NO_PARAMETER_REWRITING_FLAG, true);
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
                    castMethodName = "do" + targetTypeSimpleName + "PyCast";
                } else {
                    // e.g. "intPyCast"
                    castMethodName = targetTypeSimpleName + "PyCast";
                }
            } else {
                // e.g. "intCast"
                castMethodName = targetTypeSimpleName + "Cast";
            }


            final Node origParent = n.getParentNode().orElseThrow();
            final MethodCallExpr primitiveCastExpr = new MethodCallExpr(castMethodName, finalExprToCast);
            if (ret.equals(unboxedExprType)) {
                // tell parser not to rewrite this method call (only needed when unboxing)
                primitiveCastExpr.setData(QueryLanguageParserDataKeys.NO_PARAMETER_REWRITING_FLAG, true);
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
            printer.append(n.getType().toString());
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
     * @param expr The expression to check.
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
    public static boolean isWideningPrimitiveConversion(Class<?> original, Class<?> target) {
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
            final CastExpr castExpr = new CastExpr(StaticJavaParser.parseClassOrInterfaceType("Boolean"), uncastExpr);
            n.setThenExpr(castExpr);
            // fix parent in uncastExpr (it is cleared when it is replaced with the CastExpr)
            uncastExpr.setParentNode(castExpr);
        }

        if (classA == Boolean.class && classB == boolean.class) {
            // a little hacky, but this handles the null case where it unboxes. very weird stuff
            final Expression uncastExpr = n.getElseExpr();
            final CastExpr castExpr = new CastExpr(StaticJavaParser.parseClassOrInterfaceType("Boolean"), uncastExpr);
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
                    if (scopeType == PyObject.class || PyCallableWrapper.class.isAssignableFrom(scopeType)) {
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
                        new ClassExpr(
                                StaticJavaParser.parseClassOrInterfaceType(printer.pythonCastContext.getSimpleName()));
                getAttributeArgs.add(targetType);

                // Let's advertise to the caller the cast context type
                ret = printer.pythonCastContext;
            }
            printer.append(')');

            // replace the field access with the getAttribute() call
            // we do not need to visit() (since we already visited the scope)
            final MethodCallExpr pyGetAttributeMethodCall =
                    new MethodCallExpr(n.getScope(), GET_ATTRIBUTE_METHOD_NAME, getAttributeArgs);
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

        final Class<?> scopeType = n.getScope().map(sc -> {
            Class<?> result = sc.accept(this, scopePrinter);
            scopePrinter.append('.');
            return result;
        }).orElse(null);

        final Expression[] argExpressions = getExpressionsArray(n.getArguments());

        Class<?>[] expressionTypes = printArguments(argExpressions, VisitArgs.WITHOUT_STRING_BUILDER);

        final Class<?>[][] typeArguments = getTypeArguments(argExpressions);

        final String methodName = n.getNameAsString();
        final Method method = getMethod(scopeType, methodName, expressionTypes, typeArguments);


        // TODO: we can swap out method calls for better ones here, e.g. python functions to Java ones, or
        // replacing boxed stuff with primitives (e.g. DateTime methods), etc.
        // if(methodToOptimizerMap.containsKey(method)) {
        // methodToOptimizerMap.get(method).apply(n, printer);
        // }

        final Class<?>[] argumentTypes = method.getParameterTypes();
        final Expression[] convertedArgExpressions;

        // now do some parameter conversions...
        if (!n.containsData(QueryLanguageParserDataKeys.NO_PARAMETER_REWRITING_FLAG)) {
            convertedArgExpressions =
                    convertParameters(method, argumentTypes, expressionTypes, typeArguments, argExpressions);
        } else {
            convertedArgExpressions = argExpressions;
        }
        n.setArguments(NodeList.nodeList(convertedArgExpressions));

        if (isPotentialImplicitCall(methodName, method.getDeclaringClass())) {
            if (scopeType == null) { // python func call or Groovy closure call
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

                callMethodCall.setData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS,
                        new PyCallableDetails(null, methodName));

                if (PyCallableWrapper.class.isAssignableFrom(method.getDeclaringClass())) {
                    final Optional<Class<?>> optionalRetType = pyCallableReturnType(callMethodCall);
                    if (optionalRetType.isPresent()) {
                        Class<?> retType = optionalRetType.get();
                        final Optional<CastExpr> optionalCastExpr =
                                makeCastExpressionForPyCallable(retType, callMethodCall);
                        if (optionalCastExpr.isPresent()) {
                            final CastExpr castExpr = optionalCastExpr.get();
                            replaceChildExpression(
                                    n.getParentNode().orElseThrow(),
                                    n,
                                    castExpr);

                            callMethodCall.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS).setCasted(true);
                            try {
                                return castExpr.accept(this, printer);
                            } catch (Exception e) {
                                // exceptions could be thrown by {@link #tryVectorizePythonCallable}
                                replaceChildExpression(
                                        castExpr.getParentNode().orElseThrow(),
                                        castExpr,
                                        callMethodCall);
                                callMethodCall.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS)
                                        .setCasted(false);
                                return callMethodCall.accept(this, printer);
                            }
                        }
                    }
                }
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
                        || PyObject.class.isAssignableFrom(scopeType) && methodName.equals(GET_ATTRIBUTE_METHOD_NAME);

                final Expression scopeExpr = n.getScope().orElseThrow();
                if (isExplicitCall) {
                    printer.append(scopePrinter);
                    printer.append(methodName);
                } else {
                    final MethodCallExpr getAttributeCall = new MethodCallExpr(
                            scopeExpr,
                            GET_ATTRIBUTE_METHOD_NAME,
                            NodeList.nodeList(new StringLiteralExpr(methodName)));


                    final ObjectCreationExpr newPyCallableExpr = new ObjectCreationExpr(
                            null,
                            StaticJavaParser.parseClassOrInterfaceType(pyCallableWrapperImplName),
                            NodeList.nodeList(getAttributeCall));

                    final MethodCallExpr callMethodCall = new MethodCallExpr(
                            new EnclosedExpr(newPyCallableExpr),
                            "call",
                            n.getArguments());

                    callMethodCall.setData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS,
                            new PyCallableDetails(scopePrinter.builder.toString(), methodName));

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

            // Print method type arguments, if specified.
            // (The parser ignores these, but they must be printed so that the printer output matches the printed AST.)
            final Optional<NodeList<com.github.javaparser.ast.type.Type>> methodTypeArgs = n.getTypeArguments();
            if (methodTypeArgs.isPresent()) {
                printer.append('<');
                NodeList<com.github.javaparser.ast.type.Type> get = methodTypeArgs.get();
                for (int i = 0; i < get.size(); i++) {
                    if (i > 0) {
                        printer.append(", ");
                    }
                    com.github.javaparser.ast.type.Type typeArg = get.get(i);
                    printer.append(typeArg.asString());
                }
                printer.append('>');
            }

            printer.append(methodName);
        }

        final Class<?>[] argTypes = printArguments(convertedArgExpressions, printer);

        // Attempt python function call vectorization.
        if (scopeType != null && PyCallableWrapper.class.isAssignableFrom(scopeType)) {
            verifyPyCallableArguments(n, argTypes);
            tryVectorizePythonCallable(n, scopeType, convertedArgExpressions, argTypes);
        }

        return calculateMethodReturnTypeUsingGenerics(scopeType, n.getScope().orElse(null), method, expressionTypes,
                typeArguments);
    }

    private void verifyPyCallableArguments(@NotNull MethodCallExpr n, @NotNull Class<?>[] argTypes) {
        final String invokedMethodName = n.getNameAsString();

        if (GET_ATTRIBUTE_METHOD_NAME.equals(invokedMethodName)) {
            // Currently Python UDF handling is only supported for top module level function(callable) calls.
            // The getAttribute() calls which is needed to support Python method calls, which is beyond the scope of
            // current implementation. So we are skipping the argument verification for getAttribute() calls.
            return;
        }
        if (!n.containsData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS)) {
            return;
        }
        final PyCallableDetails pyCallableDetails = n.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS);
        final String pyMethodName = pyCallableDetails.pythonMethodName;
        final Object methodVar = queryScopeVariables.get(pyMethodName);
        if (!(methodVar instanceof PyCallableWrapper)) {
            return;
        }
        final PyCallableWrapper pyCallableWrapper = (PyCallableWrapper) methodVar;
        pyCallableWrapper.parseSignature();
        pyCallableWrapper.verifyArguments(argTypes);
    }

    private Optional<CastExpr> makeCastExpressionForPyCallable(Class<?> retType, MethodCallExpr callMethodCall) {
        if (retType.isPrimitive()) {
            return Optional.of(new CastExpr(
                    new PrimitiveType(PrimitiveType.Primitive
                            .valueOf(retType.getSimpleName().toUpperCase())),
                    callMethodCall));
        } else if (retType.getComponentType() != null) {
            final Class<?> componentType = retType.getComponentType();
            if (componentType.isPrimitive()) {
                ArrayType arrayType;
                if (componentType == boolean.class) {
                    arrayType = new ArrayType(StaticJavaParser.parseClassOrInterfaceType("java.lang.Boolean"));
                } else {
                    arrayType = new ArrayType(new PrimitiveType(PrimitiveType.Primitive
                            .valueOf(retType.getComponentType().getSimpleName().toUpperCase())));
                }
                return Optional.of(new CastExpr(arrayType, callMethodCall));
            } else if (retType.getComponentType() == String.class || retType.getComponentType() == Boolean.class
                    || retType.getComponentType() == Instant.class) {
                ArrayType arrayType =
                        new ArrayType(
                                StaticJavaParser.parseClassOrInterfaceType(retType.getComponentType().getSimpleName()));
                return Optional.of(new CastExpr(arrayType, callMethodCall));
            }
        } else if (retType == Boolean.class) {
            return Optional
                    .of(new CastExpr(StaticJavaParser.parseClassOrInterfaceType("java.lang.Boolean"), callMethodCall));
        } else if (retType == String.class) {
            return Optional
                    .of(new CastExpr(StaticJavaParser.parseClassOrInterfaceType("java.lang.String"), callMethodCall));
        } else if (retType == Instant.class) {
            return Optional
                    .of(new CastExpr(StaticJavaParser.parseClassOrInterfaceType("java.time.Instant"), callMethodCall));
        }

        return Optional.empty();
    }

    private Optional<Class<?>> pyCallableReturnType(@NotNull MethodCallExpr n) {
        final PyCallableDetails pyCallableDetails = n.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS);
        final String pyMethodName = pyCallableDetails.pythonMethodName;
        final Object paramValueRaw = queryScopeVariables.get(pyMethodName);
        if (!(paramValueRaw instanceof PyCallableWrapper)) {
            return Optional.empty();
        }
        final PyCallableWrapper pyCallableWrapper = (PyCallableWrapper) paramValueRaw;
        pyCallableWrapper.parseSignature();
        return Optional.ofNullable(pyCallableWrapper.getSignature().getReturnType());
    }

    @NotNull
    private static Expression[] getExpressionsArray(final NodeList<Expression> exprNodeList) {
        return exprNodeList == null ? new Expression[0]
                : exprNodeList.toArray(new Expression[0]);
    }

    /**
     * Vectorizes a Python callable, if possible.
     * 
     * @param n The method call expression. The scope must a PyCallableWrapper (e.g.,
     *        {@code new PyCallableWrapperDummyImpl(somePyObj).call()}).
     * @param scopeType The scope type. Must be a {@code Class} that extends {@link PyCallableWrapper}.
     * @param argExpressions The argument expressions
     * @param argTypes The actual types of the {@code argExpressions} (not the types in the method signature).
     */
    private void tryVectorizePythonCallable(@NotNull MethodCallExpr n,
            @NotNull Class<?> scopeType,
            @NotNull Expression[] argExpressions,
            @NotNull Class<?>[] argTypes) {
        final String invokedMethodName = n.getNameAsString();

        // assertions related to when the parser should even attempt vectorization:
        Assert.neqNull(scopeType, "scopeType");
        Assert.assertion(
                PyCallableWrapper.class.isAssignableFrom(scopeType),
                "PyCallableWrapper.class.isAssignableFrom(scopeType)",
                scopeType, "scopeType");

        if (GET_ATTRIBUTE_METHOD_NAME.equals(invokedMethodName)) {
            // Only PyCallableWrapper.getAttribute()/PyCallableWrapper.call() may be invoked from the query language.
            // Calls to getAttribute() are not currently eligible for vectorization.
            if (log.isDebugEnabled()) {
                log.debug().append("Python function call ")
                        .append(n.toString())
                        .append(" is not auto-vectorizable:")
                        .append(" getAttribute() is not currently vectorizable.")
                        .endl();
            }
            return;
        }
        Assert.equals(invokedMethodName, "invokedMethodName", "call");

        if (!n.containsData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS)) {
            // Currently, we can only vectorize Python method calls that were not explicitly wrapped by the user.
            // Supported: `myPythonFunc(a, b, c)`
            // Not supported: `new PyCallableWrapperJpyImpl(myPythonFunc).call(a, b, c)`
            // Not supported: `myPyObj.myPythonFunc(a, b, c)`
            // Not supported: `new PyCallableWrapperJpyImpl(myPyObj.getAttribute("myPythonFunc")).call(a, b, c)`
            //
            // (This is because additional information is stored in a PyCallableDetails when the parser
            // handles implicit calls.)
            if (log.isDebugEnabled()) {
                log.debug().append("Python function call ")
                        .append(n.toString())
                        .append(" is not auto-vectorizable:")
                        .append(" PyCallableDetails not found.")
                        .endl();
            }
            return;
        }
        final PyCallableDetails pyCallableDetails = n.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS);

        if (pyCallableDetails.pythonScopeExpr != null) {
            if (log.isDebugEnabled()) {
                log.debug().append("Python function call ")
                        .append(n.toString())
                        .append(" is not auto-vectorizable:")
                        .append(" method call is scoped. Scope expression: ")
                        .append(pyCallableDetails.pythonScopeExpr)
                        .endl();
            }
            // Currently, only static Python method calls can be vectorized.
            // Supported: `myPythonFunc(a, b, c)`
            // Not supported: `myPyObj.myPythonFunc(a, b, c)`
            return;
        }

        final String pyMethodName = pyCallableDetails.pythonMethodName;
        Assert.nonempty(pyMethodName, "pyMethodName");

        // Note: "paramValueRaw" needs to be the *actual PyCallableWrapper* corresponding to the method.
        // TODO: Support vectorization of instance methods of constant objects
        // ^^ i.e., create a constant for `new PyCallableWrapperImpl(pyScopeObj.getAttribute("pyMethodName"))`
        if (!queryScopeVariables.containsKey(pyMethodName)) {
            throw new IllegalStateException("Resolved Python function name " + pyMethodName + " not found");
        }
        final Object paramValueRaw = queryScopeVariables.get(pyMethodName);
        if (!(paramValueRaw instanceof PyCallableWrapper)) {
            throw new IllegalStateException("Resolved Python function name " + pyMethodName + " not callable");
        }
        final PyCallableWrapper pyCallableWrapper = (PyCallableWrapper) paramValueRaw;

        prepareVectorization(n, argExpressions, pyCallableWrapper);
        if (pyCallableWrapper.isVectorizable()) {
            prepareVectorizationArgs(n, argExpressions, argTypes, pyCallableWrapper);
        }
    }

    private void prepareVectorization(@NotNull MethodCallExpr n, @NotNull Expression[] expressions,
            PyCallableWrapper pyCallableWrapper) {
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
            pyCallableWrapper.setVectorizable(false);
            if (log.isDebugEnabled()) {
                log.debug().append("Python function call ").append(n.toString()).append(" is not auto-vectorizable:")
                        .append(ex.getMessage()).endl();
            }
        }
    }

    private void checkVectorizability(@NotNull final MethodCallExpr n,
            @NotNull final Expression[] expressions,
            @NotNull final PyCallableWrapper pyCallableWrapper) {

        pyCallableWrapper.parseSignature();
        if (!pyCallableWrapper.isVectorizableReturnType()) {
            throw new PythonCallVectorizationFailure(
                    "Python function return type is not supported: "
                            + pyCallableWrapper.getSignature().getReturnType());
        }

        // Python vectorized functions(numba, DH) return arrays of primitive/Object types. This will break the generated
        // expression evaluation code that expects singular values. This check makes sure that numba/dh vectorized
        // functions must be used alone as the entire expression after removing the enclosing parentheses.

        Node n1 = n;
        boolean autoCastChecked = false;
        while (n1.hasParentNode()) {
            n1 = n1.getParentNode().orElseThrow();
            Class<?> cls = n1.getClass();

            if (cls == CastExpr.class) {
                if (!autoCastChecked && n.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS).isCasted()) {
                    autoCastChecked = true;
                } else {
                    throw new PythonCallVectorizationFailure(
                            "The return values of Python vectorized functions can't be cast: " + n1);
                }
            } else if (cls == MethodCallExpr.class) {
                String methodName = ((MethodCallExpr) n1).getNameAsString();
                if (!autoCastChecked && n.getData(QueryLanguageParserDataKeys.PY_CALLABLE_DETAILS).isCasted()
                        && methodName.endsWith("Cast")) {
                    autoCastChecked = true;
                } else {
                    throw new PythonCallVectorizationFailure(
                            "Python vectorized function can't be used in another expression: " + n1);
                }
            } else if (cls != EnclosedExpr.class && cls != WrapperNode.class) {
                throw new PythonCallVectorizationFailure(
                        "Python vectorized function can't be used in another expression: " + n1);
            }
        }

        for (int i = 0; i < expressions.length; i++) {
            if (!(expressions[i] instanceof NameExpr) && !(expressions[i] instanceof LiteralExpr)) {
                throw new PythonCallVectorizationFailure("Invalid argument at index " + i
                        + ": Python vectorized function arguments can only be columns, variables, and constants: "
                        + expressions[i]);
            }
        }

        if (pyCallableWrapper.getSignature().getParameters().size() != expressions.length) {
            // note vectorization doesn't handle Python variadic arguments
            throw new PythonCallVectorizationFailure("Python function argument count mismatch: " + n + " "
                    + pyCallableWrapper.getSignature().getParameters().size() + " vs. " + expressions.length);
        }
    }

    private void prepareVectorizationArgs(
            MethodCallExpr n,
            Expression[] expressions,
            Class<?>[] argTypes,
            PyCallableWrapper pyCallableWrapper) {
        if (pyCallableWrapper.getSignature().getParameters().size() != expressions.length) {
            throw new PythonCallVectorizationFailure("Python function argument count mismatch: " + n + " "
                    + pyCallableWrapper.getSignature().getParameters().size() + " vs. " + expressions.length);
        }

        pyCallableWrapper.initializeChunkArguments();
        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i] instanceof LiteralExpr) {
                addLiteralArg(expressions[i], argTypes[i], pyCallableWrapper);
            } else if (expressions[i] instanceof NameExpr) {
                String name = expressions[i].asNameExpr().getNameAsString();
                if (columnVariables.contains(name)) {
                    // A column name or one of the special variables
                    pyCallableWrapper.addChunkArgument(new ColumnChunkArgument(name, argTypes[i]));
                } else if (queryScopeVariables.containsKey(name)) {
                    pyCallableWrapper.addChunkArgument(
                            new ConstantChunkArgument(queryScopeVariables.get(name), argTypes[i]));
                } else {
                    throw new IllegalStateException("Vectorizability check could not find variable by name: " + name);
                }
            } else {
                throw new IllegalStateException("Vectorizability check failed: " + n);
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

    @SuppressWarnings("unused")
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

        final Class<?> ret = n.getType().accept(this, printer);

        Expression[] expressions = getExpressionsArray(n.getArguments());

        final Class<?>[] expressionTypes = printArguments(expressions, VisitArgs.WITHOUT_STRING_BUILDER);

        final Class<?>[][] typeArguments = getTypeArguments(expressions);

        final Constructor<?> constructor = getConstructor(ret, expressionTypes, typeArguments);

        final Class<?>[] argumentTypes = constructor.getParameterTypes();

        // now do some parameter conversions...

        expressions = convertParameters(constructor, argumentTypes, expressionTypes, typeArguments, expressions);
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

        // return null; the type is determined from the parent ArrayCreationExpr
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
        private final Map<String, Object> possibleParams;
        private final boolean isConstantValueExpression;
        private final Pair<String, Map<Long, List<MatchPair>>> formulaShiftColPair;
        private final TimeLiteralReplacedExpression timeConversionResult;

        Result(
                Class<?> type,
                String source,
                HashSet<String> variablesUsed,
                Map<String, Object> possibleParams,
                boolean isConstantValueExpression,
                Pair<String, Map<Long, List<MatchPair>>> formulaShiftColPair,
                TimeLiteralReplacedExpression timeConversionResult) {
            this.type = Objects.requireNonNull(type, "type");
            this.source = source;
            this.variablesUsed = variablesUsed;
            this.possibleParams = possibleParams;
            this.isConstantValueExpression = isConstantValueExpression;
            this.formulaShiftColPair = formulaShiftColPair;
            this.timeConversionResult = timeConversionResult;
        }

        public Class<?> getType() {
            return type;
        }

        public String getConvertedExpression() {
            return source;
        }

        public boolean isConstantValueExpression() {
            return isConstantValueExpression;
        }

        public HashSet<String> getVariablesUsed() {
            return variablesUsed;
        }

        public Map<String, Object> getPossibleParams() {
            return possibleParams;
        }

        public Pair<String, Map<Long, List<MatchPair>>> getFormulaShiftColPair() {
            return formulaShiftColPair;
        }

        public TimeLiteralReplacedExpression getTimeConversionResult() {
            return timeConversionResult;
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
         * to calculate a type and don't care about side effects).
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
     * These are {@link DataKey DataKeys} used for internal data that control the QueryLanguageParser's behavior.
     */
    private static class QueryLanguageParserDataKeys {
        /**
         * This key is used to store a flag indicating that a logical complement expr (e.g. "!(a==b)") has been inserted
         * by the parser and should *NOT* be replaced with an operator overload. In other words, it's used to ensure
         * "a!=b" is transformed first into "!(a==b)" and then into "!(equals(a, b))", but not further transformed into
         * "not(equals(a, b))".
         * <p>
         * To set this flag on a node, use:
         * 
         * <pre>
         * node.setData(QueryLanguageParserDataKeys.BINARY_OP_NEQ_CONVERSION_FLAG, true);
         * </pre>
         *
         * To check whether this flag is set on a node, use:
         * 
         * <pre>
         * n.containsData(QueryLanguageParserDataKeys.BINARY_OP_NEQ_CONVERSION_FLAG);
         * </pre>
         */
        private static final DataKey<Boolean> BINARY_OP_NEQ_CONVERSION_FLAG = new DataKey<>() {};

        /**
         * This key is used to store a flag indicating that arguments must not be modified for a method call (i.e.,
         * {@link #convertParameters} should be skipped). This is used to ensure that {@code (int) myInteger} is
         * rewritten to {@code intCast(myInteger)} and not {@code intCast(myInteger.longValue()}, which is what normally
         * would happen because the parser will prefer a method call with primitive arguments over a method call with
         * reference arguments when possible.
         * <p>
         * To set this flag on a node, use:
         * 
         * <pre>
         * node.setData(QueryLanguageParserDataKeys.NO_PARAMETER_REWRITING_FLAG, true);
         * </pre>
         *
         * To check whether this flag is set on a node, use:
         * 
         * <pre>
         * n.containsData(QueryLanguageParserDataKeys.NO_PARAMETER_REWRITING_FLAG);
         * </pre>
         */
        private static final DataKey<Boolean> NO_PARAMETER_REWRITING_FLAG = new DataKey<>() {};

        /**
         * This key is used to store the {@link PyCallableDetails} for a Python method that has been transformed into a
         * more complex expression. For example, {@code myPyObj.myPyMethod()} will be transformed into
         * {@code (new io.deephaven.engine.table.impl.lang.PyCallableWrapperDummyImpl(myPyObject.getAttribute("myPyMethod"))).call()},
         * and the EnclosedExpr used as the scope of the {@code call()} method will have a {@code PyCallableDetails}
         * with {@code "myPyMethod"} as its {@link PyCallableDetails#pythonMethodName}.
         */
        private static final DataKey<PyCallableDetails> PY_CALLABLE_DETAILS = new DataKey<>() {};
    }

    /**
     * Stores information on Python method calls that have been transformed by the language parser.
     */
    private static class PyCallableDetails {

        @Nullable
        private final String pythonScopeExpr;
        @NotNull
        private final String pythonMethodName;

        @NotNull
        private boolean isCasted = false;

        public boolean isCasted() {
            return isCasted;
        }

        public void setCasted(boolean casted) {
            isCasted = casted;
        }

        private PyCallableDetails(@Nullable String pythonScopeExpr, @NotNull String pythonMethodName) {
            this.pythonScopeExpr = pythonScopeExpr;
            this.pythonMethodName = pythonMethodName;
        }
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

    /**
     * A placeholder expression used to temporarily take the place of a node whose parent is being changed.
     * <p>
     * This used because the JavaParser sometimes incorrectly unsets a node's parent. For example, if {@code exprA} is
     * the parent of {@code exprB}, and a new node {@code exprX} is added as the parent of {@code exprB}, then
     * {@code exprX} will automatically become the parent of {@code exprB}, but setting {@code exprX} as the child of
     * {@code exprA} may cause {@code exprA} to descend into its (former) child {@code exprB} and set its parent to null
     * (instead of keeping {@code exprB}'s parent as {@code exprX}).
     */
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
