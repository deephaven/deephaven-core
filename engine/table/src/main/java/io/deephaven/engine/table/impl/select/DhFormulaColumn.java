//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.Pair;
import io.deephaven.chunk.ChunkType;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer;
import io.deephaven.engine.table.impl.select.codegen.JavaKernelBuilder;
import io.deephaven.engine.table.impl.select.codegen.RichType;
import io.deephaven.engine.table.impl.select.formula.FormulaFactory;
import io.deephaven.engine.table.impl.select.formula.FormulaKernelFactory;
import io.deephaven.engine.table.impl.select.formula.FormulaSourceDescriptor;
import io.deephaven.engine.table.impl.select.python.ArgumentsChunked;
import io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction;
import io.deephaven.engine.table.impl.select.python.FormulaColumnPython;
import io.deephaven.engine.table.impl.util.codegen.CodeGenerator;
import io.deephaven.engine.table.impl.util.codegen.TypeAnalyzer;
import io.deephaven.engine.util.PyCallableWrapperJpyImpl;
import io.deephaven.engine.util.caching.C14nUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.VectorFactory;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.deephaven.engine.util.IterableUtils.makeCommaSeparatedList;

public class DhFormulaColumn extends AbstractFormulaColumn {
    private static final Logger log = LoggerFactory.getLogger(DhFormulaColumn.class);
    private static final String COLUMN_SOURCE_CLASSNAME = ColumnSource.class.getCanonicalName();
    private static final String C14NUTIL_CLASSNAME = C14nUtil.class.getCanonicalName();
    private static final String LAZY_RESULT_CACHE_NAME = "__lazyResultCache";
    private static final String FORMULA_FACTORY_NAME = "__FORMULA_FACTORY";
    private static final String PARAM_CLASSNAME = QueryScopeParam.class.getCanonicalName();
    private static final String EVALUATION_EXCEPTION_CLASSNAME = FormulaEvaluationException.class.getCanonicalName();
    public static boolean useKernelFormulasProperty =
            Configuration.getInstance().getBooleanWithDefault("FormulaColumn.useKernelFormulasProperty", false);

    private FormulaAnalyzer.Result analyzedFormula;
    private boolean hasConstantValue;
    private Pair<String, Map<Long, List<MatchPair>>> formulaShiftColPair;

    public FormulaColumnPython getFormulaColumnPython() {
        return formulaColumnPython;
    }

    private FormulaColumnPython formulaColumnPython;

    /**
     * Create a formula column for the given formula string.
     * <p>
     * The internal formula object is generated on-demand by calling out to the Java compiler.
     *
     * @param columnName the result column name
     * @param formulaString the formula string to be parsed by the QueryLanguageParser
     */
    DhFormulaColumn(String columnName, String formulaString) {
        super(columnName, formulaString);
    }

    /**
     * Returns the name of a primitive-type-specific getter, e.g. {@code getDouble()}, {@code getPrevDouble()},
     * {@code getInt()}, {@code getPrevInt()}, etc.
     *
     * @param type The return type
     * @param prev {@code true} for get(), {@code false} for getPrev()
     * @return An appropriate name for a getter
     */
    private static String getGetterName(Class<?> type, boolean prev) {
        final Class<?> unboxedType =
                (type.isPrimitive() ? type : io.deephaven.util.type.TypeUtils.getUnboxedType(type));
        final String get = prev ? "getPrev" : "get";
        if (unboxedType == null) {
            return get;
        } else {
            return get + Character.toUpperCase(unboxedType.getName().charAt(0)) + unboxedType.getName().substring(1);
        }
    }

    private static <T> void addIfNotNull(List<T> list, T item) {
        if (item != null) {
            list.add(item);
        }
    }

    private static String columnSourceGetMethodReturnType(ColumnDefinition<?> cd) {
        final StringBuilder sb = new StringBuilder();
        Class<?> columnType = cd.getDataType();
        if (columnType == boolean.class) {
            columnType = Boolean.class;
        }
        sb.append(columnType.getCanonicalName());
        final Class<?> componentType = cd.getComponentType();
        if (componentType != null && !componentType.isPrimitive() && columnType.getTypeParameters().length == 1) {
            sb.append("<").append(componentType.getCanonicalName()).append(">");
        }
        return sb.toString();
    }

    private static Map<String, RichType> makeNameToRichTypeDict(
            final String[] names, final Map<String, ? extends ColumnDefinition<?>> columnDefinitions) {
        final Map<String, RichType> result = new HashMap<>();
        for (final String s : names) {
            final RichType richType;
            if (s.equals("i")) {
                richType = RichType.createNonGeneric(int.class);
            } else if (s.equals("ii") || s.equals("k")) {
                richType = RichType.createNonGeneric(long.class);
            } else {
                final ColumnDefinition<?> cs = columnDefinitions.get(s);
                Class<?> columnType = cs.getDataType();
                if (columnType == boolean.class) {
                    columnType = Boolean.class;
                }
                final Class<?> componentType = cs.getComponentType();
                if (componentType != null && !componentType.isPrimitive()
                        && columnType.getTypeParameters().length == 1) {
                    richType = RichType.createGeneric(columnType, componentType);
                } else {
                    richType = RichType.createNonGeneric(columnType);
                }
            }
            result.put(s, richType);
        }
        return result;
    }

    private static Map<String, Class<?>> makeNameToTypeDict(final String[] names,
            final Map<String, ? extends ColumnDefinition<?>> columnDefinitions) {
        final Map<String, Class<?>> result = new HashMap<>();
        for (final String s : names) {
            final ColumnDefinition<?> cd = columnDefinitions.get(s);
            result.put(s, cd.getDataType());
        }
        return result;
    }

    public static Class<?> getVectorType(Class<?> declaredType) {
        return VectorFactory.forElementType(declaredType).vectorType();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return initDef(columnDefinitionMap, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public List<String> initDef(
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        if (formulaFactoryFuture != null) {
            validateColumnDefinition(columnDefinitionMap);
            return formulaColumnPython != null ? formulaColumnPython.usedColumns : usedColumns;
        }

        try {
            final QueryLanguageParser.Result result = FormulaAnalyzer.parseFormula(
                    formulaString, columnDefinitionMap, Collections.emptyMap(),
                    compilationRequestProcessor.getFormulaImports());
            analyzedFormula = FormulaAnalyzer.analyze(formulaString, columnDefinitionMap, result);
            hasConstantValue = result.isConstantValueExpression();
            formulaShiftColPair = result.getFormulaShiftColPair();

            log.debug().append("Expression (after language conversion) : ").append(analyzedFormula.cookedFormulaString)
                    .endl();

            applyUsedVariables(columnDefinitionMap, result.getVariablesUsed(), result.getPossibleParams());
            returnedType = result.getType();
            if (returnedType == boolean.class) {
                returnedType = Boolean.class;
            }
            formulaString = result.getConvertedExpression();

            // check if this is a column to be created with a Python vectorizable function
            checkAndInitializeVectorization(columnDefinitionMap, compilationRequestProcessor);
        } catch (Exception e) {
            throw new FormulaCompilationException("Formula compilation error for: " + originalFormulaString, e);
        }

        if (useKernelFormulasProperty) {
            formulaFactoryFuture = createKernelFormulaFactory(getFormulaKernelFactory(compilationRequestProcessor));
        } else {
            compileFormula(compilationRequestProcessor);
        }
        return formulaColumnPython != null ? formulaColumnPython.usedColumns : usedColumns;
    }

    private void checkAndInitializeVectorization(
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap,
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        // noinspection SuspiciousToArrayCall
        final PyCallableWrapperJpyImpl[] cws = Arrays.stream(params)
                .filter(p -> p.getValue() instanceof PyCallableWrapperJpyImpl)
                .map(QueryScopeParam::getValue)
                .toArray(PyCallableWrapperJpyImpl[]::new);
        if (cws.length != 1) {
            return;
        }
        final PyCallableWrapperJpyImpl pyCallableWrapper = cws[0];

        // could be already vectorized or to-be-vectorized,
        if (pyCallableWrapper.isVectorizable()) {
            ArgumentsChunked argumentsChunked = pyCallableWrapper
                    .buildArgumentsChunked(Arrays.asList(this.analyzedFormula.sourceDescriptor.sources));
            PyObject vectorized = pyCallableWrapper.vectorizedCallable();
            formulaColumnPython = FormulaColumnPython.create(this.columnName,
                    DeephavenCompatibleFunction.create(vectorized,
                            pyCallableWrapper.getSignature().getReturnType(),
                            this.analyzedFormula.sourceDescriptor.sources,
                            argumentsChunked,
                            true));
            formulaColumnPython.initDef(columnDefinitionMap, compilationRequestProcessor);
        }
    }

    @NotNull
    String generateClassBody() {
        final TypeAnalyzer ta = TypeAnalyzer.create(returnedType);

        final CodeGenerator g = CodeGenerator.create(
                CodeGenerator.create(ExecutionContext.getContext().getQueryLibrary().getImportStrings().toArray()), "",
                "public class $CLASSNAME$ extends [[FORMULA_CLASS_NAME]]", CodeGenerator.block(
                        generateFormulaFactoryLambda(), "",
                        "private final String __columnName;",
                        CodeGenerator.repeated("instanceVar", "private final [[TYPE]] [[NAME]];"),
                        "private final Map<Object, Object> [[LAZY_RESULT_CACHE_NAME]];",
                        analyzedFormula.timeInstanceVariables, "",
                        generateConstructor(), "",
                        generateAppropriateGetMethod(ta, false), "",
                        generateAppropriateGetMethod(ta, true), "",
                        generateOptionalObjectGetMethod(ta, false),
                        generateOptionalObjectGetMethod(ta, true),
                        generateGetChunkType(ta), "",
                        generateFillChunk(false), "",
                        generateFillChunk(true), "",
                        generateFillChunkHelper(ta), "",
                        generateApplyFormulaPerItem(ta), "",
                        generateMakeFillContext(), "",
                        generateNormalContextClass(), "",
                        generateIntSize()),
                "");
        g.replace("FORMULA_CLASS_NAME", Formula.class.getCanonicalName());
        g.replace("LAZY_RESULT_CACHE_NAME", LAZY_RESULT_CACHE_NAME);
        visitFormulaParameters(null,
                cs -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("instanceVar");
                    fc.replace("TYPE", cs.columnSourceGetTypeString);
                    fc.replace("NAME", cs.name);
                    return null;
                },
                ca -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("instanceVar");
                    fc.replace("TYPE", ca.vectorTypeString);
                    fc.replace("NAME", ca.name);
                    return null;
                },
                p -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("instanceVar");
                    fc.replace("TYPE", p.typeString);
                    fc.replace("NAME", p.name);
                    return null;
                });
        return g.build();
    }

    private CodeGenerator generateFormulaFactoryLambda() {
        final CodeGenerator g = CodeGenerator.create(
                "public static final [[FORMULA_FACTORY]] [[FORMULA_FACTORY_NAME]] = $CLASSNAME$::new;");
        g.replace("FORMULA_FACTORY", FormulaFactory.class.getCanonicalName());
        g.replace("FORMULA_FACTORY_NAME", FORMULA_FACTORY_NAME);
        return g.freeze();
    }

    private CodeGenerator generateConstructor() {
        final CodeGenerator g = CodeGenerator.create(
                "public $CLASSNAME$(final String __columnName,", CodeGenerator.indent(
                        "final TrackingRowSet __rowSet,",
                        "final boolean __lazy,",
                        "final java.util.Map<String, ? extends [[COLUMN_SOURCE_CLASSNAME]]> __columnsToData,",
                        "final [[PARAM_CLASSNAME]]... __params)"),
                CodeGenerator.block(
                        "super(__rowSet);",
                        "this.__columnName = __columnName;",
                        CodeGenerator.repeated("initColumn",
                                "[[COLUMN_NAME]] = __columnsToData.get(\"[[COLUMN_NAME]]\");"),
                        CodeGenerator.repeated("initNormalColumnArray",
                                "[[COLUMN_ARRAY_NAME]] = new [[VECTOR_TYPE_PREFIX]]ColumnWrapper(__columnsToData.get(\"[[COLUMN_NAME]]\"), __rowSet);"),
                        CodeGenerator.repeated("initParam",
                                "[[PARAM_NAME]] = ([[PARAM_TYPE]]) __params[[[PARAM_INDEX]]].getValue();"),
                        "[[LAZY_RESULT_CACHE_NAME]] = __lazy ? new ConcurrentHashMap<>() : null;"));

        g.replace("LAZY_RESULT_CACHE_NAME", LAZY_RESULT_CACHE_NAME);
        g.replace("COLUMN_SOURCE_CLASSNAME", COLUMN_SOURCE_CLASSNAME);
        g.replace("PARAM_CLASSNAME", PARAM_CLASSNAME);
        visitFormulaParameters(null,
                cs -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("initColumn");
                    fc.replace("COLUMN_NAME", cs.name);
                    return null;
                },
                ac -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("initNormalColumnArray");
                    fc.replace("COLUMN_ARRAY_NAME", ac.name);
                    fc.replace("COLUMN_NAME", ac.bareName);

                    final String vtp = getVectorType(ac.columnDefinition.getDataType()).getCanonicalName().replace(
                            "io.deephaven.vector",
                            "io.deephaven.engine.table.vectors");
                    fc.replace("VECTOR_TYPE_PREFIX", vtp);
                    return null;
                },
                p -> {
                    final CodeGenerator fc = g.instantiateNewRepeated("initParam");
                    fc.replace("PARAM_NAME", p.name);
                    fc.replace("PARAM_TYPE", p.typeString);
                    fc.replace("PARAM_INDEX", p.index + "");
                    return null;
                });

        return g.freeze();
    }

    private CodeGenerator generateApplyFormulaPerItem(final TypeAnalyzer ta) {
        final CodeGenerator g = CodeGenerator.create(
                "private [[RETURN_TYPE]] applyFormulaPerItem([[ARGS]])", CodeGenerator.block(
                        "try", CodeGenerator.block(
                                "return [[FORMULA_STRING]];"),
                        CodeGenerator.samelineBlock("catch (java.lang.Exception __e)",
                                "throw new [[EXCEPTION_TYPE]](\"In formula: \" + __columnName + \" = \" + [[JOINED_FORMULA_STRING]], __e);")));
        g.replace("RETURN_TYPE", ta.typeString);
        final List<String> args = visitFormulaParameters(n -> n.typeString + " " + n.name,
                n -> n.typeString + " " + n.name,
                null,
                null);
        g.replace("ARGS", makeCommaSeparatedList(args));
        g.replace("FORMULA_STRING", ta.wrapWithCastIfNecessary(formulaString));
        final String joinedFormulaString = QueryCompilerImpl.createEscapedJoinedString(originalFormulaString);
        g.replace("JOINED_FORMULA_STRING", joinedFormulaString);
        g.replace("EXCEPTION_TYPE", EVALUATION_EXCEPTION_CLASSNAME);
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateAppropriateGetMethod(TypeAnalyzer ta, boolean usePrev) {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public [[RETURN_TYPE]] [[GETTER_NAME]](final long k)", CodeGenerator.block(
                        (usePrev
                                ? CodeGenerator.optional("maybeCreateIorII",
                                        "final long findResult;",
                                        "try (final RowSet prev = __rowSet.copyPrev())", CodeGenerator.block(
                                                "findResult = prev.find(k);"))
                                : CodeGenerator.optional("maybeCreateIorII",
                                        "final long findResult = __rowSet.find(k);")),
                        CodeGenerator.optional("maybeCreateI",
                                "final int i = __intSize(findResult);"),
                        CodeGenerator.optional("maybeCreateII",
                                "final long ii = findResult;"),
                        CodeGenerator.repeated("cacheColumnSourceGet", "final [[TYPE]] [[VAR]] = [[GET_EXPRESSION]];"),
                        "if ([[LAZY_RESULT_CACHE_NAME]] != null)", CodeGenerator.block(
                                "final Object __lazyKey = [[C14NUTIL_CLASSNAME]].maybeMakeCompoundKey([[FORMULA_ARGS]]);",
                                "return ([[RESULT_TYPE]])[[LAZY_RESULT_CACHE_NAME]].computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem([[FORMULA_ARGS]]));"),
                        "return applyFormulaPerItem([[FORMULA_ARGS]]);"));
        final String returnTypeString;
        final String resultTypeString;
        if (ta.enginePrimitiveType != null) {
            resultTypeString = returnTypeString = ta.enginePrimitiveType.getName();
        } else {
            returnTypeString = "Object";
            resultTypeString = ta.typeString;
        }
        final String getterName = getGetterName(ta.type, usePrev);
        g.replace("RETURN_TYPE", returnTypeString);
        g.replace("RESULT_TYPE", resultTypeString);
        g.replace("GETTER_NAME", getterName);

        if (usesI || usesII) {
            g.activateOptional("maybeCreateIorII");
        }
        if (usesI) {
            g.activateOptional("maybeCreateI");
        }
        if (usesII) {
            g.activateOptional("maybeCreateII");
        }

        // This visitor initializes variables for the column source gets, and also puts together (via the lambda return
        // values), the names of all the arguments.
        final int[] nextId = {0};
        final List<String> formulaArgs = visitFormulaParameters(idx -> idx.name,
                cs -> {
                    final String cachedName = "__temp" + nextId[0]++;
                    final CodeGenerator cc = g.instantiateNewRepeated("cacheColumnSourceGet");
                    cc.replace("TYPE", cs.typeString);
                    cc.replace("VAR", cachedName);
                    cc.replace("GET_EXPRESSION", cs.makeGetExpression(usePrev));
                    return cachedName;
                },
                null,
                null);

        g.replace("FORMULA_ARGS", makeCommaSeparatedList(formulaArgs));
        g.replace("LAZY_RESULT_CACHE_NAME", LAZY_RESULT_CACHE_NAME);
        g.replace("C14NUTIL_CLASSNAME", C14NUTIL_CLASSNAME);

        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateOptionalObjectGetMethod(TypeAnalyzer ta, boolean usePrev) {
        if (ta.enginePrimitiveType == null) {
            return CodeGenerator.create(); // empty
        }
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public Object [[GETTER_NAME]](final long k)", CodeGenerator.block(
                        "return TypeUtils.box([[DELEGATED_GETTER_NAME]](k));"),
                "" // Extra spacing to get spacing right for my caller (because I am optional)
        );
        final String getterName = usePrev ? "getPrev" : "get";
        final String delegatedGetterName = getGetterName(ta.enginePrimitiveType, usePrev);
        g.replace("GETTER_NAME", getterName);
        g.replace("DELEGATED_GETTER_NAME", delegatedGetterName);
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateNormalContextClass() {
        final CodeGenerator g = CodeGenerator.create(
                "private class FormulaFillContext implements [[FILL_CONTEXT_CANONICAL]]", CodeGenerator.block(
                        // The optional i chunk
                        CodeGenerator.optional("needsIChunk",
                                "private final WritableIntChunk<OrderedRowKeys> __iChunk;"),
                        // The optional ii chunk
                        CodeGenerator.optional("needsIIChunk",
                                "private final WritableLongChunk<OrderedRowKeys> __iiChunk;"),
                        // fields
                        CodeGenerator.repeated("defineField",
                                "private final ColumnSource.GetContext __subContext[[COL_SOURCE_NAME]];"),
                        // constructor
                        "FormulaFillContext(int __chunkCapacity)", CodeGenerator.block(
                                CodeGenerator.optional("needsIChunk",
                                        "__iChunk = WritableIntChunk.makeWritableChunk(__chunkCapacity);"),
                                CodeGenerator.optional("needsIIChunk",
                                        "__iiChunk = WritableLongChunk.makeWritableChunk(__chunkCapacity);"),
                                CodeGenerator.repeated("initField",
                                        "__subContext[[COL_SOURCE_NAME]] = [[COL_SOURCE_NAME]].makeGetContext(__chunkCapacity);")),
                        "",
                        "@Override",
                        "public void close()", CodeGenerator.block(
                                CodeGenerator.optional("needsIChunk", "__iChunk.close();"),
                                CodeGenerator.optional("needsIIChunk", "__iiChunk.close();"),
                                CodeGenerator.repeated("closeField", "__subContext[[COL_SOURCE_NAME]].close();"))));
        g.replace("FILL_CONTEXT_CANONICAL", Formula.FillContext.class.getCanonicalName());
        if (usesI) {
            g.activateAllOptionals("needsIChunk");
        }
        if (usesII) {
            g.activateAllOptionals("needsIIChunk");
        }
        visitFormulaParameters(null,
                cs -> {
                    final CodeGenerator defineField = g.instantiateNewRepeated("defineField");
                    final CodeGenerator initField = g.instantiateNewRepeated("initField");
                    final CodeGenerator closeField = g.instantiateNewRepeated("closeField");
                    defineField.replace("COL_SOURCE_NAME", cs.name);
                    initField.replace("COL_SOURCE_NAME", cs.name);
                    closeField.replace("COL_SOURCE_NAME", cs.name);
                    return null;
                }, null, null);
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateMakeFillContext() {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public FormulaFillContext makeFillContext(final int __chunkCapacity)", CodeGenerator.block(
                        "return new FormulaFillContext(__chunkCapacity);"));
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateGetChunkType(TypeAnalyzer ta) {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "protected [[CHUNK_TYPE_CLASSNAME]] getChunkType()", CodeGenerator.block(
                        "return [[CHUNK_TYPE_CLASSNAME]].[[CHUNK_TYPE]];"));
        g.replace("CHUNK_TYPE_CLASSNAME", ChunkType.class.getCanonicalName());
        g.replace("CHUNK_TYPE", ta.chunkTypeString);
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateFillChunk(boolean usePrev) {
        final CodeGenerator g = CodeGenerator.create(
                "@Override",
                "public void [[FILL_METHOD]](final FillContext __context, final WritableChunk<? super Values> __destination, final RowSequence __rowSequence)",
                CodeGenerator.block(
                        "final FormulaFillContext __typedContext = (FormulaFillContext)__context;",
                        CodeGenerator.repeated("getChunks",
                                "final [[CHUNK_TYPE]] __chunk__col__[[COL_SOURCE_NAME]] = this.[[COL_SOURCE_NAME]].[[GET_CURR_OR_PREV_CHUNK]]("
                                        +
                                        "__typedContext.__subContext[[COL_SOURCE_NAME]], __rowSequence).[[AS_CHUNK_METHOD]]();"),
                        "fillChunkHelper(" + usePrev
                                + ", __typedContext, __destination, __rowSequence[[ADDITIONAL_CHUNK_ARGS]]);"));

        final String fillMethodName = String.format("fill%sChunk", usePrev ? "Prev" : "");
        g.replace("FILL_METHOD", fillMethodName);
        List<String> chunkList = visitFormulaParameters(null,
                cs -> {
                    final CodeGenerator getChunks = g.instantiateNewRepeated("getChunks");
                    getChunks.replace("COL_SOURCE_NAME", cs.name);
                    getChunks.replace("GET_CURR_OR_PREV_CHUNK", usePrev ? "getPrevChunk" : "getChunk");
                    final TypeAnalyzer tm = TypeAnalyzer.create(cs.columnDefinition.getDataType());
                    getChunks.replace("CHUNK_TYPE", tm.readChunkVariableType);
                    getChunks.replace("AS_CHUNK_METHOD", tm.asReadChunkMethodName);
                    return "__chunk__col__" + cs.name;
                },
                null, null);
        final String additionalChunkArgs = chunkList.isEmpty() ? "" : ", " + makeCommaSeparatedList(chunkList);
        g.replace("ADDITIONAL_CHUNK_ARGS", additionalChunkArgs);
        return g.freeze();
    }

    @NotNull
    private CodeGenerator generateFillChunkHelper(TypeAnalyzer ta) {
        final CodeGenerator g = CodeGenerator.create(
                "private void fillChunkHelper(final boolean __usePrev, final FormulaFillContext __context,",
                CodeGenerator.indent(
                        "final WritableChunk<? super Values> __destination,",
                        "final RowSequence __rowSequence[[ADDITIONAL_CHUNK_ARGS]])"),
                CodeGenerator.block(
                        "final [[DEST_CHUNK_TYPE]] __typedDestination = __destination.[[DEST_AS_CHUNK_METHOD]]();",
                        CodeGenerator.optional("maybeCreateIOrII",
                                "try (final RowSet prev = __usePrev ? __rowSet.copyPrev() : null;",
                                CodeGenerator.indent(
                                        "final RowSet inverted = ((prev != null) ? prev : __rowSet).invert(__rowSequence.asRowSet()))"),
                                CodeGenerator.block(
                                        CodeGenerator.optional("maybeCreateI",
                                                "__context.__iChunk.setSize(0);",
                                                "inverted.forAllRowKeys(l -> __context.__iChunk.add(__intSize(l)));"),
                                        CodeGenerator.optional("maybeCreateII",
                                                "inverted.fillRowKeyChunk(__context.__iiChunk);"))),
                        CodeGenerator.repeated("getChunks",
                                "final [[CHUNK_TYPE]] __chunk__col__[[COL_SOURCE_NAME]] = __sources[[[SOURCE_INDEX]]].[[AS_CHUNK_METHOD]]();"),
                        "final int[] __chunkPosHolder = new int[] {0};",
                        "if ([[LAZY_RESULT_CACHE_NAME]] != null)", CodeGenerator.block(
                                "__rowSequence.forAllRowKeys(k ->", CodeGenerator.block(
                                        "final int __chunkPos = __chunkPosHolder[0]++;",
                                        CodeGenerator.optional("maybeCreateI",
                                                "final int i = __context.__iChunk.get(__chunkPos);"),
                                        CodeGenerator.optional("maybeCreateII",
                                                "final long ii = __context.__iiChunk.get(__chunkPos);"),
                                        "final Object __lazyKey = [[C14NUTIL_CLASSNAME]].maybeMakeCompoundKey([[APPLY_FORMULA_ARGS]]);",
                                        "__typedDestination.set(__chunkPos, ([[RESULT_TYPE]])[[LAZY_RESULT_CACHE_NAME]].computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem([[APPLY_FORMULA_ARGS]])));"),
                                ");" // close the lambda
                        ), CodeGenerator.samelineBlock("else",
                                "__rowSequence.forAllRowKeys(k ->", CodeGenerator.block(
                                        "final int __chunkPos = __chunkPosHolder[0]++;",
                                        CodeGenerator.optional("maybeCreateI",
                                                "final int i = __context.__iChunk.get(__chunkPos);"),
                                        CodeGenerator.optional("maybeCreateII",
                                                "final long ii = __context.__iiChunk.get(__chunkPos);"),
                                        "__typedDestination.set(__chunkPos, applyFormulaPerItem([[APPLY_FORMULA_ARGS]]));"),
                                ");" // close the lambda
                        ),
                        "__typedDestination.setSize(__chunkPosHolder[0]);"

                ));

        g.replace("DEST_CHUNK_TYPE", ta.writableChunkVariableType);
        g.replace("DEST_AS_CHUNK_METHOD", ta.asWritableChunkMethodName);
        final List<String> chunkArgs = visitFormulaParameters(null,
                cs -> {
                    final String name = "__chunk__col__" + cs.name;
                    final TypeAnalyzer t2 = TypeAnalyzer.create(cs.columnDefinition.getDataType());
                    return t2.readChunkVariableType + " " + name;
                },
                null,
                null);
        final String additionalChunkArgs = chunkArgs.isEmpty() ? "" : ", " + makeCommaSeparatedList(chunkArgs);
        g.replace("ADDITIONAL_CHUNK_ARGS", additionalChunkArgs);
        if (usesI || usesII) {
            g.activateOptional("maybeCreateIOrII");
        }
        if (usesI) {
            g.activateAllOptionals("maybeCreateI");
        }
        if (usesII) {
            g.activateAllOptionals("maybeCreateII");
        }
        final List<String> applyFormulaArgs = visitFormulaParameters(ix -> ix.name,
                p -> String.format("__chunk__col__%s.get(%s)", p.name, "__chunkPos"),
                null,
                null);
        g.replace("APPLY_FORMULA_ARGS", makeCommaSeparatedList(applyFormulaArgs));

        g.replace("RESULT_TYPE", ta.enginePrimitiveType != null ? ta.enginePrimitiveType.getName() : ta.typeString);
        g.replace("LAZY_RESULT_CACHE_NAME", LAZY_RESULT_CACHE_NAME);
        g.replace("C14NUTIL_CLASSNAME", C14NUTIL_CLASSNAME);

        return g.freeze();
    }

    private CodeGenerator generateIntSize() {
        final CodeGenerator g = CodeGenerator.create(
                "private int __intSize(final long l)", CodeGenerator.block(
                        "return LongSizedDataStructure.intSize(\"FormulaColumn i usage\", l);"));
        return g.freeze();
    }

    private <T> List<T> visitFormulaParameters(
            Function<IndexParameter, T> indexLambda,
            Function<ColumnSourceParameter, T> columnSourceLambda,
            Function<ColumnArrayParameter, T> columnArrayLambda,
            Function<ParamParameter, T> paramLambda) {
        final List<T> results = new ArrayList<>();
        if (indexLambda != null) {
            if (usesI) {
                final IndexParameter ip = new IndexParameter("i", int.class, "int");
                addIfNotNull(results, indexLambda.apply(ip));
            }

            if (usesII) {
                final IndexParameter ip = new IndexParameter("ii", long.class, "long");
                addIfNotNull(results, indexLambda.apply(ip));
            }

            if (usesK) {
                final IndexParameter ip = new IndexParameter("k", long.class, "long");
                addIfNotNull(results, indexLambda.apply(ip));
            }
        }

        if (columnSourceLambda != null) {
            for (String usedColumn : usedColumns) {
                final ColumnDefinition<?> cd = columnDefinitions.get(usedColumn);
                final String columnSourceGetType = columnSourceGetMethodReturnType(cd);
                final Class<?> csType = cd.getDataType();
                final String csTypeString = COLUMN_SOURCE_CLASSNAME + '<'
                        + io.deephaven.util.type.TypeUtils.getBoxedType(csType).getCanonicalName() + '>';
                final ColumnSourceParameter csp = new ColumnSourceParameter(usedColumn, csType, columnSourceGetType,
                        cd, csTypeString);
                addIfNotNull(results, columnSourceLambda.apply(csp));
            }
        }

        if (columnArrayLambda != null) {
            for (String uca : usedColumnArrays) {
                final ColumnDefinition<?> cd = columnDefinitions.get(uca);
                final Class<?> dataType = cd.getDataType();
                final Class<?> vectorType = getVectorType(dataType);
                final String vectorTypeAsString = vectorType.getCanonicalName() +
                        (TypeUtils.isConvertibleToPrimitive(dataType) ? "" : "<" + dataType.getCanonicalName() + ">");
                final ColumnArrayParameter cap = new ColumnArrayParameter(uca + COLUMN_SUFFIX, uca,
                        dataType, vectorType, vectorTypeAsString, cd);
                addIfNotNull(results, columnArrayLambda.apply(cap));
            }
        }

        if (paramLambda != null) {
            for (int ii = 0; ii < params.length; ++ii) {
                final QueryScopeParam<?> p = params[ii];
                final ParamParameter pp = new ParamParameter(ii, p.getName(),
                        QueryScopeParamTypeUtil.getDeclaredClass(p.getValue()),
                        QueryScopeParamTypeUtil.getDeclaredTypeName(p.getValue()));
                addIfNotNull(results, paramLambda.apply(pp));
            }
        }
        return results;
    }

    protected FormulaSourceDescriptor getSourceDescriptor() {
        return analyzedFormula.sourceDescriptor;
    }

    protected CompletionStageFuture<FormulaKernelFactory> getFormulaKernelFactory(
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        return invokeKernelBuilder(compilationRequestProcessor).thenApply(result -> result.formulaKernelFactory);
    }

    private CompletionStageFuture<JavaKernelBuilder.Result> invokeKernelBuilder(
            @NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        final FormulaAnalyzer.Result af = analyzedFormula;
        final FormulaSourceDescriptor sd = af.sourceDescriptor;
        final Map<String, RichType> columnDict = makeNameToRichTypeDict(sd.sources, columnDefinitions);
        final Map<String, Class<?>> arrayDict = makeNameToTypeDict(sd.arrays, columnDefinitions);
        final Map<String, Class<?>> allParamDict = new HashMap<>();
        for (final QueryScopeParam<?> param : params) {
            allParamDict.put(param.getName(), QueryScopeParamTypeUtil.getDeclaredClass(param.getValue()));
        }
        final Map<String, Class<?>> paramDict = new HashMap<>();
        for (final String p : sd.params) {
            paramDict.put(p, allParamDict.get(p));
        }
        return JavaKernelBuilder.create(
                originalFormulaString,
                af.cookedFormulaString,
                sd.returnType,
                af.timeInstanceVariables,
                columnDict,
                arrayDict,
                paramDict,
                compilationRequestProcessor);
    }

    /**
     * For unit testing.
     */
    @NotNull
    String generateKernelClassBody() {
        try {
            return invokeKernelBuilder(QueryCompilerRequestProcessor.immediate()).get().classBody;
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException("Failed to compile formula: ", e);
        }
    }

    @Override
    public SelectColumn copy() {
        final DhFormulaColumn copy = new DhFormulaColumn(columnName, formulaString);
        if (formulaFactoryFuture != null) {
            copy.analyzedFormula = analyzedFormula;
            copy.hasConstantValue = hasConstantValue;
            copy.returnedType = returnedType;
            copy.formulaColumnPython = formulaColumnPython;
            copy.formulaShiftColPair = formulaShiftColPair;
            onCopy(copy);
        }
        return copy;
    }

    @Override
    public boolean hasConstantValue() {
        return hasConstantValue;
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> getFormulaShiftColPair() {
        return formulaShiftColPair;
    }

    private void compileFormula(@NotNull final QueryCompilerRequestProcessor compilationRequestProcessor) {
        final String what = "Compile regular formula: " + formulaString;
        final String className = "Formula";
        final String classBody = generateClassBody();

        final List<Class<?>> paramClasses = new ArrayList<>();
        final Consumer<Class<?>> addParamClass = (cls) -> {
            if (cls != null) {
                paramClasses.add(cls);
            }
        };
        visitFormulaParameters(null,
                csp -> {
                    addParamClass.accept(csp.type);
                    addParamClass.accept(csp.columnDefinition.getComponentType());
                    return null;
                },
                cap -> {
                    addParamClass.accept(cap.dataType);
                    addParamClass.accept(cap.columnDefinition.getComponentType());
                    return null;
                },
                p -> {
                    addParamClass.accept(p.type);
                    return null;
                });

        formulaFactoryFuture = compilationRequestProcessor.submit(QueryCompilerRequest.builder()
                .description("Formula Expression: " + formulaString)
                .className(className)
                .classBody(classBody)
                .packageNameRoot(QueryCompilerImpl.FORMULA_CLASS_PREFIX)
                .putAllParameterClasses(QueryScopeParamTypeUtil.expandParameterClasses(paramClasses))
                .build()).thenApply(clazz -> {
                    try {
                        return (FormulaFactory) clazz.getField(FORMULA_FACTORY_NAME).get(null);
                    } catch (ReflectiveOperationException e) {
                        throw new FormulaCompilationException("Formula compilation error for: " + what, e);
                    }
                });
    }

    private static class IndexParameter {
        final String name;
        final Class<?> type;
        final String typeString;

        public IndexParameter(String name, Class<?> type, String typeString) {
            this.name = name;
            this.type = type;
            this.typeString = typeString;
        }
    }

    private static class ColumnSourceParameter {
        final String name;
        final Class<?> type;
        final String typeString;
        final ColumnDefinition<?> columnDefinition;
        final String columnSourceGetTypeString;

        public ColumnSourceParameter(String name, Class<?> type, String typeString,
                ColumnDefinition<?> columnDefinition, String columnSourceGetTypeString) {
            this.name = name;
            this.type = type;
            this.typeString = typeString;
            this.columnDefinition = columnDefinition;
            this.columnSourceGetTypeString = columnSourceGetTypeString;
        }

        String makeGetExpression(boolean usePrev) {
            return String.format("%s.%s(k)", name, getGetterName(columnDefinition.getDataType(), usePrev));
        }
    }

    /**
     * Is this parameter immutable, and thus would contribute no state to the formula?
     * <p>
     * If any query scope parameter is not a primitive, String, or known immutable class; then it may be a mutable
     * object that results in undefined results when the column is not evaluated strictly in order.
     *
     * @return true if this query scope parameter is immutable
     */
    private static boolean isImmutableType(QueryScopeParam<?> param) {
        final Object value = param.getValue();
        if (value == null) {
            return true;
        }
        final Class<?> type = value.getClass();
        if (type == String.class || type == BigInteger.class || type == BigDecimal.class
                || type == Instant.class || type == ZonedDateTime.class || Table.class.isAssignableFrom(type)) {
            return true;
        }
        // if it is a boxed type, then it is immutable; otherwise we don't know what to do with it
        return TypeUtils.isBoxedType(type);
    }

    private boolean isUsedColumnStateless(String columnName) {
        return columnSources.get(columnName).isStateless();
    }

    @Override
    public boolean isStateless() {
        return Arrays.stream(params).allMatch(DhFormulaColumn::isImmutableType)
                && usedColumns.stream().allMatch(this::isUsedColumnStateless)
                && usedColumnArrays.stream().allMatch(this::isUsedColumnStateless);
    }

}
