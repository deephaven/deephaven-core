package io.deephaven.replicators;

import io.deephaven.base.ClassUtil;
import io.deephaven.util.text.Indenter;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generator for a library of TupleSource classes.
 */
public class TupleSourceCodeGenerator {

    private static final String OUTPUT_PACKAGE = "io.deephaven.engine.table.impl.tuplesource.generated";
    private static final File OUTPUT_RELATIVE_PATH =
            new File("engine/tuplesource/src/main/java/io/deephaven/engine/table/impl/tuplesource/generated");

    private static final String CS = "$cs$";
    private static final String VAL = "$val$";

    private static final String NEW_LINE = System.getProperty("line.separator");

    private static final String[] DEFAULT_IMPORTS = new String[] {
            "org.jetbrains.annotations.NotNull",
            "io.deephaven.datastructures.util.SmartKey",
            "io.deephaven.engine.table.TupleSource",
            "io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource",
            "io.deephaven.engine.table.ColumnSource",
            "io.deephaven.engine.table.WritableColumnSource",
            "io.deephaven.chunk.attributes.Values",
            "io.deephaven.chunk.Chunk",
            "io.deephaven.chunk.WritableChunk",
            "io.deephaven.chunk.WritableObjectChunk",
    };

    private static final String CLASS_NAME_SUFFIX = "ColumnTupleSource";
    private static final String CS1 = "columnSource1";
    private static final String CS2 = "columnSource2";
    private static final String CS3 = "columnSource3";
    private static final String RK = "rowKey";

    enum ColumnSourceType {

        // @formatter:off
              BYTE(                 "Byte", "byte",                              null,         null, false,                                 CS + ".getByte("    + RK + ")" ,                                 CS + ".getPrevByte("    + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Byte)"      + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
             SHORT(                "Short", "short",                             null,         null, false,                                 CS + ".getShort("   + RK + ")" ,                                 CS + ".getPrevShort("   + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Short)"     + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
               INT(              "Integer", "int",                               null,         null, false,                                 CS + ".getInt("     + RK + ")" ,                                 CS + ".getPrevInt("     + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Integer)"   + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
              LONG(                 "Long", "long",                              null,         null, false,                                 CS + ".getLong("    + RK + ")" ,                                 CS + ".getPrevLong("    + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Long)"      + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
             FLOAT(                "Float", "float",                              null,         null, false,                                 CS + ".getFloat("   + RK + ")" ,                                 CS + ".getPrevFloat("   + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Float)"     + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
            DOUBLE(               "Double", "double",                            null,         null, false,                                 CS + ".getDouble("  + RK + ")" ,                                 CS + ".getPrevDouble("  + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Double)"    + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
              CHAR(            "Character", "char",                              null,         null, false,                                 CS + ".getChar("    + RK + ")" ,                                 CS + ".getPrevChar("    + RK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Character)" + VAL, "io.deephaven.util.type.TypeUtils"                                                                               ),
            OBJECT(               "Object", "java.lang.Object",                  null,         null, false,                                 CS + ".get("        + RK + ")" ,                                 CS + ".getPrev("        + RK + ")" ,                                 VAL      ,                                    VAL      ,                 VAL                                                                                                                   ),
         R_BOOLEAN( "ReinterpretedBoolean", "byte",                              null,         null,  true,                                 CS + ".getByte("    + RK + ")" ,                                 CS + ".getPrevByte("    + RK + ")" , "BooleanUtils.byteAsBoolean(" + VAL + ')', "BooleanUtils.booleanAsByte("    + VAL + ')', "(Boolean)"   + VAL, "io.deephaven.util.type.TypeUtils", "io.deephaven.util.BooleanUtils"                                             ),
           BOOLEAN(              "Boolean", "java.lang.Boolean",                 "byte",  R_BOOLEAN, false, "BooleanUtils.booleanAsByte(" + CS + ".getBoolean(" + RK + "))", "BooleanUtils.booleanAsByte(" + CS + ".getPrevBoolean(" + RK + "))", "BooleanUtils.byteAsBoolean(" + VAL + ')', "BooleanUtils.booleanAsByte("    + VAL + ')', "(Boolean)"   + VAL, "io.deephaven.util.BooleanUtils"                                                                                 ),
        R_DATETIME("ReinterpretedDateTime", "long",                              null,         null,  true,                                 CS + ".getLong("    + RK + ")" ,                                 CS + ".getPrevLong("    + RK + ")" , "DateTimeUtils.nanosToTime("  + VAL + ')', "DateTimeUtils.nanos("           + VAL + ')', "(DateTime)"  + VAL, "io.deephaven.util.type.TypeUtils", "io.deephaven.time.DateTime", "io.deephaven.time.DateTimeUtils"),
          DATETIME(             "DateTime", "io.deephaven.time.DateTime", "long", R_DATETIME, false,        "DateTimeUtils.nanos(" + CS + ".get("        + RK + "))",        "DateTimeUtils.nanos(" + CS + ".getPrev("        + RK + "))", "DateTimeUtils.nanosToTime("  + VAL + ')', "DateTimeUtils.nanos("           + VAL + ')', "(DateTime)"  + VAL, "io.deephaven.time.DateTime","io.deephaven.time.DateTimeUtils"                                     ),
        ;
        // @formatter:on

        private final String nameText;
        private final String elementClassName;
        private final String internalClassName;
        private final ColumnSourceType reinterpretAsType;
        private final boolean isReinterpreted;
        private final String elementGetter;
        private final String elementPrevGetter;
        private final String boxing;
        private final String unboxing;
        private final String cast;
        private final String[] imports;
        private final boolean isPrimitive;
        private final String chunkClassName;
        private final String chunkTypeName;

        ColumnSourceType(@NotNull final String nameText,
                @NotNull final String elementClassName,
                @Nullable final String overrideInternalClassName,
                @Nullable final ColumnSourceType reinterpretAsType,
                final boolean isReinterpreted,
                @NotNull final String elementGetter,
                @NotNull final String elementPrevGetter,
                @NotNull final String boxing,
                @NotNull final String unboxing,
                @NotNull final String cast,
                @NotNull final String... imports) {
            this.nameText = nameText;
            this.elementClassName = elementClassName;
            this.internalClassName = overrideInternalClassName == null ? elementClassName : overrideInternalClassName;
            this.reinterpretAsType = reinterpretAsType;
            this.isReinterpreted = isReinterpreted;
            this.elementGetter = elementGetter;
            this.elementPrevGetter = elementPrevGetter;
            this.boxing = boxing;
            this.unboxing = unboxing;
            this.cast = cast;
            isPrimitive = !elementClassName.contains(".");
            chunkTypeName = isPrimitive
                    ? elementClassName.substring(0, 1).toUpperCase() + elementClassName.substring(1)
                    : "Object";
            chunkClassName = chunkTypeName + "Chunk";
            this.imports = Stream.concat(Stream.of("io.deephaven.chunk." + chunkClassName), Stream.of(imports))
                    .toArray(String[]::new);
        }

        String getNameText() {
            return nameText;
        }

        String getElementClassName() {
            return elementClassName;
        }

        String getElementClassSimpleName() {
            if (isPrimitive) {
                return elementClassName;
            }
            return elementClassName.substring(elementClassName.lastIndexOf('.') + 1);
        }

        String getElementClassSimpleNameBoxed() {
            if (isPrimitive) {
                try {
                    return TypeUtils.getBoxedType(ClassUtil.lookupClass(elementClassName)).getSimpleName();
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Unrecognized primitive class name " + elementClassName, e);
                }
            }
            return elementClassName.substring(elementClassName.lastIndexOf('.') + 1);
        }

        String getInternalClassName() {
            return internalClassName;
        }

        String getValuesChunkTypeString() {
            if (isPrimitive) {
                return chunkClassName + "<Values>";
            }
            return chunkClassName + '<' + getElementClassSimpleName() + ", Values>";
        }

        ColumnSourceType getReinterpretAsType() {
            return reinterpretAsType;
        }

        boolean isReinterpreted() {
            return isReinterpreted;
        }

        private String getElementGetterText(@NotNull final String columnsSourceName) {
            return elementGetter.replace(CS, columnsSourceName);
        }

        private String getElementPrevGetterText(@NotNull final String columnSourceName) {
            return elementPrevGetter.replace(CS, columnSourceName);
        }

        private String getBoxingText(@NotNull final String valueName) {
            return boxing.replace(VAL, valueName);
        }

        private String getFromChunkText(@NotNull final String valueName) {
            if (internalClassName.equals(elementClassName)) {
                return valueName;
            } else {
                return unboxing.replace(VAL, valueName);
            }
        }

        private String getUnboxingText(@NotNull final String valueName) {
            return unboxing.replace(VAL, cast.replace(VAL, valueName));
        }

        private String getExportElementText(@NotNull final String valueName) {
            return !internalClassName.equals(elementClassName) || isReinterpreted || this == OBJECT
                    ? "(ELEMENT_TYPE) " + getBoxingText(valueName)
                    : valueName;
        }

        private String[] getImports() {
            return imports;
        }
    }

    private static ColumnSourceType forPrimitive(final String elementClassName) {
        return Arrays.stream(ColumnSourceType.values())
                .filter(cst -> cst.getElementClassName().equals(elementClassName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find type for: " + elementClassName));
    }

    private static String generateSimpleClassName(@NotNull final ColumnSourceType... types) {
        return Arrays.stream(types).map(ColumnSourceType::getNameText).collect(Collectors.joining())
                + CLASS_NAME_SUFFIX;
    }

    static String generateClassName(@NotNull final ColumnSourceType... types) {
        return OUTPUT_PACKAGE + '.' + generateSimpleClassName(types);
    }

    private static void addReinterpretedUnboxing(@NotNull ColumnSourceType type1, Indenter indenter, StringBuilder code,
            String valueName, boolean comma) {
        if (type1.isReinterpreted()) {
            code.append(indenter).append(forPrimitive(type1.elementClassName).getUnboxingText(valueName))
                    .append(comma ? "," : "").append(NEW_LINE);
        } else {
            code.append(indenter).append(type1.getUnboxingText(valueName)).append(comma ? "," : "").append(NEW_LINE);
        }
    }

    private static void addReinterpretedExport(@NotNull ColumnSourceType type1, Indenter indenter, StringBuilder code,
            String valueName) {
        if (type1.isReinterpreted()) {
            code.append(indenter.increaseLevel()).append("return ")
                    .append(forPrimitive(type1.elementClassName).getBoxingText(valueName)).append(";").append(NEW_LINE);
        } else {
            code.append(indenter.increaseLevel()).append("return ").append(type1.getBoxingText(valueName)).append(";")
                    .append(NEW_LINE);

        }
    }

    private static final String TWO_COLUMN_FACTORY_SIMPLE_NAME = "TwoColumnTupleSourceFactory";
    private static final String TWO_COLUMN_FACTORY_NAME =
            "io.deephaven.engine.table.impl.tuplesource." + TWO_COLUMN_FACTORY_SIMPLE_NAME;

    private String generateTwoColumnTupleSource(@NotNull final String className, @NotNull final ColumnSourceType type1,
            @NotNull final ColumnSourceType type2) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String sourceClass1Name = type1.getElementClassSimpleNameBoxed();
        final String sourceClass2Name = type2.getElementClassSimpleNameBoxed();
        final String tupleClassName =
                TupleCodeGenerator.getTupleClassName(type1.getInternalClassName(), type2.getInternalClassName());
        final String[] extraImports = new String[] {TupleCodeGenerator.getTupleImport(tupleClassName),
                TWO_COLUMN_FACTORY_NAME};

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), extraImports).flatMap(Arrays::stream)
                .filter(i -> !i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), extraImports).flatMap(Arrays::stream)
                .filter(i -> i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(" * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types ")
                .append(sourceClass1Name).append(" and ").append(sourceClass2Name).append('.').append(NEW_LINE);
        code.append(" * <p>Generated by ").append(TupleSourceCodeGenerator.class.getName()).append(".")
                .append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("@SuppressWarnings({\"unused\", \"WeakerAccess\"})").append(NEW_LINE);
        code.append("public class ").append(className).append(" extends AbstractTupleSource<").append(tupleClassName)
                .append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ").append(TWO_COLUMN_FACTORY_SIMPLE_NAME)
                .append("} instance to create instances of {@link ").append(className).append("}. **/")
                .append(NEW_LINE);
        code.append(indenter).append("public static final ").append(TWO_COLUMN_FACTORY_SIMPLE_NAME)
                .append('<')
                .append(tupleClassName).append(", ").append(sourceClass1Name).append(", ").append(sourceClass2Name)
                .append("> FACTORY = new Factory();").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("super(").append(CS1).append(", ").append(CS2).append(");").append(NEW_LINE);
        code.append(indenter).append("this.").append(CS1).append(" = ").append(CS1).append(';').append(NEW_LINE);
        code.append(indenter).append("this.").append(CS2).append(" = ").append(CS2).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName).append(" createTuple(final long ")
                .append(RK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementGetterText(CS1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getElementGetterText(CS2)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName).append(" createPreviousTuple(final long ")
                .append(RK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementPrevGetterText(CS1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getElementPrevGetterText(CS2)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
                .append(" createTupleFromValues(@NotNull final Object... values) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getUnboxingText("values[0]")).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getUnboxingText("values[1]")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
                .append(" createTupleFromReinterpretedValues(@NotNull final Object... values) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        addReinterpretedUnboxing(type1, indenter, code, "values[0]", true);
        addReinterpretedUnboxing(type2, indenter, code, "values[1]", false);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@SuppressWarnings(\"unchecked\")").append(NEW_LINE);
        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final <ELEMENT_TYPE> void exportElement(@NotNull final ")
                .append(tupleClassName)
                .append(" tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationRowKey, ")
                .append(type1.getExportElementText("tuple.getFirstElement()")).append(");").append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationRowKey, ")
                .append(type2.getExportElementText("tuple.getSecondElement()")).append(");").append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append(
                "throw new IndexOutOfBoundsException(\"Invalid element index \" + elementIndex + \" for export\");")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportToExternalKey(@NotNull final ").append(tupleClassName)
                .append(" tuple) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new SmartKey(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getBoxingText("tuple.getFirstElement()")).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getBoxingText("tuple.getSecondElement()")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElement(@NotNull final ").append(tupleClassName)
                .append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ").append(type1.getBoxingText("tuple.getFirstElement()"))
                .append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ").append(type2.getBoxingText("tuple.getSecondElement()"))
                .append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
                "throw new IllegalArgumentException(\"Bad elementIndex for 2 element tuple: \" + elementIndex);")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElementReinterpreted(@NotNull final ")
                .append(tupleClassName).append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        addReinterpretedExport(type1, indenter, code, "tuple.getFirstElement()");
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        addReinterpretedExport(type2, indenter, code, "tuple.getSecondElement()");
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
                "throw new IllegalArgumentException(\"Bad elementIndex for 2 element tuple: \" + elementIndex);")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(NEW_LINE);

        code.append(indenter).append(
                "protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {")
                .append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("WritableObjectChunk<").append(tupleClassName)
                .append(", ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();")
                .append(NEW_LINE);
        code.append(indenter).append(type1.getValuesChunkTypeString()).append(" chunk1 = chunks[0].as")
                .append(type1.chunkTypeName).append("Chunk();").append(NEW_LINE);
        code.append(indenter).append(type2.getValuesChunkTypeString()).append(" chunk2 = chunks[1].as")
                .append(type2.chunkTypeName).append("Chunk();").append(NEW_LINE);
        code.append(indenter).append("for (int ii = 0; ii < chunkSize; ++ii) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("destinationObjectChunk.set(ii, new ").append(tupleClassName)
                .append("(").append(type1.getFromChunkText("chunk1.get(ii)")).append(", ")
                .append(type2.getFromChunkText("chunk2.get(ii)")).append("));").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("destination.setSize(chunkSize);").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ").append(TWO_COLUMN_FACTORY_SIMPLE_NAME)
                .append("} for instances of {@link ").append(className).append("}. **/").append(NEW_LINE);
        code.append(indenter).append("private static final class Factory implements ")
                .append(TWO_COLUMN_FACTORY_SIMPLE_NAME)
                .append('<').append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
                .append(sourceClass2Name).append("> {").append(NEW_LINE);
        indenter.increaseLevel();

        code.append(NEW_LINE);

        code.append(indenter).append("private Factory() {").append(NEW_LINE);
        code.append(indenter).append("}").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public TupleSource<").append(tupleClassName).append("> create(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append(CS2).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append('}').append(NEW_LINE);

        return code.toString();
    }

    private static final String THREE_COLUMN_FACTORY_SIMPLE_NAME = "ThreeColumnTupleSourceFactory";
    private static final String THREE_COLUMN_FACTORY_NAME =
            "io.deephaven.engine.table.impl.tuplesource." + THREE_COLUMN_FACTORY_SIMPLE_NAME;

    private String generateThreeColumnTupleSource(@NotNull final String className,
            @NotNull final ColumnSourceType type1, @NotNull final ColumnSourceType type2,
            @NotNull final ColumnSourceType type3) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String sourceClass1Name = type1.getElementClassSimpleNameBoxed();
        final String sourceClass2Name = type2.getElementClassSimpleNameBoxed();
        final String sourceClass3Name = type3.getElementClassSimpleNameBoxed();
        final String tupleClassName = TupleCodeGenerator.getTupleClassName(type1.getInternalClassName(),
                type2.getInternalClassName(), type3.getInternalClassName());
        final String[] extraImports = new String[] {TupleCodeGenerator.getTupleImport(tupleClassName),
                "io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory"};

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports(), extraImports)
                .flatMap(Arrays::stream).filter(i -> !i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports(), extraImports)
                .flatMap(Arrays::stream).filter(i -> i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(" * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types ")
                .append(sourceClass1Name).append(", ").append(sourceClass2Name).append(", and ")
                .append(sourceClass3Name).append('.').append(NEW_LINE);
        code.append(" * <p>Generated by ").append(TupleSourceCodeGenerator.class.getName()).append(".")
                .append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("@SuppressWarnings({\"unused\", \"WeakerAccess\"})").append(NEW_LINE);
        code.append("public class ").append(className).append(" extends AbstractTupleSource<").append(tupleClassName)
                .append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ").append(THREE_COLUMN_FACTORY_SIMPLE_NAME)
                .append("} instance to create instances of {@link ").append(className).append("}. **/")
                .append(NEW_LINE);
        code.append(indenter).append("public static final ").append(THREE_COLUMN_FACTORY_SIMPLE_NAME)
                .append('<')
                .append(tupleClassName).append(", ").append(sourceClass1Name).append(", ").append(sourceClass2Name)
                .append(", ").append(sourceClass3Name)
                .append("> FACTORY = new Factory();").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass3Name).append("> ").append(CS3)
                .append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass3Name).append("> ").append(CS3)
                .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("super(").append(CS1).append(", ").append(CS2).append(", ").append(CS3)
                .append(");").append(NEW_LINE);
        code.append(indenter).append("this.").append(CS1).append(" = ").append(CS1).append(';').append(NEW_LINE);
        code.append(indenter).append("this.").append(CS2).append(" = ").append(CS2).append(';').append(NEW_LINE);
        code.append(indenter).append("this.").append(CS3).append(" = ").append(CS3).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName).append(" createTuple(final long ")
                .append(RK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementGetterText(CS1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getElementGetterText(CS2)).append(',').append(NEW_LINE);
        code.append(indenter).append(type3.getElementGetterText(CS3)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName).append(" createPreviousTuple(final long ")
                .append(RK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementPrevGetterText(CS1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getElementPrevGetterText(CS2)).append(',').append(NEW_LINE);
        code.append(indenter).append(type3.getElementPrevGetterText(CS3)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
                .append(" createTupleFromValues(@NotNull final Object... values) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getUnboxingText("values[0]")).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getUnboxingText("values[1]")).append(',').append(NEW_LINE);
        code.append(indenter).append(type3.getUnboxingText("values[2]")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
                .append(" createTupleFromReinterpretedValues(@NotNull final Object... values) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        addReinterpretedUnboxing(type1, indenter, code, "values[0]", true);
        addReinterpretedUnboxing(type2, indenter, code, "values[1]", true);
        addReinterpretedUnboxing(type3, indenter, code, "values[2]", false);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@SuppressWarnings(\"unchecked\")").append(NEW_LINE);
        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final <ELEMENT_TYPE> void exportElement(@NotNull final ")
                .append(tupleClassName)
                .append(" tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationRowKey, ")
                .append(type1.getExportElementText("tuple.getFirstElement()")).append(");").append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationRowKey, ")
                .append(type2.getExportElementText("tuple.getSecondElement()")).append(");").append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 2) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationRowKey, ")
                .append(type3.getExportElementText("tuple.getThirdElement()")).append(");").append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append(
                "throw new IndexOutOfBoundsException(\"Invalid element index \" + elementIndex + \" for export\");")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportToExternalKey(@NotNull final ").append(tupleClassName)
                .append(" tuple) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new SmartKey(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getBoxingText("tuple.getFirstElement()")).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getBoxingText("tuple.getSecondElement()")).append(',').append(NEW_LINE);
        code.append(indenter).append(type3.getBoxingText("tuple.getThirdElement()")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElement(@NotNull final ").append(tupleClassName)
                .append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ").append(type1.getBoxingText("tuple.getFirstElement()"))
                .append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ").append(type2.getBoxingText("tuple.getSecondElement()"))
                .append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 2) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ").append(type3.getBoxingText("tuple.getThirdElement()"))
                .append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
                "throw new IllegalArgumentException(\"Bad elementIndex for 3 element tuple: \" + elementIndex);")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElementReinterpreted(@NotNull final ")
                .append(tupleClassName).append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        addReinterpretedExport(type1, indenter, code, "tuple.getFirstElement()");
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        addReinterpretedExport(type2, indenter, code, "tuple.getSecondElement()");
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 2) {").append(NEW_LINE);
        addReinterpretedExport(type3, indenter, code, "tuple.getThirdElement()");
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
                "throw new IllegalArgumentException(\"Bad elementIndex for 3 element tuple: \" + elementIndex);")
                .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append((NEW_LINE));
        code.append(indenter).append(
                "protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {")
                .append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("WritableObjectChunk<").append(tupleClassName)
                .append(", ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();")
                .append(NEW_LINE);
        code.append(indenter).append(type1.getValuesChunkTypeString()).append(" chunk1 = chunks[0].as")
                .append(type1.chunkTypeName).append("Chunk();").append(NEW_LINE);
        code.append(indenter).append(type2.getValuesChunkTypeString()).append(" chunk2 = chunks[1].as")
                .append(type2.chunkTypeName).append("Chunk();").append(NEW_LINE);
        code.append(indenter).append(type3.getValuesChunkTypeString()).append(" chunk3 = chunks[2].as")
                .append(type3.chunkTypeName).append("Chunk();").append(NEW_LINE);
        code.append(indenter).append("for (int ii = 0; ii < chunkSize; ++ii) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("destinationObjectChunk.set(ii, new ").append(tupleClassName)
                .append("(").append(type1.getFromChunkText("chunk1.get(ii)")).append(", ")
                .append(type2.getFromChunkText("chunk2.get(ii)")).append(", ")
                .append(type3.getFromChunkText("chunk3.get(ii)")).append("));").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("destinationObjectChunk.setSize(chunkSize);").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ").append(THREE_COLUMN_FACTORY_SIMPLE_NAME)
                .append("} for instances of {@link ").append(className).append("}. **/").append(NEW_LINE);
        code.append(indenter).append("private static final class Factory implements ")
                .append(THREE_COLUMN_FACTORY_SIMPLE_NAME)
                .append('<').append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
                .append(sourceClass2Name).append(", ").append(sourceClass3Name).append("> {").append(NEW_LINE);
        indenter.increaseLevel();

        code.append(NEW_LINE);

        code.append(indenter).append("private Factory() {").append(NEW_LINE);
        code.append(indenter).append("}").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public TupleSource<").append(tupleClassName).append("> create(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name).append("> ").append(CS1)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name).append("> ").append(CS2)
                .append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass3Name).append("> ").append(CS3)
                .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append(CS2).append(',').append(NEW_LINE);
        code.append(indenter).append(CS3).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append('}').append(NEW_LINE);

        return code.toString();
    }

    private void writeClass(@NotNull final String className, @NotNull final String classBody) {
        try (final PrintStream destination =
                new PrintStream(new FileOutputStream(new File(OUTPUT_RELATIVE_PATH, className + ".java")))) {
            destination.print(classBody);
            destination.flush();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to write class " + className, e);
        }
    }

    public static void main(@NotNull final String... args) {
        final TupleSourceCodeGenerator generator = new TupleSourceCodeGenerator();
        Arrays.stream(ColumnSourceType.values()).forEach(t1 -> Arrays.stream(ColumnSourceType.values()).forEach(t2 -> {
            final String twoColumnTupleSourceName = generateSimpleClassName(t1, t2);
            final String twoColumnTupleSourceBody =
                    generator.generateTwoColumnTupleSource(twoColumnTupleSourceName, t1, t2);
            generator.writeClass(twoColumnTupleSourceName, twoColumnTupleSourceBody);
            Arrays.stream(ColumnSourceType.values()).forEach(t3 -> {
                final String threeColumnTupleSourceName = generateSimpleClassName(t1, t2, t3);
                final String threeColumnTupleSourceBody =
                        generator.generateThreeColumnTupleSource(threeColumnTupleSourceName, t1, t2, t3);
                generator.writeClass(threeColumnTupleSourceName, threeColumnTupleSourceBody);
            });
        }));
    }
}
