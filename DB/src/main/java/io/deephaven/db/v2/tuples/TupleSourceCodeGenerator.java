package io.deephaven.db.v2.tuples;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.MAIN_SRC;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.util.tuples.TupleCodeGenerator;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
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
 * Generator for a library of {@link TupleSource} classes.
 */
public class TupleSourceCodeGenerator {

    private static final String OUTPUT_PACKAGE =
        TupleSourceCodeGenerator.class.getPackage().getName() + ".generated";
    private static final File OUTPUT_RELATIVE_PATH =
        new File(new File("DB", MAIN_SRC), OUTPUT_PACKAGE.replace('.', File.separatorChar));

    private static final String CS = "$cs$";
    private static final String VAL = "$val$";

    private static final String NEW_LINE = System.getProperty("line.separator");

    private static final String[] DEFAULT_IMPORTS = Stream.of(
        ColumnSource.class,
        NotNull.class,
        SmartKey.class,
        TupleSource.class,
        WritableSource.class,
        AbstractTupleSource.class,
        Chunk.class,
        Attributes.class,
        WritableChunk.class,
        ObjectChunk.class,
        WritableObjectChunk.class).map(Class::getName).toArray(String[]::new);

    private static final String CLASS_NAME_SUFFIX = "ColumnTupleSource";
    private static final String CS1 = "columnSource1";
    private static final String CS2 = "columnSource2";
    private static final String CS3 = "columnSource3";
    private static final String IK = "indexKey";

    enum ColumnSourceType {

        // @formatter:off
              BYTE(                 "Byte",       byte.class,        null,       null, false,                                 CS + ".getByte("    + IK + ")" ,                                 CS + ".getPrevByte("    + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Byte)"      + VAL, TypeUtils.class                                     ),
             SHORT(                "Short",      short.class,        null,       null, false,                                 CS + ".getShort("   + IK + ")" ,                                 CS + ".getPrevShort("   + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Short)"     + VAL, TypeUtils.class                                     ),
               INT(              "Integer",        int.class,        null,       null, false,                                 CS + ".getInt("     + IK + ")" ,                                 CS + ".getPrevInt("     + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Integer)"   + VAL, TypeUtils.class                                     ),
              LONG(                 "Long",       long.class,        null,       null, false,                                 CS + ".getLong("    + IK + ")" ,                                 CS + ".getPrevLong("    + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Long)"      + VAL, TypeUtils.class                                     ),
             FLOAT(                "Float",      float.class,        null,       null, false,                                 CS + ".getFloat("   + IK + ")" ,                                 CS + ".getPrevFloat("   + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Float)"     + VAL, TypeUtils.class                                     ),
            DOUBLE(               "Double",     double.class,        null,       null, false,                                 CS + ".getDouble("  + IK + ")" ,                                 CS + ".getPrevDouble("  + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Double)"    + VAL, TypeUtils.class                                     ),
              CHAR(            "Character",       char.class,        null,       null, false,                                 CS + ".getChar("    + IK + ")" ,                                 CS + ".getPrevChar("    + IK + ")" , "TypeUtils.box("              + VAL + ')', "TypeUtils.unbox("               + VAL + ')', "(Character)" + VAL, TypeUtils.class                                     ),
            OBJECT(               "Object",     Object.class,        null,       null, false,                                 CS + ".get("        + IK + ")" ,                                 CS + ".getPrev("        + IK + ")" ,                                         VAL      ,                                            VAL       ,                      VAL                                                     ),
         R_BOOLEAN( "ReinterpretedBoolean",       byte.class,        null,       null,  true,                                 CS + ".getByte("    + IK + ")" ,                                 CS + ".getPrevByte("    + IK + ")" , "BooleanUtils.byteAsBoolean(" + VAL + ')', "BooleanUtils.booleanAsByte("    + VAL + ')', "(Boolean)"    + VAL, BooleanUtils.class, TypeUtils.class                 ),
           BOOLEAN(              "Boolean",    Boolean.class,                    byte.class,                  R_BOOLEAN, false, "BooleanUtils.booleanAsByte(" + CS + ".getBoolean(" + IK + "))", "BooleanUtils.booleanAsByte(" + CS + ".getPrevBoolean(" + IK + "))", "BooleanUtils.byteAsBoolean(" + VAL + ')', "BooleanUtils.booleanAsByte("    + VAL + ')', "(Boolean)"    + VAL, BooleanUtils.class                                  ),
        R_DATETIME("ReinterpretedDateTime",       long.class,        null,       null,  true,                                 CS + ".getLong("    + IK + ")" ,                                 CS + ".getPrevLong("    + IK + ")" , "DBTimeUtils.nanosToTime("    + VAL + ')', "DBTimeUtils.nanos("             + VAL + ')', "(DBDateTime)" + VAL, DBDateTime.class, DBTimeUtils.class, TypeUtils.class),
          DATETIME(             "DateTime", DBDateTime.class,                    long.class,                 R_DATETIME, false,          "DBTimeUtils.nanos(" + CS + ".get("        + IK + "))",          "DBTimeUtils.nanos(" + CS + ".getPrev("        + IK + "))", "DBTimeUtils.nanosToTime("    + VAL + ')', "DBTimeUtils.nanos("             + VAL + ')', "(DBDateTime)" + VAL,  DBDateTime.class, DBTimeUtils.class                 ),
        ;
        // @formatter:on

        private final String nameText;
        private final Class<?> elementClass;
        private final Class<?> internalClass;
        private final ColumnSourceType reinterpretAsType;
        private final boolean isReinterpreted;
        private final String elementGetter;
        private final String elementPrevGetter;
        private final String boxing;
        private final String unboxing;
        private final String cast;
        private final String[] imports;
        private final Class<?> chunkClass;
        private final ChunkType chunkType;

        ColumnSourceType(@NotNull final String nameText,
            @NotNull final Class<?> elementClass,
            @Nullable final Class<?> overrideInternalClass,
            @Nullable final ColumnSourceType reinterpretAsType,
            final boolean isReinterpreted,
            @NotNull final String elementGetter,
            @NotNull final String elementPrevGetter,
            @NotNull final String boxing,
            @NotNull final String unboxing,
            @NotNull final String cast,
            @NotNull final Class... importClasses) {
            this.nameText = nameText;
            this.elementClass = elementClass;
            this.internalClass =
                overrideInternalClass == null ? elementClass : overrideInternalClass;
            this.reinterpretAsType = reinterpretAsType;
            this.isReinterpreted = isReinterpreted;
            this.elementGetter = elementGetter;
            this.elementPrevGetter = elementPrevGetter;
            this.boxing = boxing;
            this.unboxing = unboxing;
            this.cast = cast;
            this.chunkClass = ChunkType.fromElementType(elementClass).getEmptyChunk().getClass();
            this.chunkType = ChunkType.fromElementType(elementClass);
            this.imports = Stream.concat(Stream.of(chunkClass), Arrays.stream(importClasses))
                .map(Class::getName).toArray(String[]::new);
        }

        String getNameText() {
            return nameText;
        }

        Class<?> getElementClass() {
            return elementClass;
        }

        Class<?> getInternalClass() {
            return internalClass;
        }

        String getValuesChunkTypeString() {
            if (chunkClass == ObjectChunk.class) {
                return "ObjectChunk<" + elementClass.getSimpleName() + ", Attributes.Values>";
            } else {
                return chunkClass.getSimpleName() + "<Attributes.Values>";
            }
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
            if (internalClass == elementClass) {
                return valueName;
            } else {
                return unboxing.replace(VAL, valueName);
            }
        }

        private String getUnboxingText(@NotNull final String valueName) {
            return unboxing.replace(VAL, cast.replace(VAL, valueName));
        }

        private String getExportElementText(@NotNull final String valueName) {
            return elementClass != internalClass || isReinterpreted || this == OBJECT
                ? "(ELEMENT_TYPE) " + getBoxingText(valueName)
                : valueName;
        }

        private String[] getImports() {
            return imports;
        }
    }

    private static ColumnSourceType forPrimitive(final Class<?> clazz) {
        return Arrays.stream(ColumnSourceType.values())
            .filter(cst -> cst.getElementClass() == clazz).findFirst()
            .orElseThrow(() -> new RuntimeException("Could not find type to: " + clazz));
    }

    private static String generateSimpleClassName(@NotNull final ColumnSourceType... types) {
        return Arrays.stream(types).map(ColumnSourceType::getNameText).collect(Collectors.joining())
            + CLASS_NAME_SUFFIX;
    }

    static String generateClassName(@NotNull final ColumnSourceType... types) {
        return OUTPUT_PACKAGE + '.' + generateSimpleClassName(types);
    }

    private static void addReinterpretedUnboxing(@NotNull ColumnSourceType type1, Indenter indenter,
        StringBuilder code, String valueName, boolean comma) {
        if (type1.isReinterpreted()) {
            code.append(indenter)
                .append(forPrimitive(type1.elementClass).getUnboxingText(valueName))
                .append(comma ? "," : "").append(NEW_LINE);
        } else {
            code.append(indenter).append(type1.getUnboxingText(valueName)).append(comma ? "," : "")
                .append(NEW_LINE);
        }
    }

    private static void addReinterpretedExport(@NotNull ColumnSourceType type1, Indenter indenter,
        StringBuilder code, String valueName) {
        if (type1.isReinterpreted()) {
            code.append(indenter.increaseLevel()).append("return ")
                .append(forPrimitive(type1.elementClass).getBoxingText(valueName)).append(";")
                .append(NEW_LINE);
        } else {
            code.append(indenter.increaseLevel()).append("return ")
                .append(type1.getBoxingText(valueName)).append(";").append(NEW_LINE);

        }
    }

    private String generateTwoColumnTupleSource(@NotNull final String className,
        @NotNull final ColumnSourceType type1, @NotNull final ColumnSourceType type2) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String sourceClass1Name =
            TypeUtils.getBoxedType(type1.getElementClass()).getSimpleName();
        final String sourceClass2Name =
            TypeUtils.getBoxedType(type2.getElementClass()).getSimpleName();
        final String tupleClassName = TupleCodeGenerator.getTupleClassName(type1.getInternalClass(),
            type2.getInternalClass());
        final String[] extraImports =
            new String[] {TupleCodeGenerator.getTupleImport(tupleClassName),
                    TwoColumnTupleSourceFactory.class.getName()};

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), extraImports)
            .flatMap(Arrays::stream).filter(i -> !i.startsWith("java.")).sorted().distinct()
            .forEachOrdered(
                i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), extraImports)
            .flatMap(Arrays::stream).filter(i -> i.startsWith("java.")).sorted().distinct()
            .forEachOrdered(
                i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(
            " * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types ")
            .append(sourceClass1Name).append(" and ").append(sourceClass2Name).append('.')
            .append(NEW_LINE);
        code.append(" * <p>Generated by {@link ").append(TupleSourceCodeGenerator.class.getName())
            .append("}.").append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("@SuppressWarnings({\"unused\", \"WeakerAccess\"})").append(NEW_LINE);
        code.append("public class ").append(className).append(" extends AbstractTupleSource<")
            .append(tupleClassName).append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ")
            .append(TwoColumnTupleSourceFactory.class.getSimpleName())
            .append("} instance to create instances of {@link ").append(className).append("}. **/")
            .append(NEW_LINE);
        code.append(indenter).append("public static final ")
            .append(TwoColumnTupleSourceFactory.class.getSimpleName()).append('<')
            .append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
            .append(sourceClass2Name)
            .append("> FACTORY = new Factory();").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("super(").append(CS1).append(", ").append(CS2).append(");")
            .append(NEW_LINE);
        code.append(indenter).append("this.").append(CS1).append(" = ").append(CS1).append(';')
            .append(NEW_LINE);
        code.append(indenter).append("this.").append(CS2).append(" = ").append(CS2).append(';')
            .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createTuple(final long ").append(IK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementGetterText(CS1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getElementGetterText(CS2)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createPreviousTuple(final long ").append(IK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementPrevGetterText(CS1)).append(',')
            .append(NEW_LINE);
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
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getUnboxingText("values[0]")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type2.getUnboxingText("values[1]")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createTupleFromReinterpretedValues(@NotNull final Object... values) {")
            .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
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
        code.append(indenter)
            .append("public final <ELEMENT_TYPE> void exportElement(@NotNull final ")
            .append(tupleClassName)
            .append(
                " tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {")
            .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationIndexKey, ")
            .append(type1.getExportElementText("tuple.getFirstElement()")).append(");")
            .append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationIndexKey, ")
            .append(type2.getExportElementText("tuple.getSecondElement()")).append(");")
            .append(NEW_LINE);
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
        code.append(indenter).append("public final Object exportToExternalKey(@NotNull final ")
            .append(tupleClassName).append(" tuple) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new SmartKey(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getBoxingText("tuple.getFirstElement()")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type2.getBoxingText("tuple.getSecondElement()"))
            .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElement(@NotNull final ")
            .append(tupleClassName).append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ")
            .append(type1.getBoxingText("tuple.getFirstElement()")).append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ")
            .append(type2.getBoxingText("tuple.getSecondElement()")).append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
            "throw new IllegalArgumentException(\"Bad elementIndex for 2 element tuple: \" + elementIndex);")
            .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter)
            .append("public final Object exportElementReinterpreted(@NotNull final ")
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
            "protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {")
            .append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("WritableObjectChunk<").append(tupleClassName)
            .append(
                ", ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();")
            .append(NEW_LINE);
        code.append(indenter).append(type1.getValuesChunkTypeString())
            .append(" chunk1 = chunks[0].as").append(type1.chunkType).append("Chunk();")
            .append(NEW_LINE);
        code.append(indenter).append(type2.getValuesChunkTypeString())
            .append(" chunk2 = chunks[1].as").append(type2.chunkType).append("Chunk();")
            .append(NEW_LINE);
        code.append(indenter).append("for (int ii = 0; ii < chunkSize; ++ii) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("destinationObjectChunk.set(ii, new ")
            .append(tupleClassName).append("(").append(type1.getFromChunkText("chunk1.get(ii)"))
            .append(", ").append(type2.getFromChunkText("chunk2.get(ii)")).append("));")
            .append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("destination.setSize(chunkSize);").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ")
            .append(TwoColumnTupleSourceFactory.class.getSimpleName())
            .append("} for instances of {@link ").append(className).append("}. **/")
            .append(NEW_LINE);
        code.append(indenter).append("private static final class Factory implements ")
            .append(TwoColumnTupleSourceFactory.class.getSimpleName())
            .append('<').append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
            .append(sourceClass2Name).append("> {").append(NEW_LINE);
        indenter.increaseLevel();

        code.append(NEW_LINE);

        code.append(indenter).append("private Factory() {").append(NEW_LINE);
        code.append(indenter).append("}").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public TupleSource<").append(tupleClassName)
            .append("> create(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(NEW_LINE);
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

    private String generateThreeColumnTupleSource(@NotNull final String className,
        @NotNull final ColumnSourceType type1, @NotNull final ColumnSourceType type2,
        @NotNull final ColumnSourceType type3) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String sourceClass1Name =
            TypeUtils.getBoxedType(type1.getElementClass()).getSimpleName();
        final String sourceClass2Name =
            TypeUtils.getBoxedType(type2.getElementClass()).getSimpleName();
        final String sourceClass3Name =
            TypeUtils.getBoxedType(type3.getElementClass()).getSimpleName();
        final String tupleClassName = TupleCodeGenerator.getTupleClassName(type1.getInternalClass(),
            type2.getInternalClass(), type3.getInternalClass());
        final String[] extraImports =
            new String[] {TupleCodeGenerator.getTupleImport(tupleClassName),
                    ThreeColumnTupleSourceFactory.class.getName()};

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream
            .of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports(),
                extraImports)
            .flatMap(Arrays::stream).filter(i -> !i.startsWith("java.")).sorted().distinct()
            .forEachOrdered(
                i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream
            .of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports(),
                extraImports)
            .flatMap(Arrays::stream).filter(i -> i.startsWith("java.")).sorted().distinct()
            .forEachOrdered(
                i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(
            " * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types ")
            .append(sourceClass1Name).append(", ").append(sourceClass2Name).append(", and ")
            .append(sourceClass3Name).append('.').append(NEW_LINE);
        code.append(" * <p>Generated by {@link ").append(TupleSourceCodeGenerator.class.getName())
            .append("}.").append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("@SuppressWarnings({\"unused\", \"WeakerAccess\"})").append(NEW_LINE);
        code.append("public class ").append(className).append(" extends AbstractTupleSource<")
            .append(tupleClassName).append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ")
            .append(ThreeColumnTupleSourceFactory.class.getSimpleName())
            .append("} instance to create instances of {@link ").append(className).append("}. **/")
            .append(NEW_LINE);
        code.append(indenter).append("public static final ")
            .append(ThreeColumnTupleSourceFactory.class.getSimpleName()).append('<')
            .append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
            .append(sourceClass2Name).append(", ").append(sourceClass3Name)
            .append("> FACTORY = new Factory();").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(';').append(NEW_LINE);
        code.append(indenter).append("private final ColumnSource<").append(sourceClass3Name)
            .append("> ").append(CS3).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass3Name)
            .append("> ").append(CS3).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("super(").append(CS1).append(", ").append(CS2).append(", ")
            .append(CS3).append(");").append(NEW_LINE);
        code.append(indenter).append("this.").append(CS1).append(" = ").append(CS1).append(';')
            .append(NEW_LINE);
        code.append(indenter).append("this.").append(CS2).append(" = ").append(CS2).append(';')
            .append(NEW_LINE);
        code.append(indenter).append("this.").append(CS3).append(" = ").append(CS3).append(';')
            .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createTuple(final long ").append(IK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
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
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createPreviousTuple(final long ").append(IK).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getElementPrevGetterText(CS1)).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type2.getElementPrevGetterText(CS2)).append(',')
            .append(NEW_LINE);
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
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getUnboxingText("values[0]")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type2.getUnboxingText("values[1]")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type3.getUnboxingText("values[2]")).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final ").append(tupleClassName)
            .append(" createTupleFromReinterpretedValues(@NotNull final Object... values) {")
            .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new ").append(tupleClassName).append('(')
            .append(NEW_LINE);
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
        code.append(indenter)
            .append("public final <ELEMENT_TYPE> void exportElement(@NotNull final ")
            .append(tupleClassName)
            .append(
                " tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {")
            .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationIndexKey, ")
            .append(type1.getExportElementText("tuple.getFirstElement()")).append(");")
            .append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationIndexKey, ")
            .append(type2.getExportElementText("tuple.getSecondElement()")).append(");")
            .append(NEW_LINE);
        code.append(indenter).append("return;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 2) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("writableSource.set(destinationIndexKey, ")
            .append(type3.getExportElementText("tuple.getThirdElement()")).append(");")
            .append(NEW_LINE);
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
        code.append(indenter).append("public final Object exportToExternalKey(@NotNull final ")
            .append(tupleClassName).append(" tuple) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return new SmartKey(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getBoxingText("tuple.getFirstElement()")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type2.getBoxingText("tuple.getSecondElement()")).append(',')
            .append(NEW_LINE);
        code.append(indenter).append(type3.getBoxingText("tuple.getThirdElement()"))
            .append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final Object exportElement(@NotNull final ")
            .append(tupleClassName).append(" tuple, int elementIndex) {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (elementIndex == 0) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ")
            .append(type1.getBoxingText("tuple.getFirstElement()")).append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 1) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ")
            .append(type2.getBoxingText("tuple.getSecondElement()")).append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("if (elementIndex == 2) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("return ")
            .append(type3.getBoxingText("tuple.getThirdElement()")).append(";").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append(
            "throw new IllegalArgumentException(\"Bad elementIndex for 3 element tuple: \" + elementIndex);")
            .append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter)
            .append("public final Object exportElementReinterpreted(@NotNull final ")
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
            "protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {")
            .append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("WritableObjectChunk<").append(tupleClassName)
            .append(
                ", ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();")
            .append(NEW_LINE);
        code.append(indenter).append(type1.getValuesChunkTypeString())
            .append(" chunk1 = chunks[0].as").append(type1.chunkType).append("Chunk();")
            .append(NEW_LINE);
        code.append(indenter).append(type2.getValuesChunkTypeString())
            .append(" chunk2 = chunks[1].as").append(type2.chunkType).append("Chunk();")
            .append(NEW_LINE);
        code.append(indenter).append(type3.getValuesChunkTypeString())
            .append(" chunk3 = chunks[2].as").append(type3.chunkType).append("Chunk();")
            .append(NEW_LINE);
        code.append(indenter).append("for (int ii = 0; ii < chunkSize; ++ii) {").append(NEW_LINE);
        code.append(indenter.increaseLevel()).append("destinationObjectChunk.set(ii, new ")
            .append(tupleClassName).append("(").append(type1.getFromChunkText("chunk1.get(ii)"))
            .append(", ").append(type2.getFromChunkText("chunk2.get(ii)")).append(", ")
            .append(type3.getFromChunkText("chunk3.get(ii)")).append("));").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(indenter).append("destinationObjectChunk.setSize(chunkSize);").append(NEW_LINE);
        code.append(indenter.decreaseLevel()).append("}").append(NEW_LINE);
        code.append(NEW_LINE);

        code.append(indenter).append("/** {@link ")
            .append(ThreeColumnTupleSourceFactory.class.getSimpleName())
            .append("} for instances of {@link ").append(className).append("}. **/")
            .append(NEW_LINE);
        code.append(indenter).append("private static final class Factory implements ")
            .append(ThreeColumnTupleSourceFactory.class.getSimpleName())
            .append('<').append(tupleClassName).append(", ").append(sourceClass1Name).append(", ")
            .append(sourceClass2Name).append(", ").append(sourceClass3Name).append("> {")
            .append(NEW_LINE);
        indenter.increaseLevel();

        code.append(NEW_LINE);

        code.append(indenter).append("private Factory() {").append(NEW_LINE);
        code.append(indenter).append("}").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public TupleSource<").append(tupleClassName)
            .append("> create(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass1Name)
            .append("> ").append(CS1).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass2Name)
            .append("> ").append(CS2).append(',').append(NEW_LINE);
        code.append(indenter).append("@NotNull final ColumnSource<").append(sourceClass3Name)
            .append("> ").append(CS3).append(NEW_LINE);
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
        try (final PrintStream destination = new PrintStream(
            new FileOutputStream(new File(OUTPUT_RELATIVE_PATH, className + ".java")))) {
            destination.print(classBody);
            destination.flush();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to write class " + className, e);
        }
    }

    public static void main(@NotNull final String... args) {
        final TupleSourceCodeGenerator generator = new TupleSourceCodeGenerator();
        Arrays.stream(ColumnSourceType.values())
            .forEach(t1 -> Arrays.stream(ColumnSourceType.values()).forEach(t2 -> {
                final String twoColumnTupleSourceName = generateSimpleClassName(t1, t2);
                final String twoColumnTupleSourceBody =
                    generator.generateTwoColumnTupleSource(twoColumnTupleSourceName, t1, t2);
                generator.writeClass(twoColumnTupleSourceName, twoColumnTupleSourceBody);
                Arrays.stream(ColumnSourceType.values()).forEach(t3 -> {
                    final String threeColumnTupleSourceName = generateSimpleClassName(t1, t2, t3);
                    final String threeColumnTupleSourceBody = generator
                        .generateThreeColumnTupleSource(threeColumnTupleSourceName, t1, t2, t3);
                    generator.writeClass(threeColumnTupleSourceName, threeColumnTupleSourceBody);
                });
            }));
    }
}
