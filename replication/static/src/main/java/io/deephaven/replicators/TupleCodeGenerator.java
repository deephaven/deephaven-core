package io.deephaven.replicators;

import io.deephaven.util.text.Indenter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generator for a library of semi-opaque classes for tuple (ordered, multi-element) keys.
 */
public class TupleCodeGenerator {

    /*
     * TODO: Support getters for tuple elements? No use case currently, and might encourage mutations. TODO: Generify
     * Object elements? No use case without the above, I don't think. TODO: Refactor to make it possible to generate
     * arbitrary n-tuples, and eliminate duplication between generateDouble and generateTriple.
     */

    private static final String OUTPUT_PACKAGE = "io.deephaven.tuple.generated";
    private static final File OUTPUT_RELATIVE_PATH =
            new File("engine/tuple/src/main/java/io/deephaven/tuple/generated");

    private static final String LHS = "$lhs$";
    private static final String RHS = "$rhs$";
    private static final String VAL = "$val$";
    private static final String OUT = "$out$";
    private static final String IN = "$in$";

    private static final String NEW_LINE = System.getProperty("line.separator");

    private static final String[] DEFAULT_IMPORTS = new String[] {
            "io.deephaven.tuple.CanonicalizableTuple",
            "java.io.Externalizable",
            "java.io.IOException",
            "org.jetbrains.annotations.NotNull",
            "java.io.ObjectInput",
            "java.io.ObjectOutput",
            "io.deephaven.tuple.serialization.SerializationUtils",
            "io.deephaven.tuple.serialization.StreamingExternalizable",
            "gnu.trove.map.TIntObjectMap",
            "java.util.function.UnaryOperator"
    };

    private static final String CLASS_NAME_SUFFIX = "Tuple";
    private static final String ELEMENT1 = "element1";
    private static final String ELEMENT2 = "element2";
    private static final String ELEMENT3 = "element3";
    private static final String CANONICALIZED_ELEMENT1 = "canonicalizedElement1";
    private static final String CANONICALIZED_ELEMENT2 = "canonicalizedElement2";
    private static final String CANONICALIZED_ELEMENT3 = "canonicalizedElement3";
    private static final String CACHED_HASH_CODE = "cachedHashCode";
    private static final String OTHER = "other";
    private static final String TYPED_OTHER = "typedOther";

    private enum ElementType {

        // @formatter:off
           BYTE(   "Byte",    byte.class, LHS + " == " + RHS                              ,      "Byte.hashCode(" + VAL + ")", "ByteComparisons.compare("   + LHS + ", " + RHS + ")", OUT + ".writeByte("   + VAL + ')', IN + ".readByte()"  , "io.deephaven.util.compare.ByteComparisons"                       ),
          SHORT(  "Short",   short.class, LHS + " == " + RHS                              ,     "Short.hashCode(" + VAL + ")", "ShortComparisons.compare("  + LHS + ", " + RHS + ")", OUT + ".writeShort("  + VAL + ')', IN + ".readShort()" , "io.deephaven.util.compare.ShortComparisons"                      ),
            INT(    "Int",     int.class, LHS + " == " + RHS                              ,   "Integer.hashCode(" + VAL + ")", "IntComparisons.compare("    + LHS + ", " + RHS + ")", OUT + ".writeInt("    + VAL + ')', IN + ".readInt()"   , "io.deephaven.util.compare.IntComparisons"                        ),
           LONG(   "Long",    long.class, LHS + " == " + RHS                              ,      "Long.hashCode(" + VAL + ")", "LongComparisons.compare("   + LHS + ", " + RHS + ")", OUT + ".writeLong("   + VAL + ')', IN + ".readLong()"  , "io.deephaven.util.compare.LongComparisons"                       ),
          FLOAT(  "Float",    float.class, LHS + " == " + RHS                              ,     "Float.hashCode(" + VAL + ")", "FloatComparisons.compare("  + LHS + ", " + RHS + ")", OUT + ".writeFloat("  + VAL + ')', IN + ".readFloat()" , "io.deephaven.util.compare.FloatComparisons"                      ),
         DOUBLE( "Double",  double.class, LHS + " == " + RHS                              ,    "Double.hashCode(" + VAL + ")", "DoubleComparisons.compare(" + LHS + ", " + RHS + ")", OUT + ".writeDouble(" + VAL + ')', IN + ".readDouble()", "io.deephaven.util.compare.DoubleComparisons"                     ),
           CHAR(   "Char",    char.class, LHS + " == " + RHS                              , "Character.hashCode(" + VAL + ")", "CharComparisons.compare("   + LHS + ", " + RHS + ")", OUT + ".writeChar("   + VAL + ')', IN + ".readChar()"  , "io.deephaven.util.compare.CharComparisons"                       ),
         OBJECT( "Object",  Object.class, "ObjectComparisons.eq(" + LHS + ", " + RHS + ")",   "Objects.hashCode(" + VAL + ")", "ObjectComparisons.compare(" + LHS + ", " + RHS + ")", OUT + ".writeObject(" + VAL + ')', IN + ".readObject()", "io.deephaven.util.compare.ObjectComparisons", "java.util.Objects"),
        ;
        // @formatter:on

        private final String nameText;
        private final Class<?> implementation;
        private final String equalsText;
        private final String hashCodeText;
        private final String compareToText;
        private final String writeExternalText;
        private final String readExternalText;
        private final String[] imports;

        ElementType(@NotNull final String nameText,
                @NotNull final Class<?> implementation,
                @NotNull final String equalsText,
                @NotNull final String hashCodeText,
                @NotNull final String compareToText,
                @NotNull final String writeExternalText,
                @NotNull final String readExternalText,
                @NotNull final String... imports) {
            this.nameText = nameText;
            this.implementation = implementation;
            this.equalsText = equalsText;
            this.hashCodeText = hashCodeText;
            this.compareToText = compareToText;
            this.writeExternalText = writeExternalText;
            this.readExternalText = readExternalText;
            this.imports = imports;
        }

        private String getNameText() {
            return nameText;
        }

        private Class<?> getImplementation() {
            return implementation;
        }

        private String getImplementationName() {
            return implementation.getSimpleName();
        }

        private String getEqualsText(@NotNull final String lhsName, @NotNull final String rhsName) {
            return equalsText.replace(LHS, lhsName).replace(RHS, rhsName);
        }

        private String getHashCodeText(@NotNull final String elementName) {
            return hashCodeText.replace(VAL, elementName);
        }

        private String getCompareToText(@NotNull final String lhsName, @NotNull final String rhsName) {
            return compareToText.replace(LHS, lhsName).replace(RHS, rhsName);
        }

        private String getWriteExternalText(@SuppressWarnings("SameParameterValue") @NotNull final String outName,
                @NotNull final String elementName) {
            return writeExternalText.replace(OUT, outName).replace(VAL, elementName);
        }

        private String getReadExternalText(@SuppressWarnings("SameParameterValue") @NotNull final String inName,
                @NotNull final String elementName) {
            return readExternalText.replace(IN, inName).replace(VAL, elementName);
        }

        private String[] getImports() {
            return imports;
        }
    }

    private static final Map<String, ElementType> PRIMITIVE_CLASS_NAME_TO_ELEMENT_TYPE =
            Collections.unmodifiableMap(Arrays.stream(ElementType.values())
                    .filter(et -> et != ElementType.OBJECT)
                    .collect(Collectors.toMap(et -> et.getImplementation().getName(), Function.identity())));

    /**
     * Get the tuple class name for the supplied array of element class names.
     *
     * @param classNames The element class names
     * @return The tuple class name
     */
    public static String getTupleClassName(@NotNull final String... classNames) {
        if (classNames.length < 2) {
            throw new IllegalArgumentException(
                    "There are no tuple class names available for " + Arrays.toString(classNames));
        }
        if (classNames.length < 4) {
            return Arrays.stream(classNames)
                    .map(c -> PRIMITIVE_CLASS_NAME_TO_ELEMENT_TYPE.getOrDefault(c, ElementType.OBJECT).getNameText())
                    .collect(Collectors.joining()) + CLASS_NAME_SUFFIX;
        }
        return "io.deephaven.tuple.ArrayTuple";
    }

    /**
     * Prepend the output package name to the supplied tuple class name.
     *
     * @param className The tuple class name
     * @return The full tuple class name for an import statement
     */
    public static String getTupleImport(@NotNull final String className) {
        return OUTPUT_PACKAGE + '.' + className;
    }

    private String generateClassName(@NotNull final ElementType... types) {
        return Arrays.stream(types).map(ElementType::getNameText).collect(Collectors.joining()) + CLASS_NAME_SUFFIX;
    }

    private String generateDouble(@NotNull final String className, @NotNull final ElementType type1,
            @NotNull final ElementType type2) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String class1Name = type1.getImplementationName();
        final String class2Name = type2.getImplementationName();
        final boolean firstIsObject = type1 == ElementType.OBJECT;
        final boolean secondIsObject = type2 == ElementType.OBJECT;

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports()).flatMap(Arrays::stream)
                .filter(i -> !i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports()).flatMap(Arrays::stream)
                .filter(i -> i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(" * <p>2-Tuple (double) key class composed of ").append(class1Name).append(" and ")
                .append(class2Name).append(" elements.").append(NEW_LINE);
        code.append(" * <p>Generated by ").append(TupleCodeGenerator.class.getName()).append(".")
                .append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("public class ").append(className).append(" implements Comparable<").append(className)
                .append(">, Externalizable, StreamingExternalizable, CanonicalizableTuple<").append(className)
                .append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private static final long serialVersionUID = 1L;").append(NEW_LINE);
        code.append(NEW_LINE);
        code.append(indenter).append("private ").append(class1Name).append(' ').append(ELEMENT1).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("private ").append(class2Name).append(' ').append(ELEMENT2).append(';')
                .append(NEW_LINE);
        code.append(NEW_LINE);
        code.append(indenter).append("private transient int ").append(CACHED_HASH_CODE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("final ").append(class1Name).append(' ').append(ELEMENT1).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class2Name).append(' ').append(ELEMENT2).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(ELEMENT1).append(',').append(NEW_LINE);
        code.append(indenter).append(ELEMENT2).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append(
                "/** Public no-arg constructor for {@link Externalizable} support only. <em>Application code should not use this!</em> **/")
                .append(NEW_LINE);
        code.append(indenter).append("public ").append(className).append("() {").append(NEW_LINE);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private void initialize(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("final ").append(class1Name).append(' ').append(ELEMENT1).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class2Name).append(' ').append(ELEMENT2).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("this.").append(ELEMENT1).append(" = ").append(ELEMENT1).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("this.").append(ELEMENT2).append(" = ").append(ELEMENT2).append(';')
                .append(NEW_LINE);
        code.append(indenter).append(CACHED_HASH_CODE).append(" = (31 +").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getHashCodeText(ELEMENT1)).append(") * 31 +").append(NEW_LINE);
        code.append(indenter).append(type2.getHashCodeText(ELEMENT2)).append(';').append(NEW_LINE);
        indenter.decreaseLevel(3);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public final ").append(class1Name).append(" getFirstElement() {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(ELEMENT1).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public final ").append(class2Name).append(" getSecondElement() {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(ELEMENT2).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final int hashCode() {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(CACHED_HASH_CODE).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final boolean equals(final Object ").append(OTHER).append(") {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (this == ").append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return true;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (").append(OTHER).append(" == null || getClass() != other.getClass()) {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return false;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("final ").append(className).append(' ').append(TYPED_OTHER).append(" = (")
                .append(className).append(") ").append(OTHER).append(';').append(NEW_LINE);
        code.append(indenter).append("// @formatter:off").append(NEW_LINE);
        code.append(indenter).append("return ").append(type1.getEqualsText(ELEMENT1, TYPED_OTHER + '.' + ELEMENT1))
                .append(" &&").append(NEW_LINE);
        code.append(indenter).append("       ").append(type2.getEqualsText(ELEMENT2, TYPED_OTHER + '.' + ELEMENT2))
                .append(';').append(NEW_LINE);
        code.append(indenter).append("// @formatter:on").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final int compareTo(@NotNull final ").append(className).append(' ')
                .append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (this == ").append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return 0;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("int comparison;").append(NEW_LINE);
        code.append(indenter).append("// @formatter:off").append(NEW_LINE);
        code.append(indenter).append("return 0 != (comparison = ")
                .append(type1.getCompareToText(ELEMENT1, OTHER + '.' + ELEMENT1)).append(") ? comparison :")
                .append(NEW_LINE);
        code.append(indenter).append("       ").append(type2.getCompareToText(ELEMENT2, OTHER + '.' + ELEMENT2))
                .append(";").append(NEW_LINE);
        code.append(indenter).append("// @formatter:on").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public void writeExternal(@NotNull final ObjectOutput out) throws IOException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append(type1.getWriteExternalText("out", ELEMENT1)).append(';').append(NEW_LINE);
        code.append(indenter).append(type2.getWriteExternalText("out", ELEMENT2)).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        code.append(indenter.increaseLevel(2)).append(type1.getReadExternalText("in", ELEMENT1)).append(',')
                .append(NEW_LINE);
        code.append(indenter).append(type2.getReadExternalText("in", ELEMENT2)).append(NEW_LINE);
        code.append(indenter.decreaseLevel(2)).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void writeExternalStreaming(@NotNull final ObjectOutput out, @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        if (type1 != ElementType.OBJECT) {
            code.append(indenter).append(type1.getWriteExternalText("out", ELEMENT1)).append(';').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.writeObjectElement(out, cachedWriters, 0, ")
                    .append(ELEMENT1).append(");").append(NEW_LINE);
        }
        if (type2 != ElementType.OBJECT) {
            code.append(indenter).append(type2.getWriteExternalText("out", ELEMENT2)).append(';').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.writeObjectElement(out, cachedWriters, 1, ")
                    .append(ELEMENT2).append(");").append(NEW_LINE);
        }
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void readExternalStreaming(@NotNull final ObjectInput in, @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        if (type1 != ElementType.OBJECT) {
            code.append(indenter.increaseLevel(2)).append(type1.getReadExternalText("in", ELEMENT1)).append(',')
                    .append(NEW_LINE);
        } else {
            code.append(indenter.increaseLevel(2))
                    .append("StreamingExternalizable.readObjectElement(in, cachedReaders, 0)").append(',')
                    .append(NEW_LINE);
        }
        if (type2 != ElementType.OBJECT) {
            code.append(indenter).append(type2.getReadExternalText("in", ELEMENT2)).append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.readObjectElement(in, cachedReaders, 1)")
                    .append(NEW_LINE);
        }
        code.append(indenter.decreaseLevel(2)).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public String toString() {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return \"").append(className).append("{\" +").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(ELEMENT1).append(" + \", \" +").append(NEW_LINE);
        code.append(indenter).append(ELEMENT2).append(" + '}';").append(NEW_LINE);
        indenter.decreaseLevel(3);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public ").append(className)
                .append(" canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {").append(NEW_LINE);
        indenter.increaseLevel();
        if (firstIsObject) {
            code.append(indenter).append("final ").append(class1Name).append(' ').append(CANONICALIZED_ELEMENT1)
                    .append(" = canonicalizer.apply(").append(ELEMENT1).append(");").append(NEW_LINE);
        }
        if (secondIsObject) {
            code.append(indenter).append("final ").append(class2Name).append(' ').append(CANONICALIZED_ELEMENT2)
                    .append(" = canonicalizer.apply(").append(ELEMENT2).append(");").append(NEW_LINE);
        }
        if (firstIsObject && secondIsObject) {
            code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                    .append(" && ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2).append(NEW_LINE);
            indenter.increaseLevel(2);
            code.append(indenter).append("? this : new ").append(className).append('(').append(CANONICALIZED_ELEMENT1)
                    .append(", ").append(CANONICALIZED_ELEMENT2).append(");").append(NEW_LINE);
            indenter.decreaseLevel(2);
        } else if (firstIsObject) {
            code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                    .append(NEW_LINE);
            indenter.increaseLevel(2);
            code.append(indenter).append("? this : new ").append(className).append('(').append(CANONICALIZED_ELEMENT1)
                    .append(", ").append(ELEMENT2).append(");").append(NEW_LINE);
            indenter.decreaseLevel(2);
        } else if (secondIsObject) {
            code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2)
                    .append(NEW_LINE);
            indenter.increaseLevel(2);
            code.append(indenter).append("? this : new ").append(className).append('(').append(ELEMENT1).append(", ")
                    .append(CANONICALIZED_ELEMENT2).append(");").append(NEW_LINE);
            indenter.decreaseLevel(2);
        } else {
            code.append(indenter).append("return this;").append(NEW_LINE);
        }
        indenter.decreaseLevel(1);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append('}').append(NEW_LINE);

        return code.toString();
    }

    private String generateTriple(@NotNull final String className, @NotNull final ElementType type1,
            @NotNull final ElementType type2, @NotNull final ElementType type3) {
        final Indenter indenter = new Indenter();
        final StringBuilder code = new StringBuilder(1024);
        final String class1Name = type1.getImplementationName();
        final String class2Name = type2.getImplementationName();
        final String class3Name = type3.getImplementationName();
        final boolean firstIsObject = type1 == ElementType.OBJECT;
        final boolean secondIsObject = type2 == ElementType.OBJECT;
        final boolean thirdIsObject = type3 == ElementType.OBJECT;

        code.append("package ").append(OUTPUT_PACKAGE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports()).flatMap(Arrays::stream)
                .filter(i -> !i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        Stream.of(DEFAULT_IMPORTS, type1.getImports(), type2.getImports(), type3.getImports()).flatMap(Arrays::stream)
                .filter(i -> i.startsWith("java.")).sorted().distinct().forEachOrdered(
                        i -> code.append("import ").append(i).append(';').append(NEW_LINE));

        code.append(NEW_LINE);

        code.append("/**").append(NEW_LINE);
        code.append(" * <p>3-Tuple (triple) key class composed of ").append(class1Name).append(", ").append(class2Name)
                .append(", and ").append(class3Name).append(" elements.").append(NEW_LINE);
        code.append(" * <p>Generated by ").append(TupleCodeGenerator.class.getName()).append(".")
                .append(NEW_LINE);
        code.append(" */").append(NEW_LINE);
        code.append("public class ").append(className).append(" implements Comparable<").append(className)
                .append(">, Externalizable, StreamingExternalizable, CanonicalizableTuple<").append(className)
                .append("> {").append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private static final long serialVersionUID = 1L;").append(NEW_LINE);
        code.append(NEW_LINE);
        code.append(indenter).append("private ").append(class1Name).append(' ').append(ELEMENT1).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("private ").append(class2Name).append(' ').append(ELEMENT2).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("private ").append(class3Name).append(' ').append(ELEMENT3).append(';')
                .append(NEW_LINE);
        code.append(NEW_LINE);
        code.append(indenter).append("private transient int ").append(CACHED_HASH_CODE).append(';').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public ").append(className).append('(').append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("final ").append(class1Name).append(' ').append(ELEMENT1).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class2Name).append(' ').append(ELEMENT2).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class3Name).append(' ').append(ELEMENT3).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(ELEMENT1).append(',').append(NEW_LINE);
        code.append(indenter).append(ELEMENT2).append(',').append(NEW_LINE);
        code.append(indenter).append(ELEMENT3).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append(
                "/** Public no-arg constructor for {@link Externalizable} support only. <em>Application code should not use this!</em> **/")
                .append(NEW_LINE);
        code.append(indenter).append("public ").append(className).append("() {").append(NEW_LINE);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("private void initialize(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append("final ").append(class1Name).append(' ').append(ELEMENT1).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class2Name).append(' ').append(ELEMENT2).append(',')
                .append(NEW_LINE);
        code.append(indenter).append("final ").append(class3Name).append(' ').append(ELEMENT3).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("this.").append(ELEMENT1).append(" = ").append(ELEMENT1).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("this.").append(ELEMENT2).append(" = ").append(ELEMENT2).append(';')
                .append(NEW_LINE);
        code.append(indenter).append("this.").append(ELEMENT3).append(" = ").append(ELEMENT3).append(';')
                .append(NEW_LINE);
        code.append(indenter).append(CACHED_HASH_CODE).append(" = ((31 +").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getHashCodeText(ELEMENT1)).append(") * 31 +").append(NEW_LINE);
        code.append(indenter).append(type2.getHashCodeText(ELEMENT2)).append(") * 31 +").append(NEW_LINE);
        code.append(indenter).append(type3.getHashCodeText(ELEMENT3)).append(';').append(NEW_LINE);
        indenter.decreaseLevel(3);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public final ").append(class1Name).append(" getFirstElement() {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(ELEMENT1).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public final ").append(class2Name).append(" getSecondElement() {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(ELEMENT2).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("public final ").append(class3Name).append(" getThirdElement() {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(ELEMENT3).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final int hashCode() {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return ").append(CACHED_HASH_CODE).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final boolean equals(final Object ").append(OTHER).append(") {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (this == ").append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return true;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("if (").append(OTHER).append(" == null || getClass() != other.getClass()) {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return false;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("final ").append(className).append(' ').append(TYPED_OTHER).append(" = (")
                .append(className).append(") ").append(OTHER).append(';').append(NEW_LINE);
        code.append(indenter).append("// @formatter:off").append(NEW_LINE);
        code.append(indenter).append("return ").append(type1.getEqualsText(ELEMENT1, TYPED_OTHER + '.' + ELEMENT1))
                .append(" &&").append(NEW_LINE);
        code.append(indenter).append("       ").append(type2.getEqualsText(ELEMENT2, TYPED_OTHER + '.' + ELEMENT2))
                .append(" &&").append(NEW_LINE);
        code.append(indenter).append("       ").append(type3.getEqualsText(ELEMENT3, TYPED_OTHER + '.' + ELEMENT3))
                .append(';').append(NEW_LINE);
        code.append(indenter).append("// @formatter:on").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public final int compareTo(@NotNull final ").append(className).append(' ')
                .append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("if (this == ").append(OTHER).append(") {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return 0;").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);
        code.append(indenter).append("int comparison;").append(NEW_LINE);
        code.append(indenter).append("// @formatter:off").append(NEW_LINE);
        code.append(indenter).append("return 0 != (comparison = ")
                .append(type1.getCompareToText(ELEMENT1, OTHER + '.' + ELEMENT1)).append(") ? comparison :")
                .append(NEW_LINE);
        code.append(indenter).append("       0 != (comparison = ")
                .append(type2.getCompareToText(ELEMENT2, OTHER + '.' + ELEMENT2)).append(") ? comparison :")
                .append(NEW_LINE);
        code.append(indenter).append("       ").append(type3.getCompareToText(ELEMENT3, OTHER + '.' + ELEMENT3))
                .append(";").append(NEW_LINE);
        code.append(indenter).append("// @formatter:on").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public void writeExternal(@NotNull final ObjectOutput out) throws IOException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append(type1.getWriteExternalText("out", ELEMENT1)).append(';').append(NEW_LINE);
        code.append(indenter).append(type2.getWriteExternalText("out", ELEMENT2)).append(';').append(NEW_LINE);
        code.append(indenter).append(type3.getWriteExternalText("out", ELEMENT3)).append(';').append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(type1.getReadExternalText("in", ELEMENT1)).append(',').append(NEW_LINE);
        code.append(indenter).append(type2.getReadExternalText("in", ELEMENT2)).append(',').append(NEW_LINE);
        code.append(indenter).append(type3.getReadExternalText("in", ELEMENT3)).append(NEW_LINE);
        indenter.decreaseLevel(2);
        code.append(indenter).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void writeExternalStreaming(@NotNull final ObjectOutput out, @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        if (type1 != ElementType.OBJECT) {
            code.append(indenter).append(type1.getWriteExternalText("out", ELEMENT1)).append(';').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.writeObjectElement(out, cachedWriters, 0, ")
                    .append(ELEMENT1).append(");").append(NEW_LINE);
        }
        if (type2 != ElementType.OBJECT) {
            code.append(indenter).append(type2.getWriteExternalText("out", ELEMENT2)).append(';').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.writeObjectElement(out, cachedWriters, 1, ")
                    .append(ELEMENT2).append(");").append(NEW_LINE);
        }
        if (type3 != ElementType.OBJECT) {
            code.append(indenter).append(type3.getWriteExternalText("out", ELEMENT3)).append(';').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.writeObjectElement(out, cachedWriters, 2, ")
                    .append(ELEMENT3).append(");").append(NEW_LINE);
        }
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append(
                "public void readExternalStreaming(@NotNull final ObjectInput in, @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {")
                .append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("initialize(").append(NEW_LINE);
        if (type1 != ElementType.OBJECT) {
            code.append(indenter.increaseLevel(2)).append(type1.getReadExternalText("in", ELEMENT1)).append(',')
                    .append(NEW_LINE);
        } else {
            code.append(indenter.increaseLevel(2))
                    .append("StreamingExternalizable.readObjectElement(in, cachedReaders, 0)").append(',')
                    .append(NEW_LINE);
        }
        if (type2 != ElementType.OBJECT) {
            code.append(indenter).append(type2.getReadExternalText("in", ELEMENT2)).append(',').append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.readObjectElement(in, cachedReaders, 1)").append(',')
                    .append(NEW_LINE);
        }
        if (type3 != ElementType.OBJECT) {
            code.append(indenter).append(type3.getReadExternalText("in", ELEMENT3)).append(NEW_LINE);
        } else {
            code.append(indenter).append("StreamingExternalizable.readObjectElement(in, cachedReaders, 2)")
                    .append(NEW_LINE);
        }
        code.append(indenter.decreaseLevel(2)).append(");").append(NEW_LINE);
        indenter.decreaseLevel();
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public String toString() {").append(NEW_LINE);
        indenter.increaseLevel();
        code.append(indenter).append("return \"").append(className).append("{\" +").append(NEW_LINE);
        indenter.increaseLevel(2);
        code.append(indenter).append(ELEMENT1).append(" + \", \" +").append(NEW_LINE);
        code.append(indenter).append(ELEMENT2).append(" + \", \" +").append(NEW_LINE);
        code.append(indenter).append(ELEMENT3).append(" + '}';").append(NEW_LINE);
        indenter.decreaseLevel(3);
        code.append(indenter).append('}').append(NEW_LINE);

        code.append(NEW_LINE);

        code.append(indenter).append("@Override").append(NEW_LINE);
        code.append(indenter).append("public ").append(className)
                .append(" canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {").append(NEW_LINE);
        indenter.increaseLevel();
        if (firstIsObject) {
            code.append(indenter).append("final ").append(class1Name).append(' ').append(CANONICALIZED_ELEMENT1)
                    .append(" = canonicalizer.apply(").append(ELEMENT1).append(");").append(NEW_LINE);
        }
        if (secondIsObject) {
            code.append(indenter).append("final ").append(class2Name).append(' ').append(CANONICALIZED_ELEMENT2)
                    .append(" = canonicalizer.apply(").append(ELEMENT2).append(");").append(NEW_LINE);
        }
        if (thirdIsObject) {
            code.append(indenter).append("final ").append(class3Name).append(' ').append(CANONICALIZED_ELEMENT3)
                    .append(" = canonicalizer.apply(").append(ELEMENT3).append(");").append(NEW_LINE);
        }
        if (firstIsObject && secondIsObject && thirdIsObject) {
            code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                    .append(" && ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2)
                    .append(" && ").append(CANONICALIZED_ELEMENT3).append(" == ").append(ELEMENT3).append(NEW_LINE);
            indenter.increaseLevel(2);
            code.append(indenter).append("? this : new ").append(className).append('(').append(CANONICALIZED_ELEMENT1)
                    .append(", ").append(CANONICALIZED_ELEMENT2)
                    .append(", ").append(CANONICALIZED_ELEMENT3).append(");").append(NEW_LINE);
            indenter.decreaseLevel(2);
        } else if (firstIsObject) {
            if (secondIsObject) {
                code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                        .append(" && ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2).append(NEW_LINE);
                indenter.increaseLevel(2);
                code.append(indenter).append("? this : new ").append(className).append('(')
                        .append(CANONICALIZED_ELEMENT1)
                        .append(", ").append(CANONICALIZED_ELEMENT2)
                        .append(", ").append(ELEMENT3).append(");").append(NEW_LINE);
                indenter.decreaseLevel(2);
            } else if (thirdIsObject) {
                code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                        .append(" && ").append(CANONICALIZED_ELEMENT3).append(" == ").append(ELEMENT3).append(NEW_LINE);
                indenter.increaseLevel(2);
                code.append(indenter).append("? this : new ").append(className).append('(')
                        .append(CANONICALIZED_ELEMENT1)
                        .append(", ").append(ELEMENT2)
                        .append(", ").append(CANONICALIZED_ELEMENT3).append(");").append(NEW_LINE);
                indenter.decreaseLevel(2);
            } else {
                code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT1).append(" == ").append(ELEMENT1)
                        .append(NEW_LINE);
                indenter.increaseLevel(2);
                code.append(indenter).append("? this : new ").append(className).append('(')
                        .append(CANONICALIZED_ELEMENT1)
                        .append(", ").append(ELEMENT2)
                        .append(", ").append(ELEMENT3).append(");").append(NEW_LINE);
                indenter.decreaseLevel(2);
            }
        } else if (secondIsObject) {
            if (thirdIsObject) {
                code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2)
                        .append(" && ").append(CANONICALIZED_ELEMENT3).append(" == ").append(ELEMENT3).append(NEW_LINE);
                indenter.increaseLevel(2);
                code.append(indenter).append("? this : new ").append(className).append('(').append(ELEMENT1)
                        .append(", ").append(CANONICALIZED_ELEMENT2)
                        .append(", ").append(CANONICALIZED_ELEMENT3).append(");").append(NEW_LINE);
                indenter.decreaseLevel(2);
            } else {
                code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT2).append(" == ").append(ELEMENT2)
                        .append(NEW_LINE);
                indenter.increaseLevel(2);
                code.append(indenter).append("? this : new ").append(className).append('(').append(ELEMENT1)
                        .append(", ").append(CANONICALIZED_ELEMENT2)
                        .append(", ").append(ELEMENT3).append(");").append(NEW_LINE);
                indenter.decreaseLevel(2);
            }
        } else if (thirdIsObject) {
            code.append(indenter).append("return ").append(CANONICALIZED_ELEMENT3).append(" == ").append(ELEMENT3)
                    .append(NEW_LINE);
            indenter.increaseLevel(2);
            code.append(indenter).append("? this : new ").append(className).append('(').append(ELEMENT1)
                    .append(", ").append(ELEMENT2)
                    .append(", ").append(CANONICALIZED_ELEMENT3).append(");").append(NEW_LINE);
            indenter.decreaseLevel(2);
        } else {
            code.append(indenter).append("return this;").append(NEW_LINE);
        }
        indenter.decreaseLevel(1);
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
        final TupleCodeGenerator generator = new TupleCodeGenerator();
        Arrays.stream(ElementType.values()).forEach(t1 -> Arrays.stream(ElementType.values()).forEach(t2 -> {
            final String doubleName = generator.generateClassName(t1, t2);
            final String doubleBody = generator.generateDouble(doubleName, t1, t2);
            generator.writeClass(doubleName, doubleBody);
            Arrays.stream(ElementType.values()).forEach(t3 -> {
                final String tripleName = generator.generateClassName(t1, t2, t3);
                final String tripleBody = generator.generateTriple(tripleName, t1, t2, t3);
                generator.writeClass(tripleName, tripleBody);
            });
        }));
    }
}
