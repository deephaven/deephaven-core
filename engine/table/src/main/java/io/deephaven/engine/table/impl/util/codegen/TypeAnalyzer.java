package io.deephaven.engine.table.impl.util.codegen;

import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.util.type.TypeUtils;

/**
 * When {@code type} is either a primitive type or a boxed type, {@code enginePrimitiveType} is the type used by the
 * engine to represent it -- that is, Boolean for either boolean or Boolean results, but the primitive version of all
 * other primitive/boxed types. For all other types (e.g. misc Objects), {@code enginePrimitiveType} is null.
 **/
public class TypeAnalyzer {
    public static TypeAnalyzer create(Class type) {
        final Class enginePrimitiveType;
        final String chunkVariableBase;
        final String chunkExtendsGenericArgs;
        final String chunkSuperGenericArgs;
        final String chunkTypeString;
        if (type == Boolean.class || type == boolean.class) {
            enginePrimitiveType = Boolean.class;
            chunkVariableBase = "ObjectChunk";
            chunkExtendsGenericArgs = "<java.lang.Boolean, ? extends Values>";
            chunkSuperGenericArgs = "<java.lang.Boolean, ? super Values>";
            chunkTypeString = "Object";
        } else {
            // primitive -> unchanged
            // boxed -> primitive
            // other -> null
            enginePrimitiveType = TypeUtils.getUnboxedType(type);
            if (enginePrimitiveType == null) {
                chunkVariableBase = "ObjectChunk";
                chunkExtendsGenericArgs = String.format("<%s, ? extends Values>", type.getCanonicalName());
                chunkSuperGenericArgs = String.format("<%s, ? super Values>", type.getCanonicalName());
                chunkTypeString = "Object";
            } else {
                final String simpleName = enginePrimitiveType.getSimpleName();
                final String camelCasedName = Character.toUpperCase(simpleName.charAt(0)) + simpleName.substring(1);
                chunkVariableBase = camelCasedName + "Chunk";
                chunkExtendsGenericArgs = "<? extends Values>";
                chunkSuperGenericArgs = "<? super Values>";
                chunkTypeString = camelCasedName;
            }
        }

        final String returnTypeName =
                enginePrimitiveType == null ? type.getCanonicalName() : enginePrimitiveType.getName();
        final String readChunkVariableType = chunkVariableBase + chunkExtendsGenericArgs;
        final String writeChunkVariableType = "Writable" + chunkVariableBase + chunkSuperGenericArgs;
        final String asReadMethod = "as" + chunkVariableBase;
        final String asWritableMethod = "asWritable" + chunkVariableBase;
        return new TypeAnalyzer(type, enginePrimitiveType, returnTypeName, chunkTypeString, readChunkVariableType,
                writeChunkVariableType, asReadMethod, asWritableMethod);
    }

    public final Class type;
    public final Class enginePrimitiveType;
    public final String typeString;
    public final String chunkTypeString;
    public final String readChunkVariableType;
    public final String writableChunkVariableType;
    public final String asReadChunkMethodName;
    public final String asWritableChunkMethodName;

    private TypeAnalyzer(final Class type, final Class enginePrimitiveType, final String typeString,
            final String chunkTypeString,
            final String readChunkVariableType, final String writableChunkVariableType,
            final String asReadChunkMethodName, final String asWritableChunkMethodName) {
        this.type = type;
        this.enginePrimitiveType = enginePrimitiveType;
        this.typeString = typeString;
        this.chunkTypeString = chunkTypeString;
        this.readChunkVariableType = readChunkVariableType;
        this.writableChunkVariableType = writableChunkVariableType;
        this.asReadChunkMethodName = asReadChunkMethodName;
        this.asWritableChunkMethodName = asWritableChunkMethodName;
    }

    /**
     * A DhFormulaColumn will unbox the result of any formula that returns a boxed type (except Boolean). If the formula
     * returns null, this could trigger a NullPointerException.
     *
     * @param formulaString The formula to potentially wrap with a cast function
     */
    public String wrapWithCastIfNecessary(String formulaString) {
        if (type.isPrimitive() // Implies dbPrimitiveType.equals(type); no risk of NPE
                || enginePrimitiveType == Boolean.class // No risk of NPE
                || enginePrimitiveType == null) // Return type is not a primitive or boxed type
        {
            return formulaString; // No need to cast
        }

        // Otherwise, perform perform a null-safe unboxing cast
        return QueryLanguageFunctionUtils.class.getCanonicalName() + '.' + enginePrimitiveType.getName() + "Cast("
                + formulaString + ')';
    }
}
