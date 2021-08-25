package io.deephaven.db.v2.utils.codegen;

import io.deephaven.db.tables.lang.DBLanguageFunctionUtil;
import io.deephaven.util.type.TypeUtils;

/**
 * When {@code type} is either a primitive type or a boxed type, {@code dbPrimitiveType} is the type used by the DB to
 * represent it -- that is, Boolean for either boolean or Boolean results, but the primitive version of all other
 * primitive/boxed types. For all other types (e.g. misc Objects), {@code dbPrimitiveType} is null.
 **/
public class TypeAnalyzer {
    public static TypeAnalyzer create(Class type) {
        final Class dbPrimitiveType;
        final String chunkVariableBase;
        final String chunkExtendsGenericArgs;
        final String chunkSuperGenericArgs;
        final String chunkTypeString;
        if (type == Boolean.class || type == boolean.class) {
            dbPrimitiveType = Boolean.class;
            chunkVariableBase = "ObjectChunk";
            chunkExtendsGenericArgs = "<java.lang.Boolean, ? extends Attributes.Values>";
            chunkSuperGenericArgs = "<java.lang.Boolean, ? super Attributes.Values>";
            chunkTypeString = "Object";
        } else {
            // primitive -> unchanged
            // boxed -> primitive
            // other -> null
            dbPrimitiveType = TypeUtils.getUnboxedType(type);
            if (dbPrimitiveType == null) {
                chunkVariableBase = "ObjectChunk";
                chunkExtendsGenericArgs = String.format("<%s, ? extends Attributes.Values>", type.getCanonicalName());
                chunkSuperGenericArgs = String.format("<%s, ? super Attributes.Values>", type.getCanonicalName());
                chunkTypeString = "Object";
            } else {
                final String simpleName = dbPrimitiveType.getSimpleName();
                final String camelCasedName = Character.toUpperCase(simpleName.charAt(0)) + simpleName.substring(1);
                chunkVariableBase = camelCasedName + "Chunk";
                chunkExtendsGenericArgs = "<? extends Attributes.Values>";
                chunkSuperGenericArgs = "<? super Attributes.Values>";
                chunkTypeString = camelCasedName;
            }
        }

        final String returnTypeName =
                dbPrimitiveType == null ? type.getCanonicalName() : dbPrimitiveType.getName();
        final String readChunkVariableType = chunkVariableBase + chunkExtendsGenericArgs;
        final String writeChunkVariableType = "Writable" + chunkVariableBase + chunkSuperGenericArgs;
        final String asReadMethod = "as" + chunkVariableBase;
        final String asWritableMethod = "asWritable" + chunkVariableBase;
        return new TypeAnalyzer(type, dbPrimitiveType, returnTypeName, chunkTypeString, readChunkVariableType,
                writeChunkVariableType, asReadMethod, asWritableMethod);
    }

    public final Class type;
    public final Class dbPrimitiveType;
    public final String typeString;
    public final String chunkTypeString;
    public final String readChunkVariableType;
    public final String writableChunkVariableType;
    public final String asReadChunkMethodName;
    public final String asWritableChunkMethodName;

    private TypeAnalyzer(final Class type, final Class dbPrimitiveType, final String typeString,
            final String chunkTypeString,
            final String readChunkVariableType, final String writableChunkVariableType,
            final String asReadChunkMethodName, final String asWritableChunkMethodName) {
        this.type = type;
        this.dbPrimitiveType = dbPrimitiveType;
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
                || dbPrimitiveType == Boolean.class // No risk of NPE
                || dbPrimitiveType == null) // Return type is not a primitive or boxed type
        {
            return formulaString; // No need to cast
        }

        // Otherwise, perform perform a null-safe unboxing cast
        return DBLanguageFunctionUtil.class.getCanonicalName() + '.' + dbPrimitiveType.getName() + "Cast("
                + formulaString + ')';
    }
}
