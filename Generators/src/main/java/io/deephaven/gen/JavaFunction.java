package io.deephaven.gen;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * A Java function description for use in code generation.
 */
public class JavaFunction implements Comparable<JavaFunction> {
    private static final Logger log = Logger.getLogger(JavaFunction.class.toString());

    private final String className;
    private final String classNameShort;
    private final String methodName;
    private final TypeVariable<Method>[] typeParameters;
    private final Type returnType;
    private final Type[] parameterTypes;
    private final String[] parameterNames;
    private final boolean isVarArgs;

    public JavaFunction(final String className, final String classNameShort, final String methodName,
                        final TypeVariable<Method>[] typeParameters, final Type returnType, final Type[] parameterTypes,
                        final String[] parameterNames, final boolean isVarArgs) {
        this.className = className;
        this.classNameShort = classNameShort;
        this.methodName = methodName;
        this.typeParameters = typeParameters;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.parameterNames = parameterNames;
        this.isVarArgs = isVarArgs;
    }

    public JavaFunction(final Method m) {
        this(
                m.getDeclaringClass().getCanonicalName(),
                m.getDeclaringClass().getSimpleName(),
                m.getName(),
                m.getTypeParameters(),
                m.getGenericReturnType(),
                m.getGenericParameterTypes(),
                Arrays.stream(m.getParameters()).map(Parameter::getName).toArray(String[]::new),
                m.isVarArgs());

        for (Parameter parameter : m.getParameters()) {
            if (!parameter.isNamePresent()) {
                throw new IllegalArgumentException(
                        "Parameter names are not present in the code!  Was the code compiled with \"-parameters\": "
                                + this);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JavaFunction that = (JavaFunction) o;

        if (!Objects.equals(methodName, that.methodName))
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parameterTypes, that.parameterTypes);

    }

    @Override
    public int hashCode() {
        int result = methodName != null ? methodName.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(parameterTypes);
        return result;
    }

    @Override
    public String toString() {
        return "JavaFunction{" +
                "className='" + className + '\'' +
                ", methodName='" + methodName + '\'' +
                ", typeParameters=" + Arrays.toString(typeParameters) +
                ", returnType=" + returnType +
                ", parameterTypes=" + Arrays.toString(parameterTypes) +
                ", parameterNames=" + Arrays.toString(parameterNames) +
                '}';
    }

    @Override
    public int compareTo(@NotNull JavaFunction o) {
        int cm = methodName.compareTo(o.methodName);
        if (cm != 0) {
            return cm;
        }
        if (parameterTypes.length != o.parameterTypes.length) {
            return parameterTypes.length - o.parameterTypes.length;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Type t1 = parameterTypes[i];
            Type t2 = o.parameterTypes[i];
            int ct = t1.toString().compareTo(t2.toString());
            if (ct != 0) {
                return ct;
            }
        }

        return 0;
    }

    public String getClassName() {
        return className;
    }

    public String getClassNameShort() {
        return classNameShort;
    }

    public String getMethodName() {
        return methodName;
    }

    public TypeVariable<Method>[] getTypeParameters() {
        return typeParameters;
    }

    public Type getReturnType() {
        return returnType;
    }

    public Class<?> getReturnClass() {
        if (returnType == null) {
            return null;
        }

        try {
            return TypeUtils.getErasedType(returnType);
        } catch (UnsupportedOperationException e) {
            log.warning("Unable to determine Class from returnType=" + returnType.getTypeName());
            return null;
        }
    }

    public Type[] getParameterTypes() {
        return parameterTypes;
    }

    public String[] getParameterNames() {
        return parameterNames;
    }

    public boolean isVarArgs() {
        return isVarArgs;
    }
}
