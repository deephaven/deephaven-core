package io.deephaven.libs;

import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import io.deephaven.util.type.TypeUtils;


/**
 * Groovy has a bug where performing a static import on multiple libraries containing functions with the same name
 * causes some of the functions to not be present in the namespace. This class combines static imports from multiple
 * sources into a single class that can be imported.
 */
public class GroovyStaticImportGenerator {
    private static Logger log = Logger.getLogger(GroovyStaticImportGenerator.class.toString());

    public static class JavaFunction implements Comparable<JavaFunction> {
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
                                    + toString());
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

            if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null)
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

        public Class getReturnClass() {
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


    private final Map<JavaFunction, JavaFunction> staticFunctions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skips;

    private GroovyStaticImportGenerator(final String[] imports, Collection<Predicate<JavaFunction>> skips)
            throws ClassNotFoundException {
        this.skips = skips;

        for (String imp : imports) {
            Class<?> c = Class.forName(imp);
            log.info("Processing class: " + c);

            for (Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                boolean isStatic = Modifier.isStatic(m.getModifiers());
                boolean isPublic = Modifier.isPublic(m.getModifiers());

                if (isStatic && isPublic) {
                    addPublicStatic(m);
                }
            }
        }
    }

    private void addPublicStatic(Method m) {
        log.info("Processing public static method: " + m);

        JavaFunction f = new JavaFunction(m);
        // System.out.println(f);

        if (staticFunctions.containsKey(f)) {
            JavaFunction fAlready = staticFunctions.get(f);
            final String message = "Signature Already Present:	" + fAlready + "\t" + f;
            log.severe(message);
            throw new RuntimeException(message);
        } else {
            log.info("Added public static method: " + f);
            staticFunctions.put(f, f);
        }
    }

    private Set<String> generateImports() {
        Set<String> imports = new TreeSet<>();

        for (JavaFunction f : staticFunctions.keySet()) {
            imports.add(f.className);

            imports.addAll(typesToImport(f.returnType));

            for (Type t : f.parameterTypes) {
                imports.addAll(typesToImport(t));
            }
        }

        return imports;
    }

    private static Set<String> typesToImport(Type t) {
        Set<String> result = new LinkedHashSet<>();

        if (t instanceof Class) {
            final Class<?> c = (Class) t;
            final boolean isArray = c.isArray();
            final boolean isPrimitive = c.isPrimitive();

            if (isPrimitive) {
                return result;
            } else if (isArray) {
                return typesToImport(c.getComponentType());
            } else {
                result.add(t.getTypeName());
            }
        } else if (t instanceof ParameterizedType) {
            final ParameterizedType pt = (ParameterizedType) t;
            result.add(pt.getRawType().getTypeName());

            for (Type a : pt.getActualTypeArguments()) {
                result.addAll(typesToImport(a));
            }
        } else if (t instanceof TypeVariable) {
            // type variables are generic so they don't need importing
            return result;
        } else if (t instanceof GenericArrayType) {
            GenericArrayType at = (GenericArrayType) t;
            return typesToImport(at.getGenericComponentType());
        } else {
            throw new UnsupportedOperationException("Unsupported Type type: " + t.getClass());
        }

        return result;
    }

    private String generateCode() {

        String code = "/*\n" +
                " * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
                " */\n\n" +
                "/****************************************************************************************************************************\n"
                +
                " ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GroovyStaticImportGenerator or \"./gradlew :Generators:groovyStaticImportGenerator\" to regenerate\n"
                +
                " ****************************************************************************************************************************/\n\n";

        code += "package io.deephaven.libs;\n\n";

        Set<String> imports = generateImports();

        for (String imp : imports) {
            code += "import " + imp + ";\n";
        }

        code += "\n";
        code += "/**\n";
        code += " * Functions statically imported into Groovy.\n";
        code += " *\n";
        code += " * @see io.deephaven.libs.primitives\n";
        code += " */\n";
        code += "public class GroovyStaticImports {\n";

        for (JavaFunction f : staticFunctions.keySet()) {
            boolean skip = false;
            for (Predicate<JavaFunction> skipCheck : skips) {
                skip = skip || skipCheck.test(f);
            }

            if (skip) {
                log.warning("*** Skipping function: " + f);
                continue;
            }

            String returnType = f.returnType.getTypeName();
            String s =
                    "    /** @see " + f.getClassName() + "#" + f.getMethodName() + "(" +
                            Arrays.stream(f.parameterTypes).map(t -> t.getTypeName().replace("<T>", ""))
                                    .collect(Collectors.joining(","))
                            +
                            ") */\n" +
                            "    public static ";

            if (f.typeParameters.length > 0) {
                s += "<";

                for (int i = 0; i < f.typeParameters.length; i++) {
                    if (i != 0) {
                        s += ",";
                    }

                    TypeVariable<Method> t = f.typeParameters[i];
                    log.info("BOUNDS: " + Arrays.toString(t.getBounds()));
                    s += t;

                    Type[] bounds = t.getBounds();

                    if (bounds.length != 1) {
                        throw new RuntimeException("Unsupported bounds: " + Arrays.toString(bounds));
                    }

                    Type bound = bounds[0];

                    if (!bound.equals(Object.class)) {
                        s += " extends " + bound.getTypeName();
                    }

                }

                s += ">";
            }

            s += " " + returnType + " " + f.methodName + "(";
            String callArgs = "";

            for (int i = 0; i < f.parameterTypes.length; i++) {
                if (i != 0) {
                    s += ",";
                    callArgs += ",";
                }

                Type t = f.parameterTypes[i];

                s += " " + t.getTypeName() + " " + f.parameterNames[i];
                callArgs += " " + f.parameterNames[i];
            }

            s += " ) {";
            s += "return " + f.classNameShort + "." + f.methodName + "(" + callArgs + " );";
            s += "}";

            code += s;
            code += "\n";
        }

        code += "}\n\n";

        return code;
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        String devroot = null;
        boolean assertNoChange = false;
        if (args.length == 0) {
            devroot = Configuration.getInstance().getDevRootPath();
        } else if (args.length == 1) {
            devroot = args[0];
        } else if (args.length == 2) {
            devroot = args[0];
            assertNoChange = Boolean.parseBoolean(args[1]);
        } else {
            System.out.println("Usage: [<devroot> [assertNoChange]]");
            System.exit(-1);
        }

        log.setLevel(Level.WARNING);
        log.warning("Running GroovyStaticImportGenerator assertNoChange=" + assertNoChange);

        final String[] imports = {
                "io.deephaven.libs.primitives.BooleanPrimitives",
                "io.deephaven.libs.primitives.ByteNumericPrimitives",
                "io.deephaven.libs.primitives.BytePrimitives",
                "io.deephaven.libs.primitives.CharacterPrimitives",
                "io.deephaven.libs.primitives.DoubleFpPrimitives",
                "io.deephaven.libs.primitives.DoubleNumericPrimitives",
                "io.deephaven.libs.primitives.DoublePrimitives",
                "io.deephaven.libs.primitives.FloatFpPrimitives",
                "io.deephaven.libs.primitives.FloatNumericPrimitives",
                "io.deephaven.libs.primitives.FloatPrimitives",
                "io.deephaven.libs.primitives.IntegerNumericPrimitives",
                "io.deephaven.libs.primitives.IntegerPrimitives",
                "io.deephaven.libs.primitives.LongNumericPrimitives",
                "io.deephaven.libs.primitives.LongPrimitives",
                "io.deephaven.libs.primitives.ObjectPrimitives",
                "io.deephaven.libs.primitives.ShortNumericPrimitives",
                "io.deephaven.libs.primitives.ShortPrimitives",
                "io.deephaven.libs.primitives.SpecialPrimitives",
                "io.deephaven.libs.primitives.ComparePrimitives",
                "io.deephaven.libs.primitives.Casting",
        };

        @SuppressWarnings("unchecked")
        GroovyStaticImportGenerator gen = new GroovyStaticImportGenerator(imports,
                Collections.singletonList((f) -> f.methodName.equals("sum") && f.parameterTypes.length == 1
                        && f.parameterTypes[0].getTypeName().contains("io.deephaven.db.tables.dbarrays.DbArray<")) // skipping
                                                                                                                   // common
                                                                                                                   // erasure
                                                                                                                   // "sum"
        );

        final String code = gen.generateCode();
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + "/DB/src/main/java/io/deephaven/libs/GroovyStaticImports.java";

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            if (!code.equals(oldCode)) {
                throw new RuntimeException(
                        "Change in generated code.  Run GroovyStaticImportGenerator or \"./gradlew :DB:groovyStaticImportGenerator\" to regenerate\n");
            }
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning("io.deephaven.libs.GroovyStaticImports.java written to: " + file);
        }
    }

}
