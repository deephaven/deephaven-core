package io.deephaven.plot.util;

import io.deephaven.libs.GroovyStaticImportGenerator.JavaFunction;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class GenerateFigureAPI {

    private static class Key implements Comparable<Key>{
        private final String name;
        private final boolean isStatic;
        private final boolean isPublic;

        public Key(final Method m) {
            this.name = m.getName();
            this.isStatic = Modifier.isStatic(m.getModifiers());
            this.isPublic = Modifier.isPublic(m.getModifiers());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return isStatic == key.isStatic && isPublic == key.isPublic && Objects.equals(name, key.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, isStatic, isPublic);
        }

        @Override
        public String toString() {
            return "Key{" +
                    "name='" + name + '\'' +
                    ", isStatic=" + isStatic +
                    ", isPublic=" + isPublic +
                    '}';
        }

        @Override
        public int compareTo(@NotNull Key o) {
            final int c1 = this.name.compareTo(o.name);

            if(c1 != 0){
                return c1;
            }

            if(this.isStatic != o.isStatic){
                if(this.isStatic){
                    return 1;
                } else {
                    return -1;
                }
            }

            if(this.isPublic != o.isPublic){
                if(this.isPublic){
                    return 1;
                } else {
                    return -1;
                }
            }

            return 0;
        }
    }

    public static Map<Key, ArrayList<JavaFunction>> getSignatures(final String imp) throws ClassNotFoundException {
            final Class<?> c = Class.forName(imp);
            final Map<Key, ArrayList<JavaFunction>> signatures = new TreeMap<>();

            for (final Method m : c.getMethods()) {
                if(!m.getReturnType().getTypeName().equals(imp)){
                    // only look at methods of the plot builder
                    continue;
                }

                final Key key = new Key(m);
                final JavaFunction f = new JavaFunction(m);
                final ArrayList<JavaFunction> sigs = signatures.computeIfAbsent(key, k -> new ArrayList<>());
                sigs.add(f);
            }

            return signatures;
    }

    private static void printSignature(final Key key, final ArrayList<JavaFunction> signatures){
        System.out.println("-----------------------------------------------------------------------");

        System.out.println("Name: " + key.name);
        System.out.println("IsPublic: " + key.isPublic );
        System.out.println("IsStatic: " + key.isStatic);

        final Set<String> returnTypes = new TreeSet<>();
        final Map<String, Set<String>> params = new TreeMap<>();

        for(final JavaFunction f : signatures) {
            returnTypes.add(f.getReturnType().getTypeName());

            for(int i=0; i<f.getParameterNames().length; i++){
                final Set<String> paramTypes = params.computeIfAbsent(f.getParameterNames()[i], n-> new TreeSet<>());
                paramTypes.add(f.getParameterTypes()[i].getTypeName());
            }
        }

        System.out.println("ReturnTypes: ");

        for (String returnType : returnTypes) {
            System.out.println("\t" + returnType);
        }

        System.out.println("Params:");

        for (Map.Entry<String, Set<String>> entry : params.entrySet()) {
            System.out.println("\t" + entry.getKey() + "=" + entry.getValue());
        }

        System.out.println("Signatures:");

        for(final JavaFunction f : signatures){
            StringBuilder sig = new StringBuilder(f.getReturnType().getTypeName());
            sig.append(" (");

            for(int i=0; i<f.getParameterNames().length; i++){
                if(i >0){
                    sig.append(", ");
                }

                sig.append(f.getParameterNames()[i]).append("=").append(f.getParameterTypes()[i].getTypeName());
            }

            sig.append(")");

            System.out.println("\t" + sig);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {

        final String imp = "io.deephaven.plot.Figure";
        final Map<Key, ArrayList<JavaFunction>> signatures = getSignatures(imp);

        System.out.println(signatures);

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            if(!entry.getKey().isPublic){
                continue;
            }

            printSignature(entry.getKey(), entry.getValue());
        }
    }
}
