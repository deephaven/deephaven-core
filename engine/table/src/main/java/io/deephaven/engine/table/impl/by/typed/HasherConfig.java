package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.ParameterSpec;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class HasherConfig<T> {
    final Class<T> baseClass;
    public final String classPrefix;
    public final String packageGroup;
    public final String packageMiddle;
    final boolean openAddressed;
    final boolean openAddressedAlternate;
    final boolean alwaysMoveMain;
    final String mainStateName;
    final String overflowOrAlternateStateName;
    final String emptyStateName;
    final Class<?> stateType;
    final Consumer<CodeBlock.Builder> moveMain;
    final List<ProbeSpec> probes;
    final List<BuildSpec> builds;

    HasherConfig(Class<T> baseClass, String classPrefix, String packageGroup, String packageMiddle,
            boolean openAddressed,
            boolean openAddressedAlternate, boolean alwaysMoveMain, String mainStateName,
            String overflowOrAlternateStateName,
            String emptyStateName, Class<?> stateType, Consumer<CodeBlock.Builder> moveMain, List<ProbeSpec> probes,
            List<BuildSpec> builds) {
        this.baseClass = baseClass;
        this.classPrefix = classPrefix;
        this.packageGroup = packageGroup;
        this.packageMiddle = packageMiddle;
        this.openAddressed = openAddressed;
        this.openAddressedAlternate = openAddressedAlternate;
        this.alwaysMoveMain = alwaysMoveMain;
        this.mainStateName = mainStateName;
        this.overflowOrAlternateStateName = overflowOrAlternateStateName;
        this.emptyStateName = emptyStateName;
        this.stateType = stateType;
        this.moveMain = moveMain;
        this.probes = probes;
        this.builds = builds;
    }

    static class ProbeSpec {
        final String name;
        final String stateValueName;
        final Consumer<CodeBlock.Builder> found;
        final Consumer<CodeBlock.Builder> missing;
        final ParameterSpec[] params;

        public ProbeSpec(String name, String stateValueName, Consumer<CodeBlock.Builder> found,
                Consumer<CodeBlock.Builder> missing, ParameterSpec... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.found = found;
            this.missing = missing;
            this.params = params;
        }
    }

    static class BuildSpec {
        final String name;
        final String stateValueName;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> found;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert;
        final ParameterSpec[] params;

        public BuildSpec(String name, String stateValueName, BiConsumer<HasherConfig<?>, CodeBlock.Builder> found,
                BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert, ParameterSpec... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.found = found;
            this.insert = insert;
            this.params = params;
        }
    }
}
