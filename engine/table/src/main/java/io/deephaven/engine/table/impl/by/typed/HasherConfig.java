package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import groovyjarjarantlr4.v4.runtime.misc.NotNull;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
    final boolean includeOriginalSources;
    final boolean supportRehash;
    final List<BiFunction<HasherConfig<T>, ChunkType[], MethodSpec>> extraMethods;
    final List<ProbeSpec> probes;
    final List<BuildSpec> builds;

    HasherConfig(Class<T> baseClass, String classPrefix, String packageGroup, String packageMiddle,
            boolean openAddressed,
            boolean openAddressedAlternate, boolean alwaysMoveMain,
            boolean includeOriginalSources,
            boolean supportRehash,
            String mainStateName,
            String overflowOrAlternateStateName,
            String emptyStateName, Class<?> stateType, Consumer<CodeBlock.Builder> moveMain,
            List<ProbeSpec> probes,
            List<BuildSpec> builds,
            List<BiFunction<HasherConfig<T>, ChunkType[], MethodSpec>> extraMethods) {
        this.baseClass = baseClass;
        this.classPrefix = classPrefix;
        this.packageGroup = packageGroup;
        this.packageMiddle = packageMiddle;
        this.openAddressed = openAddressed;
        this.openAddressedAlternate = openAddressedAlternate;
        this.alwaysMoveMain = alwaysMoveMain;
        this.includeOriginalSources = includeOriginalSources;
        this.supportRehash = supportRehash;
        this.mainStateName = mainStateName;
        this.overflowOrAlternateStateName = overflowOrAlternateStateName;
        this.emptyStateName = emptyStateName;
        this.stateType = stateType;
        this.moveMain = moveMain;
        this.probes = probes;
        this.builds = builds;
        this.extraMethods = extraMethods;
    }

    static class ProbeSpec {
        final String name;
        final String stateValueName;
        final boolean requiresRowKeyChunk;
        final Consumer<CodeBlock.Builder> found;
        final Consumer<CodeBlock.Builder> missing;
        final ParameterSpec[] params;

        public ProbeSpec(String name, String stateValueName, boolean requiresRowKeyChunk, Consumer<CodeBlock.Builder> found,
                         Consumer<CodeBlock.Builder> missing, ParameterSpec... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.requiresRowKeyChunk = requiresRowKeyChunk;
            this.found = found;
            this.missing = missing;
            this.params = params;
        }
    }

    static class BuildSpec {
        final String name;
        final String stateValueName;
        final boolean requiresRowKeyChunk;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> found;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert;
        final ParameterSpec[] params;

        public BuildSpec(String name, String stateValueName, boolean requiresRowKeyChunk, BiConsumer<HasherConfig<?>, CodeBlock.Builder> found,
                         BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert, ParameterSpec... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.requiresRowKeyChunk = requiresRowKeyChunk;
            this.found = found;
            this.insert = insert;
            this.params = params;
        }
    }

    public static class Builder<T> {
        private final Class<T> baseClass;
        private String classPrefix;
        private String packageGroup;
        private String packageMiddle;
        private boolean openAddressed = true;
        private boolean openAddressedAlternate = true;
        private boolean alwaysMoveMain = false;
        private boolean includeOriginalSources = false;
        private boolean supportRehash = true;
        private String mainStateName;
        private String overflowOrAlternateStateName;
        private String emptyStateName;
        private Class<?> stateType;
        private Consumer<CodeBlock.Builder> moveMain;
        private final List<ProbeSpec> probes = new ArrayList<>();
        private final List<BuildSpec> builds = new ArrayList<>();
        private final List<BiFunction<HasherConfig<T>, ChunkType[], MethodSpec>> extraMethods = new ArrayList<>();

        Builder(@NotNull Class<T> baseClass) {
            this.baseClass = baseClass;
        }

        public Builder<T> classPrefix(String classPrefix) {
            this.classPrefix = classPrefix;
            return this;
        }

        public Builder<T> packageGroup(String packageGroup) {
            this.packageGroup = packageGroup;
            return this;
        }

        public Builder<T> packageMiddle(String packageMiddle) {
            this.packageMiddle = packageMiddle;
            return this;
        }

        public Builder<T> openAddressed(boolean openAddressed) {
            this.openAddressed = openAddressed;
            if (!openAddressed) {
                this.openAddressedAlternate = false;
            }
            return this;
        }

        public Builder<T> openAddressedAlternate(boolean openAddressedAlternate) {
            this.openAddressedAlternate = openAddressedAlternate;
            return this;
        }

        public Builder<T> alwaysMoveMain(boolean alwaysMoveMain) {
            this.alwaysMoveMain = alwaysMoveMain;
            return this;
        }

        public Builder<T> includeOriginalSources(boolean includeOriginalSources) {
            this.includeOriginalSources = includeOriginalSources;
            return this;
        }

        public Builder<T> supportRehash(boolean supportRehash) {
            this.supportRehash = supportRehash;
            return this;
        }

        public Builder<T> mainStateName(String mainStateName) {
            this.mainStateName = mainStateName;
            return this;
        }

        public Builder<T> overflowOrAlternateStateName(String overflowOrAlternateStateName) {
            this.overflowOrAlternateStateName = overflowOrAlternateStateName;
            return this;
        }

        public Builder<T> emptyStateName(String emptyStateName) {
            this.emptyStateName = emptyStateName;
            return this;
        }

        public Builder<T> stateType(Class<?> stateType) {
            this.stateType = stateType;
            return this;
        }

        public Builder<T> moveMain(Consumer<CodeBlock.Builder> moveMain) {
            this.moveMain = moveMain;
            return this;
        }

        public Builder<T> addProbe(ProbeSpec probe) {
            probes.add(probe);
            return this;
        }

        public Builder<T> addBuild(BuildSpec build) {
            builds.add(build);
            return this;
        }

        public Builder<T> addExtraMethod(BiFunction<HasherConfig<T>, ChunkType[], MethodSpec> methodProducer) {
            extraMethods.add(methodProducer);
            return this;
        }

        HasherConfig<T> build() {
            Assert.neqNull(classPrefix, "classPrefix");
            Assert.neqNull(packageGroup, "packageGroup");
            Assert.neqNull(packageMiddle, "packageMiddle");
            Assert.neqNull(mainStateName, "mainStateName");
            if (openAddressedAlternate || !openAddressed) {
                Assert.neqNull(overflowOrAlternateStateName, "overflowOrAlternateStateName");
            }
            Assert.neqNull(emptyStateName, "emptyStateName");
            Assert.neqNull(stateType, "stateType");

            return new HasherConfig<>(baseClass, classPrefix, packageGroup, packageMiddle, openAddressed,
                    openAddressedAlternate, alwaysMoveMain, includeOriginalSources, supportRehash, mainStateName,
                    overflowOrAlternateStateName, emptyStateName,
                    stateType, moveMain, probes, builds, extraMethods);
        }
    }
}
