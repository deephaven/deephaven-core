//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import groovyjarjarantlr4.v4.runtime.misc.NotNull;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class HasherConfig<T> {
    final Class<T> baseClass;
    public final String classPrefix;
    public final String packageGroup;
    public final String packageMiddle;
    final boolean openAddressedAlternate;
    final boolean supportTombstones;
    final boolean alwaysMoveMain;
    final String mainStateName;
    final String overflowOrAlternateStateName;
    final String emptyStateName;
    public final String tombstoneStateName;
    final Class<?> stateType;
    final Consumer<CodeBlock.Builder> moveMainFull;
    final Consumer<CodeBlock.Builder> moveMainAlternate;
    final Consumer<CodeBlock.Builder> rehashFullSetup;
    final boolean includeOriginalSources;
    final boolean supportRehash;
    final List<BiFunction<HasherConfig<T>, ChunkType[], MethodSpec>> extraMethods;
    final List<ParameterSpec> extraPartialRehashParameters;
    final List<ProbeSpec> probes;
    final List<BuildSpec> builds;

    HasherConfig(Class<T> baseClass, String classPrefix, String packageGroup, String packageMiddle,
            boolean openAddressedAlternate,
            boolean supportTombstones,
            boolean alwaysMoveMain,
            boolean includeOriginalSources,
            boolean supportRehash,
            String mainStateName,
            String overflowOrAlternateStateName,
            String emptyStateName,
            String tombstoneStateName,
            Class<?> stateType,
            Consumer<CodeBlock.Builder> moveMainFull,
            Consumer<CodeBlock.Builder> moveMainAlternate,
            Consumer<CodeBlock.Builder> rehashFullSetup,
            List<ParameterSpec> extraPartialRehashParameters,
            List<ProbeSpec> probes,
            List<BuildSpec> builds,
            List<BiFunction<HasherConfig<T>, ChunkType[], MethodSpec>> extraMethods) {
        this.baseClass = baseClass;
        this.classPrefix = classPrefix;
        this.packageGroup = packageGroup;
        this.packageMiddle = packageMiddle;
        this.openAddressedAlternate = openAddressedAlternate;
        this.supportTombstones = supportTombstones;
        this.alwaysMoveMain = alwaysMoveMain;
        this.includeOriginalSources = includeOriginalSources;
        this.supportRehash = supportRehash;
        this.mainStateName = mainStateName;
        this.overflowOrAlternateStateName = overflowOrAlternateStateName;
        this.emptyStateName = emptyStateName;
        this.tombstoneStateName = tombstoneStateName;
        this.stateType = stateType;
        this.moveMainFull = moveMainFull;
        this.moveMainAlternate = moveMainAlternate;
        this.rehashFullSetup = rehashFullSetup;
        this.extraPartialRehashParameters = extraPartialRehashParameters;
        this.probes = probes;
        this.builds = builds;
        this.extraMethods = extraMethods;
    }

    @FunctionalInterface
    interface FoundMethodBuilder {
        void accept(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder);
    }

    static class ProbeSpec {
        final String name;
        final String stateValueName;
        final boolean requiresRowKeyChunk;
        final FoundMethodBuilder found;
        final Consumer<CodeBlock.Builder> missing;
        final ParameterSpec[] params;

        public ProbeSpec(String name, String stateValueName, boolean requiresRowKeyChunk,
                FoundMethodBuilder found,
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
        @FunctionalInterface
        public interface MethodBuilder {
            void accept(HasherConfig<?> config, CodeBlock.Builder builder);
        }

        @FunctionalInterface
        public interface MethodBuilderWithChunkTypes {
            void accept(HasherConfig<?> config, ChunkType[] chunkTypes, CodeBlock.Builder builder);
        }

        final String name;
        final String stateValueName;
        final boolean requiresRowKeyChunk;
        /**
         * In cases where we know that a partial rehash cannot be occurring; we can simplify the generated code by not
         * checking for the alternate table.
         */
        final boolean allowAlternates;
        /**
         * In cases where we know that an entry cannot possibly be deleted; we can simplify the generated code by not
         * checking for tombstones.
         */
        final boolean checkDeletions;
        final FoundMethodBuilder found;
        final MethodBuilderWithChunkTypes insert;
        final ParameterSpec[] params;

        public BuildSpec(String name, String stateValueName, boolean requiresRowKeyChunk,
                boolean allowAlternates, boolean checkDeletions, FoundMethodBuilder found,
                MethodBuilder insert, ParameterSpec... params) {
            // Convert the MethodBuilder to MethodBuilderWithChunkTypes.
            this(name,
                    stateValueName,
                    requiresRowKeyChunk,
                    allowAlternates,
                    checkDeletions,
                    found,
                    (config, chunkTypes, builder) -> insert.accept(config, builder),
                    params);
        }

        public BuildSpec(String name, String stateValueName, boolean requiresRowKeyChunk,
                boolean allowAlternates, boolean checkDeletions, FoundMethodBuilder found,
                MethodBuilderWithChunkTypes insert,
                ParameterSpec... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.requiresRowKeyChunk = requiresRowKeyChunk;
            this.allowAlternates = allowAlternates;
            this.checkDeletions = checkDeletions;
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
        private boolean supportTombstones = false;
        private boolean openAddressedAlternate = true;
        private boolean alwaysMoveMain = false;
        private boolean includeOriginalSources = false;
        private boolean supportRehash = true;
        private String mainStateName;
        private String overflowOrAlternateStateName;
        private String emptyStateName;
        private String tombstoneStateName;
        private Class<?> stateType;
        private Consumer<CodeBlock.Builder> moveMainAlternate;
        private Consumer<CodeBlock.Builder> moveMainFull;
        private Consumer<CodeBlock.Builder> rehashFullSetup;
        private final List<ParameterSpec> extraPartialRehashParameters = new ArrayList<>();
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

        public Builder<T> openAddressedAlternate(boolean openAddressedAlternate) {
            this.openAddressedAlternate = openAddressedAlternate;
            return this;
        }

        public Builder<T> supportTombstones(boolean supportTombstones) {
            this.supportTombstones = supportTombstones;
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

        public Builder<T> tombstoneStateName(String tombstoneStateName) {
            this.tombstoneStateName = tombstoneStateName;
            return this;
        }

        public Builder<T> stateType(Class<?> stateType) {
            this.stateType = stateType;
            return this;
        }

        public Builder<T> moveMainAlternate(Consumer<CodeBlock.Builder> moveMainAlternate) {
            this.moveMainAlternate = moveMainAlternate;
            return this;
        }

        public Builder<T> moveMainFull(Consumer<CodeBlock.Builder> moveMainFull) {
            this.moveMainFull = moveMainFull;
            return this;
        }

        public Builder<T> rehashFullSetup(Consumer<CodeBlock.Builder> rehashFullSetup) {
            this.rehashFullSetup = rehashFullSetup;
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

        public Builder<T> addExtraPartialRehashParameter(ParameterSpec paramSpec) {
            extraPartialRehashParameters.add(paramSpec);
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
            if (openAddressedAlternate) {
                Assert.neqNull(overflowOrAlternateStateName, "overflowOrAlternateStateName");
            }
            Assert.neqNull(emptyStateName, "emptyStateName");
            Assert.neqNull(stateType, "stateType");

            return new HasherConfig<>(baseClass, classPrefix, packageGroup, packageMiddle,
                    openAddressedAlternate, supportTombstones, alwaysMoveMain, includeOriginalSources, supportRehash,
                    mainStateName,
                    overflowOrAlternateStateName, emptyStateName, tombstoneStateName,
                    stateType, moveMainFull, moveMainAlternate, rehashFullSetup, extraPartialRehashParameters, probes,
                    builds, extraMethods);
        }
    }
}
