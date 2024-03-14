//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.*;

import java.util.Collection;
import java.util.Collections;

/**
 * Simple profiler that just records how many rows were in the result (presuming you set the result size).
 */
public class ResultHashProfiler implements InternalProfiler {
    private static String resultHash;

    @Override
    public String getDescription() {
        return "Result Hash";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        resultHash = "";
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams,
            IterationResult result) {
        return Collections.singletonList(new TextResult(resultHash, "Result Hash"));
    }

    public static void setResultHash(String hash) {
        ResultHashProfiler.resultHash = hash;
    }
}
