/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.modelfarm.ModelMultiExec.Models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * A set of multiple models where predicates determine which of the models execute on any iteration.
 * The predicates use input data and the most recent state for an underlying to determine which
 * models should execute on an iteration. For example, the state could be the most recent valid
 * result from a numerical model.
 * <p>
 * Active models are executed in their input order.
 */
public class ConditionalModels<DATA_TYPE, STATE_TYPE, KEY_TYPE> implements Models<DATA_TYPE> {

    private static final io.deephaven.io.logger.Logger log =
        ProcessEnvironment.getDefaultLog(ConditionalModels.class);
    private static final boolean LOG_PERF = Configuration.getInstance()
        .getBooleanWithDefault("ModelFarm.logConditionalModelsPerformance", false);

    private final Map<KEY_TYPE, STATE_TYPE> stateMap;
    private final Model<DATA_TYPE>[] models;
    private final BiPredicate<DATA_TYPE, STATE_TYPE>[] predicates;
    private final Function<DATA_TYPE, KEY_TYPE> dataToKey;

    private final Map<KEY_TYPE, Object> lockMap = new ConcurrentHashMap<>();

    /**
     * Creates a new set of conditionally active models.
     * <p>
     * Active models are executed in their input order.
     *
     * @param models models to execute.
     * @param predicates predicates used to determine if each model should execute.
     * @param stateMap map of state by underlying id. THIS MAP SHOULD BE THREAD SAFE, OR RESULTS
     *        WILL BE UNPREDICTABLE!
     * @param dataToKey function to get a key from data.
     */
    public ConditionalModels(final Model<DATA_TYPE>[] models,
        final BiPredicate<DATA_TYPE, STATE_TYPE>[] predicates,
        final Map<KEY_TYPE, STATE_TYPE> stateMap, final Function<DATA_TYPE, KEY_TYPE> dataToKey) {
        Require.neqNull(stateMap, "stateMap");
        Require.neqNull(models, "models");
        Require.elementsNeqNull(models, "models");
        Require.neqNull(predicates, "predicates");
        Require.elementsNeqNull(predicates, "predicates");
        Require.neqNull(dataToKey, "dataToKey");

        this.stateMap = stateMap;
        this.models = models;
        this.predicates = predicates;
        this.dataToKey = dataToKey;
    }

    @Override
    public Object getLock(final DATA_TYPE data) {
        // Each underlying has a unique lock.
        // This lock ensures that a single underlying cannot execute on multiple threads at the same
        // time.
        return lockMap.computeIfAbsent(dataToKey.apply(data), key -> new Object());
    }

    @Override
    public Iterator<Model<DATA_TYPE>> iterator(final DATA_TYPE data) {
        final long t0 = System.nanoTime();
        final KEY_TYPE key = dataToKey.apply(data);
        final STATE_TYPE validFit = stateMap.get(key);
        final long t1 = System.nanoTime();
        final ArrayList<Model<DATA_TYPE>> toRun = new ArrayList<>(models.length);

        final boolean[] pvals = new boolean[models.length];
        final long[] ptimes = new long[models.length];

        for (int i = 0; i < models.length; i++) {
            final long t0p = System.nanoTime();
            final boolean pval;

            try {
                pval = predicates[i].test(data, validFit);
            } catch (Exception e) {
                throw new RuntimeException("Exception evaluating predicate. model=" + i, e);
            }

            final long t1p = System.nanoTime();
            pvals[i] = pval;
            ptimes[i] = (t1p - t0p) / 1000;

            if (pval) {
                toRun.add(models[i]);
            }
        }

        final long t2 = System.nanoTime();

        if (LOG_PERF) {
            log.warn().append("ConditionalModels.iterator PERFORMANCE: key=").append(key.toString())
                .append("tall=").append((t2 - t1) / 1000)
                .append(" tstate=").append((t1 - t0) / 1000)
                .append(" predicatevals=").append(Arrays.toString(pvals))
                .append(" predicatetimes=").append(Arrays.toString(ptimes))
                .endl();
        }

        return toRun.iterator();
    }
}
