package io.deephaven.api.agg.spec;

/**
 * These are defaults whose values can change without breaking wire-API compatibility, but may break scripts.
 */
class SemanticDefaults {

    public static final boolean COUNT_NULLS_DEFAULT = false;

    public static final boolean INCLUDE_NULLS_DEFAULT = false;

    public static final boolean MEDIAN_AVERAGE_EVENLY_DIVIDED_DEFAULT = true;

    public static final boolean PERCENTILE_AVERAGE_EVENLY_DIVIDED_DEFAULT = false;

    public static final String PARAM_TOKEN_DEFAULT = "each";
}
