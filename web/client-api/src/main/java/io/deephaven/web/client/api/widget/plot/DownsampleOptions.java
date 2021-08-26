package io.deephaven.web.client.api.widget.plot;

import jsinterop.annotations.JsType;

@JsType(namespace = "dh.plot")
public final class DownsampleOptions {
    /**
     * Max number of items in the series before DEFAULT will not attempt to load the series without downsampling. Above
     * this size if downsample fails or is not applicable, the series won't be loaded unless DISABLE is passed to
     * series.subscribe().
     */
    public static int MAX_SERIES_SIZE = 30_000;

    /**
     * Max number of items in the series where the subscription will be allowed at all. Above this limit, even with
     * downsampling disabled, the series will not load data.
     */
    public static int MAX_SUBSCRIPTION_SIZE = 200_000;
    /**
     * Flag to let the API decide what data will be available, based on the nature of the data, the series, and how the
     * axes are configured.
     */
    public static final DownsampleOptions DEFAULT = new DownsampleOptions();

    /**
     * Flat to entirely disable downsampling, and force all data to load, no matter how many items that would be, up to
     * the limit of MAX_SUBSCRIPTION_SIZE.
     */
    public static final DownsampleOptions DISABLE = new DownsampleOptions();

    private DownsampleOptions() {}
}
