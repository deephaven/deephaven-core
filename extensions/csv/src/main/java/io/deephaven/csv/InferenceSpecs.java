package io.deephaven.csv;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Inference specifications contains the configuration and logic for inferring an acceptable parser from string values.
 *
 * @see #infer(Iterator)
 */
@Immutable
@BuildableStyle
public abstract class InferenceSpecs {
    public static final List<Parser<?>> STRINGS_PARSERS = Parsers.STRINGS;

    public static final List<Parser<?>> MINIMAL_PARSERS = Parsers.MINIMAL;

    public static final List<Parser<?>> STANDARD_PARSERS = Parsers.DEFAULT;

    public static final List<Parser<?>> COMPLETE_PARSERS = Parsers.COMPLETE;

    public static final List<Parser<?>> STANDARD_TIMES_PARSERS = Parsers.STANDARD_TIMES;

    public static final List<Parser<?>> STANDARD_MILLITIMES_PARSERS = Parsers.STANDARD_MILLITIMES;

    public static final List<Parser<?>> STANDARD_MICROTIMES_PARSERS = Parsers.STANDARD_MICROTIMES;

    public static final List<Parser<?>> STANDARD_NANOTIMES_PARSERS = Parsers.STANDARD_NANOTIMES;

    /**
     * Creates a builder for {@link InferenceSpecs}.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableInferenceSpecs.builder();
    }

    /**
     * The string-only inference.
     *
     * @return the string-only inference
     */
    public static InferenceSpecs strings() {
        return builder().addAllParsers(STRINGS_PARSERS).build();
    }

    /**
     * The "minimal" inference.
     */
    public static InferenceSpecs minimal() {
        return builder().addAllParsers(MINIMAL_PARSERS).build();
    }

    /**
     * The "standard" inference, does not parse bytes, shorts, or floats.
     */
    public static InferenceSpecs standard() {
        return builder().addAllParsers(STANDARD_PARSERS).build();
    }

    /**
     * The "complete" inference.
     */
    public static InferenceSpecs complete() {
        return builder().addAllParsers(COMPLETE_PARSERS).build();
    }

    /**
     * The standard parsers with additional {@link java.time.Instant}-based parsing.
     *
     * @return the standard times inference
     */
    public static InferenceSpecs standardTimes() {
        return builder().addAllParsers(STANDARD_TIMES_PARSERS).build();
    }

    public static InferenceSpecs milliTimes() {
        return builder().addAllParsers(STANDARD_MILLITIMES_PARSERS).build();
    }

    public static InferenceSpecs microTimes() {
        return builder().addAllParsers(STANDARD_MICROTIMES_PARSERS).build();
    }

    public static InferenceSpecs nanoTimes() {
        return builder().addAllParsers(STANDARD_NANOTIMES_PARSERS).build();
    }

    /**
     * The parsers that the user wants to participate in type inference. Note that the order that the parsers in this
     * list matters only for custom parsers. In particular:
     * <ol>
     * <li>Standard system parsers (singletons from the {@link Parsers} class) will run in their standard precedence
     * order, regardless of the order they appear here.</li>
     * <li>All specified system parsers will be run before any specified custom parsers.</li>
     * <li>Custom parsers will be run in the order they are specified here.</li>
     * </ol>
     *
     * @return the parsers
     */
    public abstract List<Parser<?>> parsers();

    /**
     * The parser to return when all values are null. May be {@code null}.
     *
     * <p>
     * By default, returns a {@link Parsers#STRING}.
     *
     * @return the on-null values parser
     */
    @Default
    @Nullable
    public Parser<?> nullParser() {
        return Parsers.STRING;
    }

    public interface Builder {

        Builder nullParser(Parser<?> parser);

        Builder addParsers(Parser<?> item);

        Builder addParsers(Parser<?>... items);

        Builder addAllParsers(Iterable<? extends Parser<?>> items);

        InferenceSpecs build();
    }
}
