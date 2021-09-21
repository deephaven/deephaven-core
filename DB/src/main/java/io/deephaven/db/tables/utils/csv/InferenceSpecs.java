package io.deephaven.db.tables.utils.csv;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Inference specifications contains the configuration and logic for inferring an acceptable parser from string values.
 *
 * @see #infer(Iterator)
 */
@Immutable
@BuildableStyle
public abstract class InferenceSpecs {

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
     * <p>
     * Contains the following parsers:
     *
     * <ul>
     * <li>{@link Parser#STRING}</li>
     * <li>{@link Parser#INSTANT}</li>
     * <li>{@link Parser#SHORT}</li>
     * <li>{@link Parser#INT}</li>
     * <li>{@link Parser#LONG}</li>
     * <li>{@link Parser#DOUBLE}</li>
     * <li>{@link Parser#BOOL}</li>
     * <li>{@link Parser#CHAR}</li>
     * <li>{@link Parser#BYTE}</li>
     * <li>{@link Parser#FLOAT}</li>
     * </ul>
     *
     * Uses the default {@link #onNullParser()}.
     *
     * <p>
     * Note: the non-string parsers are only relevant when the appropriate {@link #limitToType(Type)} is invoked.
     *
     * @return the string-only inference
     */
    public static InferenceSpecs strings() {
        return builder().addParsers(
                Parser.STRING,
                Parser.INSTANT,
                Parser.SHORT,
                Parser.INT,
                Parser.LONG,
                Parser.DOUBLE,
                Parser.BOOL,
                Parser.CHAR,
                Parser.BYTE,
                Parser.FLOAT)
                .build();
    }

    /**
     * The "minimal" inference.
     *
     * <p>
     * Contains the following parsers:
     *
     * <ul>
     * <li>{@link Parser#INSTANT}</li>
     * <li>{@link Parser#LONG}</li>
     * <li>{@link Parser#DOUBLE}</li>
     * <li>{@link Parser#BOOL}</li>
     * <li>{@link Parser#STRING}</li>
     * <li>{@link Parser#BYTE}</li>
     * <li>{@link Parser#SHORT}</li>
     * <li>{@link Parser#INT}</li>
     * <li>{@link Parser#FLOAT}</li>
     * <li>{@link Parser#CHAR}</li>
     * </ul>
     *
     * Uses the default {@link #onNullParser()}.
     *
     * <p>
     * Note: the byte, short, int, float, and char parsers are only relevant when the appropriate
     * {@link #limitToType(Type)} is invoked.
     *
     * @return the minimal inference
     */
    public static InferenceSpecs minimal() {
        return builder().addParsers(
                Parser.INSTANT,
                Parser.LONG,
                Parser.DOUBLE,
                Parser.BOOL,
                Parser.STRING,
                Parser.BYTE,
                Parser.SHORT,
                Parser.INT,
                Parser.FLOAT,
                Parser.CHAR)
                .build();
    }

    /**
     * The "standard" inference, does not parse floats or bytes.
     *
     * <p>
     * Contains the following parsers:
     *
     * <ul>
     * <li>{@link Parser#INSTANT}</li>
     * <li>{@link Parser#SHORT}</li>
     * <li>{@link Parser#INT}</li>
     * <li>{@link Parser#LONG}</li>
     * <li>{@link Parser#DOUBLE}</li>
     * <li>{@link Parser#BOOL}</li>
     * <li>{@link Parser#CHAR}</li>
     * <li>{@link Parser#STRING}</li>
     * <li>{@link Parser#BYTE}</li>
     * <li>{@link Parser#FLOAT}</li>
     * </ul>
     *
     * Uses the default {@link #onNullParser()}.
     *
     * <p>
     * Note: the byte and float parsers are only relevant when the appropriate {@link #limitToType(Type)} is invoked.
     *
     * @return the standard inference
     */
    public static InferenceSpecs standard() {
        return builder().addParsers(
                Parser.INSTANT,
                Parser.SHORT,
                Parser.INT,
                Parser.LONG,
                Parser.DOUBLE,
                Parser.BOOL,
                Parser.CHAR,
                Parser.STRING,
                Parser.BYTE,
                Parser.FLOAT)
                .build();
    }

    /**
     * The standard parsers with additional {@link java.time.Instant}-based parsing.
     *
     * <p>
     * Contains the following parsers:
     *
     * <ul>
     * <li>{@link Parser#INSTANT}</li>
     * <li>{@link Parser#INSTANT_DB}</li>
     * <li>{@link Parser#epochMilliAndMicroParsers21stCentury(Parser)}, with {@link Parser#LONG}</li>
     * <li>{@link Parser#SHORT}</li>
     * <li>{@link Parser#INT}</li>
     * <li>{@link Parser#LONG}</li>
     * <li>{@link Parser#DOUBLE}</li>
     * <li>{@link Parser#BOOL}</li>
     * <li>{@link Parser#CHAR}</li>
     * <li>{@link Parser#STRING}</li>
     * <li>{@link Parser#BYTE}</li>
     * <li>{@link Parser#FLOAT}</li>
     * </ul>
     *
     * Uses the default {@link #onNullParser()}.
     *
     * <p>
     * Note: the byte and float parsers are only relevant when the appropriate {@link #limitToType(Type)} is invoked.
     *
     * @return the standard times inference
     */
    public static InferenceSpecs standardTimes() {
        final List<Parser<Instant>> parsers = Parser.epochMilliAndMicroParsers21stCentury(Parser.LONG);
        return builder().addParsers(
                Parser.INSTANT,
                Parser.INSTANT_DB,
                parsers.get(0),
                parsers.get(1),
                Parser.SHORT,
                Parser.INT,
                Parser.LONG,
                Parser.DOUBLE,
                Parser.BOOL,
                Parser.CHAR,
                Parser.STRING,
                Parser.BYTE,
                Parser.FLOAT)
                .build();
    }

    /**
     * The parsers, in preference-based order.
     *
     * @return the parsers
     */
    public abstract List<Parser<?>> parsers();

    /**
     * The parser to return when all values are null. May be {@code null}.
     *
     * <p>
     * By default, returns a {@link Parser#STRING}.
     *
     * @return the on-null values parser
     */
    @Default
    @Nullable
    public Parser<?> onNullParser() {
        return Parser.STRING;
    }

    /**
     * Filters out all parsers that do not have {@code type}.
     *
     * <p>
     * {@link #onNullParser()} will be set to the first parser that matches {@code type}.
     *
     * @param type the type to limit to
     * @return the new inference based on type
     */
    public InferenceSpecs limitToType(Type<?> type) {
        Parser<?> first = null;
        final Builder builder = builder();
        for (Parser<?> parser : parsers()) {
            if (type.equals(parser.type())) {
                builder.addParsers(parser);
                if (first == null) {
                    first = parser;
                }
            }
        }
        return builder.onNullParser(first).build();
    }

    /**
     * Finds the best parser by checking and eliminating parsers based on {@link Parser#isParsable(String)}. The
     * returned parser will be the lowest indexed parser remaining based on the order specified in {@link #parsers()}.
     *
     * <p>
     * When all {@code values} are null, the returned value will be an optional that wraps {@link #onNullParser()}.
     * 
     * @param values the values to be inferred
     * @return the best parser, if any
     */
    public Optional<Parser<?>> infer(Iterator<String> values) {
        boolean allNull = true;
        final List<Parser<?>> candidates = new ArrayList<>(parsers());
        while (values.hasNext() && !candidates.isEmpty()) {
            final String item = values.next();
            if (item != null) {
                allNull = false;
                candidates.removeIf(parser -> !parser.isParsable(item));
            }
        }
        if (allNull) {
            return Optional.ofNullable(onNullParser());
        }
        return candidates.stream().findFirst();
    }

    @Check
    final void checkNonEmpty() {
        if (parsers().isEmpty()) {
            throw new IllegalArgumentException("Must provide at least one parser for inference");
        }
    }

    public interface Builder {

        Builder onNullParser(Parser<?> parser);

        Builder addParsers(Parser<?> item);

        Builder addParsers(Parser<?>... items);

        Builder addAllParsers(Iterable<? extends Parser<?>> items);

        InferenceSpecs build();
    }
}
