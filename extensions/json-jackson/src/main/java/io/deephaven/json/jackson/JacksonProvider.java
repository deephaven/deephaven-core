//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.AnyValue;
import io.deephaven.json.ArrayValue;
import io.deephaven.json.DoubleValue;
import io.deephaven.json.IntValue;
import io.deephaven.json.LongValue;
import io.deephaven.json.ObjectEntriesValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import io.deephaven.json.TupleValue;
import io.deephaven.json.TypedObjectValue;
import io.deephaven.json.Value;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.util.annotations.FinalDefault;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link Value JSON value} {@link ObjectProcessor processor} implementation using
 * <a href="https://github.com/FasterXML/jackson">Jackson</a>>.
 *
 * <p>
 * This implementation allows users to efficiently parse / destructure a
 * <a href="https://www.json.org/json-en.html">JSON value</a> (from a supported {@link JacksonProvider#getInputTypes()
 * input type}) into {@link WritableChunk writable chunks} according to the type(s) as specified by its {@link Value
 * Value type}. This is done using the <a href="https://github.com/FasterXML/jackson-core">Jackson <b>streaming
 * API</b></a> {@link JsonParser} (as opposed to the databind / object mapping API, which must first create intermediate
 * objects).
 *
 * <p>
 * The "simple" types are self-explanatory. For example, the {@link StringValue} represents a {@link String} output
 * type, and (by default) expects a JSON string as input; the {@link IntValue} represents an {@code int} output type,
 * and (by default) expects a JSON number as input. The allowed JSON input types can be specified via
 * {@link Value#allowedTypes() allowed types}; users are encouraged to use the strictest type they can according to how
 * their JSON data is serialized.
 *
 * <p>
 * The most common "complex" type is {@link ObjectValue}, which expects to parse a JSON object of known fields. The
 * object contains {@link ObjectValue#fields()}, which represent other {@link Value values}. The fields are recursively
 * resolved and flattened into the {@link ObjectProcessor#outputTypes()}. For example, a JSON object, which itself
 * contains another JSON object
 *
 * <pre>
 * {
 *   "city": "Plymouth",
 *   "point": {
 *       "latitude": 45.018269,
 *       "longitude": -93.473892
 *   }
 * }
 * </pre>
 *
 * when represented with structuring as one might expect ({@link ObjectValue}({@link StringValue},
 * {@link ObjectValue}({@link DoubleValue}, {@link DoubleValue}))), will produce {@link ObjectProcessor#outputTypes()
 * output types} representing {@code [String, double, double]}. Furthermore, the field names and delimiter "_" will be
 * used by default to provide the {@link NamedObjectProcessor#names() names}
 * {@code ["city", "point_latitude", "point_longitude"]}.
 *
 * <p>
 * The {@link ArrayValue} represents a variable-length array, which expects to parse a JSON array where each element is
 * expected to have the same {@link ArrayValue#element() element type}. (This is in contrast to JSON arrays more
 * generally, where each element of the array can be a different JSON value type.) The output type will be the output
 * type(s) of the element type as the component type of a native array, {@link Type#arrayType()}. For example, if we
 * used the previous example as the {@link ArrayValue#element() array component type}, it will produce
 * {@link ObjectProcessor#outputTypes() output types} representing {@code [String[], double[], double[]]} (the
 * {@link NamedObjectProcessor#names() names} will remain unchanged).
 *
 * <p>
 * The {@link TupleValue} represents a fixed number of {@link TupleValue#namedValues() value types}, which expects to
 * parse a fixed-length JSON array where each element corresponds to the same-indexed value type. The values are
 * recursively resolved and flattened into the {@link ObjectProcessor#outputTypes()}; for example, the earlier example's
 * data could be re-represented as the JSON array
 *
 * <pre>
 * ["Plymouth", 45.018269, -93.473892]
 * </pre>
 * 
 * and structured as one might expect ({@link TupleValue}({@link StringValue}, {@link DoubleValue},
 * {@link DoubleValue})), and will produce {@link ObjectProcessor#outputTypes() output types} representing
 * {@code [String, double, double]}. Even though no field names are present in the JSON value, users may set
 * {@link TupleValue#namedValues() names} for each element (and will otherwise inherit integer-indexed default names).
 *
 * <p>
 * The {@link TypedObjectValue} represents a union of {@link ObjectValue object values} where the first field is
 * type-discriminating. For example, the following might be modelled as a type-discriminated object with
 * type-discriminating field "type", shared "symbol" {@link StringValue}, "quote" object of "bid" {@link DoubleValue}
 * and an "ask" {@link DoubleValue}, and "trade" object containing a "price" {@link DoubleValue} and a "size"
 * {@link LongValue}.
 *
 * <pre>
 * {
 *   "type": "quote",
 *   "symbol": "BAR",
 *   "bid": 10.01,
 *   "ask": 10.05
 * }
 * {
 *   "type": "trade",
 *   "symbol": "FOO",
 *   "price": 70.03,
 *   "size": 42
 * }
 * </pre>
 *
 * The {@link ObjectProcessor#outputTypes() output types} are first the type-discriminating field, then the shared
 * fields (if any), followed by the individual {@link ObjectValue object value} fields; with the above example, that
 * would result in {@link ObjectProcessor#outputTypes() output types}
 * {@code [String, String, double, double, double long]} and {@link NamedObjectProcessor#names() names}
 * {@code ["type", "symbol", "quote_bid", "quote_ask", "trade_price", "trade_size"]}.
 *
 * <p>
 * The {@link ObjectEntriesValue} represents a variable-length object, which expects to parse a JSON object where each
 * key-value entry has a common {@link ObjectEntriesValue#value() value type}. The output type will be the key and value
 * element types as a component of native arrays ({@link Type#arrayType()}). For example, a JSON object, whose values
 * are also JSON objects
 *
 * <pre>
 * {
 *   "Plymouth": {
 *       "latitude": 45.018269,
 *       "longitude": -93.473892
 *   },
 *   "New York": {
 *       "latitude": 40.730610,
 *       "longitude": -73.935242
 *   }
 * }
 * </pre>
 *
 * when represented with structuring as one might expect ({@link ObjectEntriesValue}({@link StringValue},
 * {@link ObjectValue}({@link DoubleValue}, {@link DoubleValue}))), will produce {@link ObjectProcessor#outputTypes()
 * output types} representing {@code [String[], double[], double[]]}, and {@link NamedObjectProcessor#names() names}
 * {@code ["Key", "latitude", "longitude"]}.
 *
 * <p>
 * The {@link AnyValue} type represents a {@link TreeNode} output; this requires that the Jackson databinding API be
 * available on the classpath. This is useful for initial modelling and debugging purposes.
 */
public interface JacksonProvider extends NamedObjectProcessor.Provider {

    /**
     * Creates a jackson provider using a default factory. Equivalent to
     * {@code of(options, JacksonConfiguration.defaultFactoryBuilder().build())}.
     *
     * @param options the object options
     * @return the jackson provider
     * @see #of(Value, JsonFactory)
     * @see JacksonConfiguration#defaultFactoryBuilder()
     */
    static JacksonProvider of(Value options) {
        return of(options, JacksonConfiguration.defaultFactory());
    }

    /**
     * Creates a jackson provider using the provided {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson factory
     * @return the jackson provider
     */
    static JacksonProvider of(Value options, JsonFactory factory) {
        return Mixin.of(options, factory);
    }

    /**
     * The supported types. Includes {@link String}, {@code byte[]}, {@code char[]}, {@link File}, {@link Path},
     * {@link URL}, {@link ByteBuffer}, and {@link CharBuffer}.
     *
     * @return the supported types
     */
    static Set<Type<?>> getInputTypes() {
        return Set.of(
                Type.stringType(),
                Type.byteType().arrayType(),
                Type.charType().arrayType(),
                Type.ofCustom(File.class),
                Type.ofCustom(Path.class),
                Type.ofCustom(URL.class),
                Type.ofCustom(ByteBuffer.class),
                Type.ofCustom(CharBuffer.class));
    }

    /**
     * The supported types. Equivalent to {@link #getInputTypes()}.
     *
     * @return the supported types
     */
    @Override
    @FinalDefault
    default Set<Type<?>> inputTypes() {
        return getInputTypes();
    }

    /**
     * Creates an object processor based on the {@code inputType} with a default {@link JsonFactory}.
     *
     * @param inputType the input type
     * @return the object processor
     * @param <T> the input type
     * @see #stringProcessor()
     * @see #bytesProcessor()
     * @see #charsProcessor()
     * @see #fileProcessor()
     * @see #pathProcessor()
     * @see #urlProcessor()
     * @see #byteBufferProcessor()
     * @see #charBufferProcessor()
     */
    @Override
    <T> ObjectProcessor<? super T> processor(Type<T> inputType);

    List<String> names(Function<List<String>, String> f);

    /**
     * Creates a {@link String} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(String)
     */
    ObjectProcessor<String> stringProcessor();

    /**
     * Creates a {@code byte[]} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(byte[])
     */
    ObjectProcessor<byte[]> bytesProcessor();

    /**
     * Creates a {@code char[]} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(char[])
     */
    ObjectProcessor<char[]> charsProcessor();

    /**
     * Creates a {@link File} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(File)
     */
    ObjectProcessor<File> fileProcessor();

    /**
     * Creates a {@link Path} json object processor.
     *
     * @return the object processor
     */
    ObjectProcessor<Path> pathProcessor();

    /**
     * Creates a {@link URL} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(URL)
     */
    ObjectProcessor<URL> urlProcessor();

    /**
     * Creates a {@link ByteBuffer} json object processor.
     *
     * @return the object processor
     */
    ObjectProcessor<ByteBuffer> byteBufferProcessor();

    /**
     * Creates a {@link CharBuffer} json object processor.
     *
     * @return the object processor
     */
    ObjectProcessor<CharBuffer> charBufferProcessor();
}
