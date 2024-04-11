//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A specific JSON processor implementation using Jackson.
 */
public interface JacksonProvider extends NamedObjectProcessor.Provider {

    /**
     * Creates a jackson provider using a default factory. Equivalent to
     * {@code of(options, JacksonConfiguration.defaultFactoryBuilder().build())}.
     *
     * @param options the object options
     * @return the jackson provider
     * @see #of(ValueOptions, JsonFactory)
     * @see JacksonConfiguration#defaultFactoryBuilder()
     */
    static JacksonProvider of(ValueOptions options) {
        return of(options, JacksonConfiguration.defaultFactory());
    }

    /**
     * Creates a jackson provider using the provided {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson factory
     * @return the jackson provider
     */
    static JacksonProvider of(ValueOptions options, JsonFactory factory) {
        return Mixin.of(options, factory);
    }

    /**
     * The supported types. Includes {@link String}, {@code byte[]}, {@code char[]}, {@link File}, {@link Path},
     * {@link URL}, and {@link ByteBuffer}.
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
                Type.ofCustom(ByteBuffer.class));
    }

    /**
     * The supported types. Equivalent to {@link #getInputTypes()}.
     *
     * @return the supported types
     */
    @Override
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
}
