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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A specific JSON processor implementation using Jackson. This provides more control over the default
 * {@link ValueOptions#processor(Class)} and {@link ValueOptions#named(Class)}.
 */
public interface JacksonProcessors extends NamedObjectProcessor.Provider, ObjectProcessor.Provider {

    /**
     * Creates a jackson provider using the provided {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson factory
     * @return the jackson provider
     */
    static JacksonProcessors of(ValueOptions options, JsonFactory factory) {
        return Mixin.of(options, factory);
    }


    Stream<? extends Type<?>> outputTypes();

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
     * @see #urlProcessor()
     * @see #byteBufferProcessor()
     */
    @Override
    <T> ObjectProcessor<? super T> processor(Class<T> inputType);

    /**
     * Creates a named object processor based on the {@code inputType} with a default {@link JsonFactory} and default
     * naming function. Equivalent to
     * {@code NamedObjectProcessor.of(processor(inputType), names(JacksonProcessor::toColumnName))}.
     *
     * @param inputType the input type
     * @return the named object processor
     * @param <T> the input type
     * @see NamedObjectProcessor#of(ObjectProcessor, Iterable)
     * @see #processor(Class)
     * @see #names(Function)
     * @see Mixin#toColumnName(List)
     */
    @Override
    <T> NamedObjectProcessor<? super T> named(Class<T> inputType);

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
