package io.deephaven.kafka.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A descriptor provider from a {@link #clazz() class} on the classpath.
 *
 * @param <T> the message type
 */
@Immutable
@SimpleStyle
public abstract class DescriptorMessageClass<T extends Message> implements DescriptorProvider {
    public static <T extends Message> DescriptorMessageClass<T> of(Class<T> clazz) {
        return ImmutableDescriptorMessageClass.of(clazz);
    }

    /**
     * The message class.
     *
     * @return the message class
     */
    @Parameter
    public abstract Class<T> clazz();
}
