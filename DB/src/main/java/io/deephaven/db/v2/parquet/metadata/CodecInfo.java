package io.deephaven.db.v2.parquet.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Representation class for codec information stored in key-value metadata for Deephaven-written Parquet files.
 */
@Value.Immutable
@BuildableStyle
@JsonSerialize(as = ImmutableCodecInfo.class)
@JsonDeserialize(as = ImmutableCodecInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class CodecInfo {

    /**
     * @return Codec name, currently always a Java class name
     */
    public abstract String codecName();

    /**
     * @return Implementation-defined argument string
     */
    public abstract Optional<String> codecArg();

    /**
     * @return Data type produced by the codec, currently always a Java class name
     */
    public abstract String dataType();

    /**
     * @return Component type for array and vector data types, currently always a Java class name
     */
    public abstract Optional<String> componentType();

    @Value.Check
    final void checkCodecName() {
        if (codecName().isEmpty()) {
            throw new IllegalArgumentException("Empty codec name");
        }
    }

    @Value.Check
    final void checkDataType() {
        if (dataType().isEmpty()) {
            throw new IllegalArgumentException("Empty data type");
        }
    }

    @Value.Check
    final void checkComponentType() {
        if (componentType().filter(String::isEmpty).isPresent()) {
            throw new IllegalArgumentException("Empty component type");
        }
    }

    public static CodecInfo.Builder builder() {
        return ImmutableCodecInfo.builder();
    }

    public interface Builder {

        Builder codecName(String codecName);

        Builder codecArg(String codecArg);

        Builder dataType(String dataType);

        Builder componentType(String componentType);

        CodecInfo build();
    }
}
