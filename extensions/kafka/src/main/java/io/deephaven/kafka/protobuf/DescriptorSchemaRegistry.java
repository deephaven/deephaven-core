//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * A descriptor provider sourced from a Kafka schema registry.
 *
 * @see <a href="https://docs.confluent.io/platform/current/schema-registry/index.html">Schema Registry</a>
 */
@Immutable
@BuildableStyle
public abstract class DescriptorSchemaRegistry implements DescriptorProvider {

    public static Builder builder() {
        return ImmutableDescriptorSchemaRegistry.builder();
    }

    /**
     * The schema subject to fetch from the schema registry.
     *
     * @return the schema subject
     */
    public abstract String subject();

    /**
     * The schema version to fetch from the schema registry. When not set, the latest schema will be fetched.
     *
     * <p>
     * For purposes of reproducibility across restarts where schema changes may occur, it is advisable for callers to
     * set this. This will ensure the resulting {@link io.deephaven.engine.table.TableDefinition table definition} will
     * not change across restarts. This gives the caller an explicit opportunity to update any downstream consumers
     * before bumping schema versions.
     *
     * @return the schema version, or none for latest
     */
    public abstract OptionalInt version();

    /**
     * The fully-qualified protobuf {@link com.google.protobuf.Message} name, for example "com.example.MyMessage". This
     * message's {@link Descriptor} will be used as the basis for the resulting table's
     * {@link io.deephaven.engine.table.TableDefinition definition}. When not set, the first message descriptor in the
     * protobuf schema will be used.
     *
     * <p>
     * It is advisable for callers to explicitly set this.
     *
     * @return the schema message name
     */
    public abstract Optional<String> messageName();

    public interface Builder {
        Builder subject(String subject);

        Builder version(int version);

        Builder messageName(String messageName);

        DescriptorSchemaRegistry build();
    }
}
