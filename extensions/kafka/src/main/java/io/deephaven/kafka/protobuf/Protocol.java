package io.deephaven.kafka.protobuf;

/**
 * The serialization / deserialization protocol.
 *
 * @see #serdes()
 * @see #raw()
 */
public interface Protocol {

    /**
     * The Kafka Protobuf serdes protocol. The payload's first byte is the serdes magic byte, the next 4-bytes are the
     * schema ID, the next variable-sized bytes are the message indexes, followed by the normal binary encoding of the
     * Protobuf data.
     *
     * @return the Kafka Protobuf serdes protocol
     * @see <a href=
     *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html">Kafka
     *      Protobuf serdes</a>
     * @see <a href=
     *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format">wire-format</a>
     * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protobuf encoding</a>
     */
    static Protocol serdes() {
        return Impl.SERDES;
    }

    /**
     * The raw Protobuf protocol. The full payload is the normal binary encoding of the Protobuf data.
     *
     * @return the raw Protobuf protocol
     * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protobuf encoding</a>
     */
    static Protocol raw() {
        return Impl.RAW;
    }

    enum Impl implements Protocol {
        RAW, SERDES;
    }
}
