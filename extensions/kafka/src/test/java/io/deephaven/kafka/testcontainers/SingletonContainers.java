//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.testcontainers;

import org.apache.kafka.clients.CommonClientConfigs;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

final class SingletonContainers {

    /**
     * We are currently bifurcating the different containers based on the top-level kafka servers that are available. If
     * we happen to find we want to test against multiple versions of a given server, we can either do that manually on
     * an ad-hoc basis (by setting the appropriate system property, or by temporarily updating the docker/registry/), or
     * more permanently by adding variants with explicit versions, ie, KAFKA_3_9, KAFKA_4_0, KAFKA_4_1, etc.
     */
    private enum Name {
        // @formatter:off
        CONFLUENT_KAFKA("cp-kafka", "confluentinc/cp-kafka"),
        KAFKA("kafka", "apache/kafka"),
        KAFKA_NATIVE("kafka-native", "apache/kafka-native"),
        REDPANDA("redpanda", "redpandadata/redpanda");
        // @formatter:on

        private final DockerImageName imageName;

        Name(String deephavenName, String imageName) {
            final String value = System.getProperty(String.format("testcontainers.%s.image", deephavenName));
            this.imageName = DockerImageName.parse(value).asCompatibleSubstituteFor(imageName);
        }
    }

    private static void startAndAddShutdownHook(final GenericContainer<?> container) {
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
    }

    // This pattern allows the respective images to be spun up as a container once per-JVM as opposed to once per-class
    // or once per-test.
    // https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    // https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers


    public static final class ConfluentKafka {
        private static final ConfluentKafkaContainer CONTAINER =
                new ConfluentKafkaContainer(Name.CONFLUENT_KAFKA.imageName);

        static {
            startAndAddShutdownHook(CONTAINER);
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static String bootstrapServers() {
            return CONTAINER.getBootstrapServers();
        }

        public static Map<String, Object> propertiesMap() {
            return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        }

        private ConfluentKafka() {}
    }

    public static final class Kafka {
        private static final KafkaContainer CONTAINER = new KafkaContainer(Name.KAFKA.imageName);

        static {
            startAndAddShutdownHook(CONTAINER);
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static String bootstrapServers() {
            return CONTAINER.getBootstrapServers();
        }

        public static Map<String, Object> propertiesMap() {
            return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        }

        private Kafka() {}
    }

    public static final class KafkaNative {
        private static final KafkaContainer CONTAINER = new KafkaContainer(Name.KAFKA_NATIVE.imageName);

        static {
            startAndAddShutdownHook(CONTAINER);
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static String bootstrapServers() {
            return CONTAINER.getBootstrapServers();
        }

        public static Map<String, Object> propertiesMap() {
            return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        }

        private KafkaNative() {}
    }

    public static final class Redpanda {
        private static final RedpandaContainer CONTAINER = new RedpandaContainer(Name.REDPANDA.imageName);

        static {
            startAndAddShutdownHook(CONTAINER);
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static String bootstrapServers() {
            return CONTAINER.getBootstrapServers();
        }

        public static Map<String, Object> propertiesMap() {
            return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        }

        private Redpanda() {}
    }

    private SingletonContainers() {}
}
