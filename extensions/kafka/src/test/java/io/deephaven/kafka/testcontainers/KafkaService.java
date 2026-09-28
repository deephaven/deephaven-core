//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.testcontainers;

import io.deephaven.kafka.testcontainers.SingletonContainers.ConfluentKafka;
import io.deephaven.kafka.testcontainers.SingletonContainers.Kafka;
import io.deephaven.kafka.testcontainers.SingletonContainers.KafkaNative;
import io.deephaven.kafka.testcontainers.SingletonContainers.Redpanda;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public enum KafkaService {
    CONFLUENT_KAFKA(SingletonContainers.Name.CONFLUENT_KAFKA) {
        @Override
        public void init() {
            ConfluentKafka.init();
        }

        @Override
        public Map<String, Object> propertiesMap() {
            return ConfluentKafka.propertiesMap();
        }
    },
    KAFKA(SingletonContainers.Name.KAFKA) {
        @Override
        public void init() {
            Kafka.init();
        }

        @Override
        public Map<String, Object> propertiesMap() {
            return Kafka.propertiesMap();
        }
    },
    KAFKA_NATIVE(SingletonContainers.Name.KAFKA_NATIVE) {
        @Override
        public void init() {
            KafkaNative.init();
        }

        @Override
        public Map<String, Object> propertiesMap() {
            return KafkaNative.propertiesMap();
        }
    },
    REDPANDA(SingletonContainers.Name.REDPANDA) {
        @Override
        public void init() {
            Redpanda.init();
        }

        @Override
        public Map<String, Object> propertiesMap() {
            return Redpanda.propertiesMap();
        }
    };

    private final SingletonContainers.Name name;

    KafkaService(SingletonContainers.Name name) {
        this.name = Objects.requireNonNull(name);
    }

    public final boolean isEnabled() {
        return name.isEnabled();
    }

    public abstract void init();

    public abstract Map<String, Object> propertiesMap();

    public final AdminClient admin() {
        return AdminClient.create(propertiesMap());
    }

    public final <K, V> KafkaProducer<K, V> producer(Serializer<K> key, Serializer<V> value) {
        return new KafkaProducer<>(propertiesMap(), key, value);
    }

    public final Properties properties() {
        final Properties properties = new Properties();
        properties.putAll(propertiesMap());
        return properties;
    }
}
