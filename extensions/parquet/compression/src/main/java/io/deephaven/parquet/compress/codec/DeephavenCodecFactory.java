package io.deephaven.parquet.compress.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class DeephavenCodecFactory {
    private static Configuration configurationWithCodecClasses(List<Class<?>> codecClasses) {
        Configuration conf = new Configuration();
        //noinspection unchecked
        CompressionCodecFactory.setCodecClasses(conf, (List) codecClasses);
        return conf;
    }

    private final Configuration configuration;

    public DeephavenCodecFactory(List<Class<?>> codecClasses) {
        this(configurationWithCodecClasses(codecClasses));
    }

    public DeephavenCodecFactory(Configuration configuration) {
        this.configuration = configuration;
    }
}
