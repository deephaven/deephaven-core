# Parquet compression support

This project abstracts over parquet's expected compression formats in a way to make it easy for the Deephaven
parquet reader/writer code to be able to use them. 

There are two types in parquet that ostensibly offer compression codecs,
* `org.apache.parquet.compression.CompressionCodecFactory`, which depends on
* `org.apache.hadoop.io.compress.CompressionCodecFactory`

With no other information, it would seem like the parquet version gets its base functionality from the more general
hadoop type, and while it is true that both factories provide access to `org.apache.hadoop.io.compress.CompressionCodec`
instances, the parquet implementation disregards the hadoop implementation's ability to select codecs from either
configuration or from the classpath (via service loader), and instead relies on hardcoded fully-qualified classnames
found in `org.apache.parquet.hadoop.metadata.CompressionCodecName`. That is why we use the hadoop implementation.

Most of these codecs are present in hadoop-common or parquet-hadoop, and are self-contained and entirely
implemented in bytecode. One counter-example would be the LZ4 codec,  provided by `org.apache.hadoop.io.compress.Lz4Codec`,
which requires an external dependency that tries to load native code (but can fall back to bytecode). 

Two implementations aren't provided at all in hadoop-common or parquet-hadoop:
* `org.apache.hadoop.io.compress.BrotliCodec` - No implementation is available of this in Maven Central, though other
repositories have an implementation. For our testing, we use `jbrotli-native-darwin-x86-amd64` that is limited to only 
native implementations for x86 platforms.
* `com.hadoop.compression.lzo.LzoCodec` - There are GPL implementations of the LZO codec available, either bytecode or 
native, but this license isn't compatible with many other projects, so we disregard it. Instead, we use 
`io.airlift.compress.lzo.LzoCodec`, that is shared under APACHE-2.0 license.

We also ignore the provided codec for snappy, `org.apache.hadoop.io.compress.SnappyCodec`, since it's not compatible with 
other parquet implementations which claim to use Snappy. Instead, the configuration is modified to replace this 
ServiceLoader-provided implementation with `org.apache.parquet.hadoop.codec.SnappyCodec`, which is the classname
hardcoded in `org.apache.parquet.compression.CompressionCodecFactory`.

Note that `org.apache.parquet.hadoop.metadata.ColumnChunkMetaData` instances created by Deephaven table writing code
do still require `CompressionCodecName` instances, which means that we must still have a way to translate our own codecs
into this enum's values, and only officially supported compression codecs can ever be used to write.

So, this project offers codecs from `org.apache.hadoop.io.compress.CompressionCodecFactory`, with configuration options,
and utilities to map back to official codec names.
