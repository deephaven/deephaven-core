package io.deephaven.kafka;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link KafkaPublishOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableKafkaPublishOptions.builder()}.
 */
@Generated(from = "KafkaPublishOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableKafkaPublishOptions extends KafkaPublishOptions {
  private final Table table;
  private final @Nullable String topic;
  private final @Nullable Integer partition;
  private final Properties config;
  private final KafkaTools.Produce.KeyOrValueSpec keySpec;
  private final KafkaTools.Produce.KeyOrValueSpec valueSpec;
  private final boolean lastBy;
  private final boolean publishInitial;
  private final @Nullable ColumnName topicColumn;
  private final @Nullable ColumnName partitionColumn;
  private final @Nullable ColumnName timestampColumn;

  private ImmutableKafkaPublishOptions(ImmutableKafkaPublishOptions.Builder builder) {
    this.table = builder.table;
    this.topic = builder.topic;
    this.partition = builder.partition;
    this.config = builder.config;
    this.topicColumn = builder.topicColumn;
    this.partitionColumn = builder.partitionColumn;
    this.timestampColumn = builder.timestampColumn;
    if (builder.keySpec != null) {
      initShim.keySpec(builder.keySpec);
    }
    if (builder.valueSpec != null) {
      initShim.valueSpec(builder.valueSpec);
    }
    if (builder.lastByIsSet()) {
      initShim.lastBy(builder.lastBy);
    }
    if (builder.publishInitialIsSet()) {
      initShim.publishInitial(builder.publishInitial);
    }
    this.keySpec = initShim.keySpec();
    this.valueSpec = initShim.valueSpec();
    this.lastBy = initShim.lastBy();
    this.publishInitial = initShim.publishInitial();
    this.initShim = null;
  }

  private ImmutableKafkaPublishOptions(
      Table table,
      @Nullable String topic,
      @Nullable Integer partition,
      Properties config,
      KafkaTools.Produce.KeyOrValueSpec keySpec,
      KafkaTools.Produce.KeyOrValueSpec valueSpec,
      boolean lastBy,
      boolean publishInitial,
      @Nullable ColumnName topicColumn,
      @Nullable ColumnName partitionColumn,
      @Nullable ColumnName timestampColumn) {
    this.table = table;
    this.topic = topic;
    this.partition = partition;
    this.config = config;
    this.keySpec = keySpec;
    this.valueSpec = valueSpec;
    this.lastBy = lastBy;
    this.publishInitial = publishInitial;
    this.topicColumn = topicColumn;
    this.partitionColumn = partitionColumn;
    this.timestampColumn = timestampColumn;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "KafkaPublishOptions", generator = "Immutables")
  private final class InitShim {
    private byte keySpecBuildStage = STAGE_UNINITIALIZED;
    private KafkaTools.Produce.KeyOrValueSpec keySpec;

    KafkaTools.Produce.KeyOrValueSpec keySpec() {
      if (keySpecBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (keySpecBuildStage == STAGE_UNINITIALIZED) {
        keySpecBuildStage = STAGE_INITIALIZING;
        this.keySpec = Objects.requireNonNull(ImmutableKafkaPublishOptions.super.keySpec(), "keySpec");
        keySpecBuildStage = STAGE_INITIALIZED;
      }
      return this.keySpec;
    }

    void keySpec(KafkaTools.Produce.KeyOrValueSpec keySpec) {
      this.keySpec = keySpec;
      keySpecBuildStage = STAGE_INITIALIZED;
    }

    private byte valueSpecBuildStage = STAGE_UNINITIALIZED;
    private KafkaTools.Produce.KeyOrValueSpec valueSpec;

    KafkaTools.Produce.KeyOrValueSpec valueSpec() {
      if (valueSpecBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (valueSpecBuildStage == STAGE_UNINITIALIZED) {
        valueSpecBuildStage = STAGE_INITIALIZING;
        this.valueSpec = Objects.requireNonNull(ImmutableKafkaPublishOptions.super.valueSpec(), "valueSpec");
        valueSpecBuildStage = STAGE_INITIALIZED;
      }
      return this.valueSpec;
    }

    void valueSpec(KafkaTools.Produce.KeyOrValueSpec valueSpec) {
      this.valueSpec = valueSpec;
      valueSpecBuildStage = STAGE_INITIALIZED;
    }

    private byte lastByBuildStage = STAGE_UNINITIALIZED;
    private boolean lastBy;

    boolean lastBy() {
      if (lastByBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (lastByBuildStage == STAGE_UNINITIALIZED) {
        lastByBuildStage = STAGE_INITIALIZING;
        this.lastBy = ImmutableKafkaPublishOptions.super.lastBy();
        lastByBuildStage = STAGE_INITIALIZED;
      }
      return this.lastBy;
    }

    void lastBy(boolean lastBy) {
      this.lastBy = lastBy;
      lastByBuildStage = STAGE_INITIALIZED;
    }

    private byte publishInitialBuildStage = STAGE_UNINITIALIZED;
    private boolean publishInitial;

    boolean publishInitial() {
      if (publishInitialBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (publishInitialBuildStage == STAGE_UNINITIALIZED) {
        publishInitialBuildStage = STAGE_INITIALIZING;
        this.publishInitial = ImmutableKafkaPublishOptions.super.publishInitial();
        publishInitialBuildStage = STAGE_INITIALIZED;
      }
      return this.publishInitial;
    }

    void publishInitial(boolean publishInitial) {
      this.publishInitial = publishInitial;
      publishInitialBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (keySpecBuildStage == STAGE_INITIALIZING) attributes.add("keySpec");
      if (valueSpecBuildStage == STAGE_INITIALIZING) attributes.add("valueSpec");
      if (lastByBuildStage == STAGE_INITIALIZING) attributes.add("lastBy");
      if (publishInitialBuildStage == STAGE_INITIALIZING) attributes.add("publishInitial");
      return "Cannot build KafkaPublishOptions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The table used as a source of data to be sent to Kafka.
   * @return the table
   */
  @Override
  public Table table() {
    return table;
  }

  /**
   * The default Kafka topic to publish to. When {@code null}, {@link #topicColumn()} must be set.
   * @return the default Kafka topic
   * @see #topicColumn()
   */
  @Override
  public @Nullable String topic() {
    return topic;
  }

  /**
   * The default Kafka partition to publish to.
   * @return the default Kafka partition
   * @see #partitionColumn()
   */
  @Override
  public OptionalInt partition() {
    return partition != null
        ? OptionalInt.of(partition)
        : OptionalInt.empty();
  }

  /**
   * The Kafka configuration properties.
   * @return the Kafka configuration
   */
  @Override
  public Properties config() {
    return config;
  }

  /**
   * The conversion specification for Kafka record keys from table column data. By default, is
   * {@link Produce#ignoreSpec()}.
   * @return the key spec
   */
  @Override
  public KafkaTools.Produce.KeyOrValueSpec keySpec() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.keySpec()
        : this.keySpec;
  }

  /**
   * The conversion specification for Kafka record values from table column data. By default, is
   * {@link Produce#ignoreSpec()}.
   * @return the value spec
   */
  @Override
  public KafkaTools.Produce.KeyOrValueSpec valueSpec() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.valueSpec()
        : this.valueSpec;
  }

  /**
   * Whether to publish only the last record for each unique key. If {@code true}, the publishing logic will
   * internally perform a {@link Table#lastBy(String...) lastBy} aggregation on {@link #table()} grouped by the input
   * columns of {@link #keySpec()}. When {@code true}, the {@link #keySpec() keySpec} must not be
   * {@link Produce#ignoreSpec() ignoreSpec}. By default, is {@code false}.
   * @return if the publishing should be done with a last-by table
   */
  @Override
  public boolean lastBy() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.lastBy()
        : this.lastBy;
  }

  /**
   * If the initial data in {@link #table()} should be published. When {@code false}, {@link #table()} must be
   * {@link Table#isRefreshing() refreshing}. By default, is {@code true}.
   * 
   * @return if the initial table data should be published
   */
  @Override
  public boolean publishInitial() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.publishInitial()
        : this.publishInitial;
  }

  /**
   * The topic column. When set, uses the the given {@link CharSequence}-compatible column from {@link #table()} as
   * the first source for setting the Kafka record topic. When not present, or if the column value is null,
   * {@link #topic()} will be used.
   * @return the topic column name
   */
  @Override
  public Optional<ColumnName> topicColumn() {
    return Optional.ofNullable(topicColumn);
  }

  /**
   * The partition column. When set, uses the the given {@code int} column from {@link #table()} as the first source
   * for setting the Kafka record partition. When not present, or if the column value is null, {@link #partition()}
   * will be used if present. If a valid partition number is specified, that partition will be used when sending the
   * record. Otherwise, Kafka will choose a partition using a hash of the key if the key is present, or will assign a
   * partition in a round-robin fashion if the key is not present.
   * @return the partition column name
   */
  @Override
  public Optional<ColumnName> partitionColumn() {
    return Optional.ofNullable(partitionColumn);
  }

  /**
   * The timestamp column. When set, uses the the given {@link Instant} column from {@link #table()} as the first
   * source for setting the Kafka record timestamp. When not present, or if the column value is null, the producer
   * will stamp the record with its current time. The timestamp eventually used by Kafka depends on the timestamp type
   * configured for the topic. If the topic is configured to use CreateTime, the timestamp in the producer record will
   * be used by the broker. If the topic is configured to use LogAppendTime, the timestamp in the producer record will
   * be overwritten by the broker with the broker local time when it appends the message to its log.
   * @return the timestamp column name
   */
  @Override
  public Optional<ColumnName> timestampColumn() {
    return Optional.ofNullable(timestampColumn);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#table() table} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for table
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withTable(Table value) {
    if (this.table == value) return this;
    Table newValue = Objects.requireNonNull(value, "table");
    return validate(new ImmutableKafkaPublishOptions(
        newValue,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#topic() topic} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for topic (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withTopic(@Nullable String value) {
    if (Objects.equals(this.topic, value)) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        value,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link KafkaPublishOptions#partition() partition} attribute.
   * @param value The value for partition
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKafkaPublishOptions withPartition(int value) {
    @Nullable Integer newValue = value;
    if (Objects.equals(this.partition, newValue)) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        newValue,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link KafkaPublishOptions#partition() partition} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for partition
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKafkaPublishOptions withPartition(OptionalInt optional) {
    @Nullable Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.partition, value)) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        value,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#config() config} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for config
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withConfig(Properties value) {
    if (this.config == value) return this;
    Properties newValue = Objects.requireNonNull(value, "config");
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        newValue,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#keySpec() keySpec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for keySpec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withKeySpec(KafkaTools.Produce.KeyOrValueSpec value) {
    if (this.keySpec == value) return this;
    KafkaTools.Produce.KeyOrValueSpec newValue = Objects.requireNonNull(value, "keySpec");
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        newValue,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#valueSpec() valueSpec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for valueSpec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withValueSpec(KafkaTools.Produce.KeyOrValueSpec value) {
    if (this.valueSpec == value) return this;
    KafkaTools.Produce.KeyOrValueSpec newValue = Objects.requireNonNull(value, "valueSpec");
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        newValue,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#lastBy() lastBy} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for lastBy
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withLastBy(boolean value) {
    if (this.lastBy == value) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        value,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaPublishOptions#publishInitial() publishInitial} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for publishInitial
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableKafkaPublishOptions withPublishInitial(boolean value) {
    if (this.publishInitial == value) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        value,
        this.topicColumn,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link KafkaPublishOptions#topicColumn() topicColumn} attribute.
   * @param value The value for topicColumn
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKafkaPublishOptions withTopicColumn(ColumnName value) {
    @Nullable ColumnName newValue = Objects.requireNonNull(value, "topicColumn");
    if (this.topicColumn == newValue) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        newValue,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link KafkaPublishOptions#topicColumn() topicColumn} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for topicColumn
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableKafkaPublishOptions withTopicColumn(Optional<? extends ColumnName> optional) {
    @Nullable ColumnName value = optional.orElse(null);
    if (this.topicColumn == value) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        value,
        this.partitionColumn,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link KafkaPublishOptions#partitionColumn() partitionColumn} attribute.
   * @param value The value for partitionColumn
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKafkaPublishOptions withPartitionColumn(ColumnName value) {
    @Nullable ColumnName newValue = Objects.requireNonNull(value, "partitionColumn");
    if (this.partitionColumn == newValue) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        newValue,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link KafkaPublishOptions#partitionColumn() partitionColumn} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for partitionColumn
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableKafkaPublishOptions withPartitionColumn(Optional<? extends ColumnName> optional) {
    @Nullable ColumnName value = optional.orElse(null);
    if (this.partitionColumn == value) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        value,
        this.timestampColumn));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link KafkaPublishOptions#timestampColumn() timestampColumn} attribute.
   * @param value The value for timestampColumn
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKafkaPublishOptions withTimestampColumn(ColumnName value) {
    @Nullable ColumnName newValue = Objects.requireNonNull(value, "timestampColumn");
    if (this.timestampColumn == newValue) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link KafkaPublishOptions#timestampColumn() timestampColumn} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for timestampColumn
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableKafkaPublishOptions withTimestampColumn(Optional<? extends ColumnName> optional) {
    @Nullable ColumnName value = optional.orElse(null);
    if (this.timestampColumn == value) return this;
    return validate(new ImmutableKafkaPublishOptions(
        this.table,
        this.topic,
        this.partition,
        this.config,
        this.keySpec,
        this.valueSpec,
        this.lastBy,
        this.publishInitial,
        this.topicColumn,
        this.partitionColumn,
        value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableKafkaPublishOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableKafkaPublishOptions
        && equalTo(0, (ImmutableKafkaPublishOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableKafkaPublishOptions another) {
    return table.equals(another.table)
        && Objects.equals(topic, another.topic)
        && Objects.equals(partition, another.partition)
        && config.equals(another.config)
        && keySpec.equals(another.keySpec)
        && valueSpec.equals(another.valueSpec)
        && lastBy == another.lastBy
        && publishInitial == another.publishInitial
        && Objects.equals(topicColumn, another.topicColumn)
        && Objects.equals(partitionColumn, another.partitionColumn)
        && Objects.equals(timestampColumn, another.timestampColumn);
  }

  /**
   * Computes a hash code from attributes: {@code table}, {@code topic}, {@code partition}, {@code config}, {@code keySpec}, {@code valueSpec}, {@code lastBy}, {@code publishInitial}, {@code topicColumn}, {@code partitionColumn}, {@code timestampColumn}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + table.hashCode();
    h += (h << 5) + Objects.hashCode(topic);
    h += (h << 5) + Objects.hashCode(partition);
    h += (h << 5) + config.hashCode();
    h += (h << 5) + keySpec.hashCode();
    h += (h << 5) + valueSpec.hashCode();
    h += (h << 5) + Booleans.hashCode(lastBy);
    h += (h << 5) + Booleans.hashCode(publishInitial);
    h += (h << 5) + Objects.hashCode(topicColumn);
    h += (h << 5) + Objects.hashCode(partitionColumn);
    h += (h << 5) + Objects.hashCode(timestampColumn);
    return h;
  }

  /**
   * Prints the immutable value {@code KafkaPublishOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("KafkaPublishOptions")
        .omitNullValues()
        .add("table", table)
        .add("topic", topic)
        .add("partition", partition)
        .add("config", config)
        .add("keySpec", keySpec)
        .add("valueSpec", valueSpec)
        .add("lastBy", lastBy)
        .add("publishInitial", publishInitial)
        .add("topicColumn", topicColumn)
        .add("partitionColumn", partitionColumn)
        .add("timestampColumn", timestampColumn)
        .toString();
  }

  private static ImmutableKafkaPublishOptions validate(ImmutableKafkaPublishOptions instance) {
    instance.checkTimestampColumn();
    instance.checkPartitionColumn();
    instance.checkTopicColumn();
    instance.checkTopic();
    instance.checkLastBy();
    instance.checkPublishInitial();
    instance.checkNotBothIgnore();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link KafkaPublishOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable KafkaPublishOptions instance
   */
  public static ImmutableKafkaPublishOptions copyOf(KafkaPublishOptions instance) {
    if (instance instanceof ImmutableKafkaPublishOptions) {
      return (ImmutableKafkaPublishOptions) instance;
    }
    return ImmutableKafkaPublishOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableKafkaPublishOptions ImmutableKafkaPublishOptions}.
   * <pre>
   * ImmutableKafkaPublishOptions.builder()
   *    .table(io.deephaven.engine.table.Table) // required {@link KafkaPublishOptions#table() table}
   *    .topic(String | null) // nullable {@link KafkaPublishOptions#topic() topic}
   *    .partition(int) // optional {@link KafkaPublishOptions#partition() partition}
   *    .config(Properties) // required {@link KafkaPublishOptions#config() config}
   *    .keySpec(io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec) // optional {@link KafkaPublishOptions#keySpec() keySpec}
   *    .valueSpec(io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec) // optional {@link KafkaPublishOptions#valueSpec() valueSpec}
   *    .lastBy(boolean) // optional {@link KafkaPublishOptions#lastBy() lastBy}
   *    .publishInitial(boolean) // optional {@link KafkaPublishOptions#publishInitial() publishInitial}
   *    .topicColumn(io.deephaven.api.ColumnName) // optional {@link KafkaPublishOptions#topicColumn() topicColumn}
   *    .partitionColumn(io.deephaven.api.ColumnName) // optional {@link KafkaPublishOptions#partitionColumn() partitionColumn}
   *    .timestampColumn(io.deephaven.api.ColumnName) // optional {@link KafkaPublishOptions#timestampColumn() timestampColumn}
   *    .build();
   * </pre>
   * @return A new ImmutableKafkaPublishOptions builder
   */
  public static ImmutableKafkaPublishOptions.Builder builder() {
    return new ImmutableKafkaPublishOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableKafkaPublishOptions ImmutableKafkaPublishOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaPublishOptions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements KafkaPublishOptions.Builder {
    private static final long INIT_BIT_TABLE = 0x1L;
    private static final long INIT_BIT_CONFIG = 0x2L;
    private static final long OPT_BIT_LAST_BY = 0x1L;
    private static final long OPT_BIT_PUBLISH_INITIAL = 0x2L;
    private long initBits = 0x3L;
    private long optBits;

    private @Nullable Table table;
    private @Nullable String topic;
    private @Nullable Integer partition;
    private @Nullable Properties config;
    private @Nullable KafkaTools.Produce.KeyOrValueSpec keySpec;
    private @Nullable KafkaTools.Produce.KeyOrValueSpec valueSpec;
    private boolean lastBy;
    private boolean publishInitial;
    private @Nullable ColumnName topicColumn;
    private @Nullable ColumnName partitionColumn;
    private @Nullable ColumnName timestampColumn;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code KafkaPublishOptions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaPublishOptions instance) {
      Objects.requireNonNull(instance, "instance");
      table(instance.table());
      @Nullable String topicValue = instance.topic();
      if (topicValue != null) {
        topic(topicValue);
      }
      OptionalInt partitionOptional = instance.partition();
      if (partitionOptional.isPresent()) {
        partition(partitionOptional);
      }
      config(instance.config());
      keySpec(instance.keySpec());
      valueSpec(instance.valueSpec());
      lastBy(instance.lastBy());
      publishInitial(instance.publishInitial());
      Optional<ColumnName> topicColumnOptional = instance.topicColumn();
      if (topicColumnOptional.isPresent()) {
        topicColumn(topicColumnOptional);
      }
      Optional<ColumnName> partitionColumnOptional = instance.partitionColumn();
      if (partitionColumnOptional.isPresent()) {
        partitionColumn(partitionColumnOptional);
      }
      Optional<ColumnName> timestampColumnOptional = instance.timestampColumn();
      if (timestampColumnOptional.isPresent()) {
        timestampColumn(timestampColumnOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#table() table} attribute.
     * @param table The value for table 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder table(Table table) {
      this.table = Objects.requireNonNull(table, "table");
      initBits &= ~INIT_BIT_TABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#topic() topic} attribute.
     * @param topic The value for topic (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder topic(@Nullable String topic) {
      this.topic = topic;
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#partition() partition} to partition.
     * @param partition The value for partition
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder partition(int partition) {
      this.partition = partition;
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#partition() partition} to partition.
     * @param partition The value for partition
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder partition(OptionalInt partition) {
      this.partition = partition.isPresent() ? partition.getAsInt() : null;
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#config() config} attribute.
     * @param config The value for config 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder config(Properties config) {
      this.config = Objects.requireNonNull(config, "config");
      initBits &= ~INIT_BIT_CONFIG;
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#keySpec() keySpec} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link KafkaPublishOptions#keySpec() keySpec}.</em>
     * @param keySpec The value for keySpec 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder keySpec(KafkaTools.Produce.KeyOrValueSpec keySpec) {
      this.keySpec = Objects.requireNonNull(keySpec, "keySpec");
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#valueSpec() valueSpec} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link KafkaPublishOptions#valueSpec() valueSpec}.</em>
     * @param valueSpec The value for valueSpec 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder valueSpec(KafkaTools.Produce.KeyOrValueSpec valueSpec) {
      this.valueSpec = Objects.requireNonNull(valueSpec, "valueSpec");
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#lastBy() lastBy} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link KafkaPublishOptions#lastBy() lastBy}.</em>
     * @param lastBy The value for lastBy 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder lastBy(boolean lastBy) {
      this.lastBy = lastBy;
      optBits |= OPT_BIT_LAST_BY;
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaPublishOptions#publishInitial() publishInitial} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link KafkaPublishOptions#publishInitial() publishInitial}.</em>
     * @param publishInitial The value for publishInitial 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder publishInitial(boolean publishInitial) {
      this.publishInitial = publishInitial;
      optBits |= OPT_BIT_PUBLISH_INITIAL;
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#topicColumn() topicColumn} to topicColumn.
     * @param topicColumn The value for topicColumn
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder topicColumn(ColumnName topicColumn) {
      this.topicColumn = Objects.requireNonNull(topicColumn, "topicColumn");
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#topicColumn() topicColumn} to topicColumn.
     * @param topicColumn The value for topicColumn
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder topicColumn(Optional<? extends ColumnName> topicColumn) {
      this.topicColumn = topicColumn.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#partitionColumn() partitionColumn} to partitionColumn.
     * @param partitionColumn The value for partitionColumn
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder partitionColumn(ColumnName partitionColumn) {
      this.partitionColumn = Objects.requireNonNull(partitionColumn, "partitionColumn");
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#partitionColumn() partitionColumn} to partitionColumn.
     * @param partitionColumn The value for partitionColumn
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder partitionColumn(Optional<? extends ColumnName> partitionColumn) {
      this.partitionColumn = partitionColumn.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#timestampColumn() timestampColumn} to timestampColumn.
     * @param timestampColumn The value for timestampColumn
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder timestampColumn(ColumnName timestampColumn) {
      this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
      return this;
    }

    /**
     * Initializes the optional value {@link KafkaPublishOptions#timestampColumn() timestampColumn} to timestampColumn.
     * @param timestampColumn The value for timestampColumn
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder timestampColumn(Optional<? extends ColumnName> timestampColumn) {
      this.timestampColumn = timestampColumn.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableKafkaPublishOptions ImmutableKafkaPublishOptions}.
     * @return An immutable instance of KafkaPublishOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableKafkaPublishOptions build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableKafkaPublishOptions.validate(new ImmutableKafkaPublishOptions(this));
    }

    private boolean lastByIsSet() {
      return (optBits & OPT_BIT_LAST_BY) != 0;
    }

    private boolean publishInitialIsSet() {
      return (optBits & OPT_BIT_PUBLISH_INITIAL) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TABLE) != 0) attributes.add("table");
      if ((initBits & INIT_BIT_CONFIG) != 0) attributes.add("config");
      return "Cannot build KafkaPublishOptions, some of required attributes are not set " + attributes;
    }
  }
}
