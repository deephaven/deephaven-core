package io.deephaven.api.snapshot;

import io.deephaven.api.JoinAddition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SnapshotWhenOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSnapshotWhenOptions.builder()}.
 */
@Generated(from = "SnapshotWhenOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableSnapshotWhenOptions extends SnapshotWhenOptions {
  private final Set<SnapshotWhenOptions.Flag> flags;
  private final List<JoinAddition> stampColumns;

  private ImmutableSnapshotWhenOptions(
      Set<SnapshotWhenOptions.Flag> flags,
      List<JoinAddition> stampColumns) {
    this.flags = flags;
    this.stampColumns = stampColumns;
  }

  /**
   * The feature flags.
   */
  @Override
  public Set<SnapshotWhenOptions.Flag> flags() {
    return flags;
  }

  /**
   * The {@code trigger} table stamp columns. If empty, it represents all columns from the {@code trigger} table.
   */
  @Override
  public List<JoinAddition> stampColumns() {
    return stampColumns;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SnapshotWhenOptions#flags() flags}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSnapshotWhenOptions withFlags(SnapshotWhenOptions.Flag... elements) {
    Set<SnapshotWhenOptions.Flag> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableSnapshotWhenOptions(newValue, this.stampColumns));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SnapshotWhenOptions#flags() flags}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of flags elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSnapshotWhenOptions withFlags(Iterable<SnapshotWhenOptions.Flag> elements) {
    if (this.flags == elements) return this;
    Set<SnapshotWhenOptions.Flag> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableSnapshotWhenOptions(newValue, this.stampColumns));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SnapshotWhenOptions#stampColumns() stampColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSnapshotWhenOptions withStampColumns(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableSnapshotWhenOptions(this.flags, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SnapshotWhenOptions#stampColumns() stampColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of stampColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSnapshotWhenOptions withStampColumns(Iterable<? extends JoinAddition> elements) {
    if (this.stampColumns == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableSnapshotWhenOptions(this.flags, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSnapshotWhenOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSnapshotWhenOptions
        && equalTo(0, (ImmutableSnapshotWhenOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableSnapshotWhenOptions another) {
    return flags.equals(another.flags)
        && stampColumns.equals(another.stampColumns);
  }

  /**
   * Computes a hash code from attributes: {@code flags}, {@code stampColumns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + flags.hashCode();
    h += (h << 5) + stampColumns.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SnapshotWhenOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SnapshotWhenOptions{"
        + "flags=" + flags
        + ", stampColumns=" + stampColumns
        + "}";
  }

  private static ImmutableSnapshotWhenOptions validate(ImmutableSnapshotWhenOptions instance) {
    instance.checkHistory();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link SnapshotWhenOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SnapshotWhenOptions instance
   */
  public static ImmutableSnapshotWhenOptions copyOf(SnapshotWhenOptions instance) {
    if (instance instanceof ImmutableSnapshotWhenOptions) {
      return (ImmutableSnapshotWhenOptions) instance;
    }
    return ImmutableSnapshotWhenOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSnapshotWhenOptions ImmutableSnapshotWhenOptions}.
   * <pre>
   * ImmutableSnapshotWhenOptions.builder()
   *    .addFlags|addAllFlags(io.deephaven.api.snapshot.SnapshotWhenOptions.Flag) // {@link SnapshotWhenOptions#flags() flags} elements
   *    .addStampColumns|addAllStampColumns(io.deephaven.api.JoinAddition) // {@link SnapshotWhenOptions#stampColumns() stampColumns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableSnapshotWhenOptions builder
   */
  public static ImmutableSnapshotWhenOptions.Builder builder() {
    return new ImmutableSnapshotWhenOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSnapshotWhenOptions ImmutableSnapshotWhenOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SnapshotWhenOptions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements SnapshotWhenOptions.Builder {
    private EnumSet<SnapshotWhenOptions.Flag> flags = EnumSet.noneOf(SnapshotWhenOptions.Flag.class);
    private List<JoinAddition> stampColumns = new ArrayList<JoinAddition>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SnapshotWhenOptions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SnapshotWhenOptions instance) {
      Objects.requireNonNull(instance, "instance");
      addAllFlags(instance.flags());
      addAllStampColumns(instance.stampColumns());
      return this;
    }

    /**
     * Adds one element to {@link SnapshotWhenOptions#flags() flags} set.
     * @param element A flags element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFlags(SnapshotWhenOptions.Flag element) {
      this.flags.add(Objects.requireNonNull(element, "flags element"));
      return this;
    }

    /**
     * Adds elements to {@link SnapshotWhenOptions#flags() flags} set.
     * @param elements An array of flags elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFlags(SnapshotWhenOptions.Flag... elements) {
      for (SnapshotWhenOptions.Flag element : elements) {
        this.flags.add(Objects.requireNonNull(element, "flags element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link SnapshotWhenOptions#flags() flags} set.
     * @param elements An iterable of flags elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder flags(Iterable<SnapshotWhenOptions.Flag> elements) {
      this.flags.clear();
      return addAllFlags(elements);
    }

    /**
     * Adds elements to {@link SnapshotWhenOptions#flags() flags} set.
     * @param elements An iterable of flags elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllFlags(Iterable<SnapshotWhenOptions.Flag> elements) {
      for (SnapshotWhenOptions.Flag element : elements) {
        this.flags.add(Objects.requireNonNull(element, "flags element"));
      }
      return this;
    }

    /**
     * Adds one element to {@link SnapshotWhenOptions#stampColumns() stampColumns} list.
     * @param element A stampColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addStampColumns(JoinAddition element) {
      this.stampColumns.add(Objects.requireNonNull(element, "stampColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link SnapshotWhenOptions#stampColumns() stampColumns} list.
     * @param elements An array of stampColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addStampColumns(JoinAddition... elements) {
      for (JoinAddition element : elements) {
        this.stampColumns.add(Objects.requireNonNull(element, "stampColumns element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link SnapshotWhenOptions#stampColumns() stampColumns} list.
     * @param elements An iterable of stampColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder stampColumns(Iterable<? extends JoinAddition> elements) {
      this.stampColumns.clear();
      return addAllStampColumns(elements);
    }

    /**
     * Adds elements to {@link SnapshotWhenOptions#stampColumns() stampColumns} list.
     * @param elements An iterable of stampColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllStampColumns(Iterable<? extends JoinAddition> elements) {
      for (JoinAddition element : elements) {
        this.stampColumns.add(Objects.requireNonNull(element, "stampColumns element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableSnapshotWhenOptions ImmutableSnapshotWhenOptions}.
     * @return An immutable instance of SnapshotWhenOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSnapshotWhenOptions build() {
      return ImmutableSnapshotWhenOptions.validate(new ImmutableSnapshotWhenOptions(createUnmodifiableEnumSet(flags), createUnmodifiableList(true, stampColumns)));
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Enum<T>> Set<T> createUnmodifiableEnumSet(Iterable<T> iterable) {
    if (iterable instanceof EnumSet<?>) {
      return Collections.unmodifiableSet(EnumSet.copyOf((EnumSet<T>) iterable));
    }
    List<T> list = createSafeList(iterable, true, false);
    switch(list.size()) {
    case 0: return Collections.emptySet();
    case 1: return Collections.singleton(list.get(0));
    default: return Collections.unmodifiableSet(EnumSet.copyOf(list));
    }
  }
}
