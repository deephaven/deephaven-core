package io.deephaven.queryutil.dataadapter.rec.desc;

import io.deephaven.engine.table.Table;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.DefaultMultiRowRecordAdapter;
import io.deephaven.queryutil.dataadapter.datafetch.single.SingleRowRecordAdapter;
import io.deephaven.queryutil.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.queryutil.dataadapter.rec.RecordUpdater;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class RecordAdapterDescriptorBuilder<T> {

    private final Map<String, RecordUpdater<T, ?>> colNameToAdapterMap = new LinkedHashMap<>();
    private final Supplier<T> emptyRecordSupplier;

    private BiFunction<Table, RecordAdapterDescriptor<T>, SingleRowRecordAdapter<T>> singleRowAdapterSupplier = SingleRowRecordAdapter::create;
    private BiFunction<Table, RecordAdapterDescriptor<T>, MultiRowRecordAdapter<T>> multiRowAdapterSupplier = DefaultMultiRowRecordAdapter::create;

    private RecordAdapterDescriptorBuilder(Supplier<T> emptyRecordSupplier) {
        this.emptyRecordSupplier = emptyRecordSupplier;
    }

    /**
     * Create a builder for records of type {@code T}.
     *
     * @param emptyRecordSupplier A supplier of empty records
     * @param <T>                 The record data type
     * @return A RecordAdapterDescriptor builder that can populate empty records of type {@code T} with data
     */
    public static <T> RecordAdapterDescriptorBuilder<T> create(Supplier<T> emptyRecordSupplier) {
        return new RecordAdapterDescriptorBuilder<>(emptyRecordSupplier);
    }

    /**
     * Create a builder for records of type {@code T}, with the same {@link #emptyRecordSupplier} and
     * {@link #colNameToAdapterMap column adapters} as the given {@code base}.
     *
     * @param base The base RecordAdapterDescriptor to copy
     * @param <T>  The record data type
     * @return A RecordAdapterDescriptor builder that can populate empty records of type {@code T} with data
     */
    public static <T> RecordAdapterDescriptorBuilder<T> create(RecordAdapterDescriptor<T> base) {
        final RecordAdapterDescriptorBuilder<T> copy = new RecordAdapterDescriptorBuilder<>(base::getEmptyRecord);
        copy.colNameToAdapterMap.putAll(base.getColumnAdapters());
        copy.singleRowAdapterSupplier = base.getSingleRowAdapterSupplier();
        copy.multiRowAdapterSupplier = base.getMultiRowAdapterSupplier();
        return copy;
    }

    /**
     * Add an adapter that maps data from a column named {@code colName} into a record of type {@code T}. The input
     * type {@code C} to the adapter must match the data type for {@code colName} in the table with which the
     * record adapter will be used.
     *
     * @param colName The name of the column to map into records
     * @param adapter An adapter that updates a record of type {@code T} with a value of type {@code C} (or the corresponding primitive type, if {@code C} is a boxed  type).
     * @param <C>     The data type in the column corresponding to {@code colName}
     * @return This builder.
     */
    public <C> RecordAdapterDescriptorBuilder<T> addColumnAdapter(final String colName, final RecordUpdater<T, C> adapter) {
        colNameToAdapterMap.put(colName, adapter);
        return this;
    }

    public RecordUpdater<T, ?> removeColumn(final String colName) {
        return colNameToAdapterMap.remove(colName);
    }

    public void setSingleRowAdapterSupplier(@NotNull BiFunction<Table, RecordAdapterDescriptor<T>, SingleRowRecordAdapter<T>> singleRowAdapterSupplier) {
        this.singleRowAdapterSupplier = singleRowAdapterSupplier;
    }

    public void setMultiRowAdapterSupplier(@NotNull BiFunction<Table, RecordAdapterDescriptor<T>, MultiRowRecordAdapter<T>> multiRowAdapterSupplier) {
        this.multiRowAdapterSupplier = multiRowAdapterSupplier;
    }

    public RecordAdapterDescriptor<T> build() {
        return new RecordAdapterDescriptorImpl<T>(
                Collections.unmodifiableMap(new LinkedHashMap<>(colNameToAdapterMap)),
                emptyRecordSupplier,
                singleRowAdapterSupplier,
                multiRowAdapterSupplier);
    }

    private static class RecordAdapterDescriptorImpl<R> implements RecordAdapterDescriptor<R> {

        private final Map<String, RecordUpdater<R, ?>> colNameToAdapterMap;
        private final Supplier<R> emptyRecordSupplier;

        private final BiFunction<Table, RecordAdapterDescriptor<R>, SingleRowRecordAdapter<R>> singleRowAdapterSupplier;
        private final BiFunction<Table, RecordAdapterDescriptor<R>, MultiRowRecordAdapter<R>> multiRowAdapterSupplier;

        private RecordAdapterDescriptorImpl(Map<String, RecordUpdater<R, ?>> colNameToAdapterMap,
                                            Supplier<R> emptyRecordSupplier,
                                            BiFunction<Table, RecordAdapterDescriptor<R>, SingleRowRecordAdapter<R>> singleRowAdapterSupplier,
                                            BiFunction<Table, RecordAdapterDescriptor<R>, MultiRowRecordAdapter<R>> multiRowAdapterSupplier) {
            this.colNameToAdapterMap = colNameToAdapterMap;
            this.emptyRecordSupplier = emptyRecordSupplier;
            this.singleRowAdapterSupplier = singleRowAdapterSupplier;
            this.multiRowAdapterSupplier = multiRowAdapterSupplier;
        }

        @Override
        public Map<String, RecordUpdater<R, ?>> getColumnAdapters() {
            return colNameToAdapterMap;
        }

        @NotNull
        @Override
        public R getEmptyRecord() {
            return emptyRecordSupplier.get();
        }

        public BiFunction<Table, RecordAdapterDescriptor<R>, SingleRowRecordAdapter<R>> getSingleRowAdapterSupplier() {
            return singleRowAdapterSupplier;
        }

        public BiFunction<Table, RecordAdapterDescriptor<R>, MultiRowRecordAdapter<R>> getMultiRowAdapterSupplier() {
            return multiRowAdapterSupplier;
        }
    }
}
