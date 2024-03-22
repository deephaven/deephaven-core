//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.desc;

import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.datafetch.bulk.DefaultMultiRowRecordAdapter;
import io.deephaven.dataadapter.datafetch.single.SingleRowRecordAdapter;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.updaters.*;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 *
 * @param <R> The record type
 */
public class RecordAdapterDescriptorBuilder<R> {

    private final Map<String, RecordUpdater<R, ?>> colNameToAdapterMap = new LinkedHashMap<>();
    private final Supplier<R> emptyRecordSupplier;

    private BiFunction<Table, RecordAdapterDescriptor<R>, SingleRowRecordAdapter<R>> singleRowAdapterSupplier =
            SingleRowRecordAdapter::create;
    private BiFunction<Table, RecordAdapterDescriptor<R>, MultiRowRecordAdapter<R>> multiRowAdapterSupplier =
            DefaultMultiRowRecordAdapter::create;

    private RecordAdapterDescriptorBuilder(Supplier<R> emptyRecordSupplier) {
        this.emptyRecordSupplier = emptyRecordSupplier;
    }

    /**
     * Create a builder for records of type {@code R}.
     *
     * @param emptyRecordSupplier A supplier of empty records
     * @param <T> The record data type
     * @return A RecordAdapterDescriptor builder that can populate empty records of type {@code R} with data
     */
    public static <T> RecordAdapterDescriptorBuilder<T> create(Supplier<T> emptyRecordSupplier) {
        return new RecordAdapterDescriptorBuilder<>(emptyRecordSupplier);
    }

    /**
     * Create a builder for records of type {@code R}, with the same {@link #emptyRecordSupplier} and
     * {@link #colNameToAdapterMap column adapters} as the given {@code base}.
     *
     * @param base The base RecordAdapterDescriptor to copy
     * @param <T> The record data type
     * @return A RecordAdapterDescriptor builder that can populate empty records of type {@code R} with data
     */
    public static <T> RecordAdapterDescriptorBuilder<T> create(RecordAdapterDescriptor<T> base) {
        final RecordAdapterDescriptorBuilder<T> copy = new RecordAdapterDescriptorBuilder<>(base::getEmptyRecord);
        copy.colNameToAdapterMap.putAll(base.getColumnAdapters());
        copy.singleRowAdapterSupplier = base.getSingleRowAdapterSupplier();
        copy.multiRowAdapterSupplier = base.getMultiRowAdapterSupplier();
        return copy;
    }

    /**
     * Add an adapter that maps data from a column named {@code colName} into a record of type {@code R}. The input type
     * {@code C} to the adapter must match the data type for {@code colName} in the table with which the record adapter
     * will be used.
     *
     * @param colName The name of the column to map into records
     * @param adapter An adapter that updates a record of type {@code R} with a value of type {@code C} (or the
     *        corresponding primitive type, if {@code C} is a boxed type).
     * @param <C> The data type in the column corresponding to {@code colName}
     * @return This builder.
     */
    public <C> RecordAdapterDescriptorBuilder<R> addColumnAdapter(final String colName,
            final RecordUpdater<R, C> adapter) {
        colNameToAdapterMap.put(colName, adapter);
        return this;
    }

    // These are used for capturing lambadas with the correct types:

    public <T> RecordAdapterDescriptorBuilder<R> addObjColumnAdapter(final String colName, Class<T> colType,
            final BiConsumer<R, T> recordUpdater) {
        return addColumnAdapter(colName, ObjRecordUpdater.getObjectUpdater(colType, recordUpdater));
    }

    public RecordAdapterDescriptorBuilder<R> addStringColumnAdapter(final String colName,
            final BiConsumer<R, String> recordUpdater) {
        return addObjColumnAdapter(colName, String.class, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addCharColumnAdapter(final String colName,
            final CharRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addByteColumnAdapter(final String colName,
            final ByteRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addShortColumnAdapter(final String colName,
            final ShortRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addIntColumnAdapter(final String colName,
            final IntRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addFloatColumnAdapter(final String colName,
            final FloatRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addLongColumnAdapter(final String colName,
            final LongRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordAdapterDescriptorBuilder<R> addDoubleColumnAdapter(final String colName,
            final DoubleRecordUpdater<R> recordUpdater) {
        return addColumnAdapter(colName, recordUpdater);
    }

    public RecordUpdater<R, ?> removeColumn(final String colName) {
        return colNameToAdapterMap.remove(colName);
    }

    public void setSingleRowAdapterSupplier(
            @NotNull BiFunction<Table, RecordAdapterDescriptor<R>, SingleRowRecordAdapter<R>> singleRowAdapterSupplier) {
        this.singleRowAdapterSupplier = singleRowAdapterSupplier;
    }

    public void setMultiRowAdapterSupplier(
            @NotNull BiFunction<Table, RecordAdapterDescriptor<R>, MultiRowRecordAdapter<R>> multiRowAdapterSupplier) {
        this.multiRowAdapterSupplier = multiRowAdapterSupplier;
    }

    public RecordAdapterDescriptor<R> build() {
        return new RecordAdapterDescriptorImpl<R>(
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

        private RecordAdapterDescriptorImpl(
                Map<String, RecordUpdater<R, ?>> colNameToAdapterMap,
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
