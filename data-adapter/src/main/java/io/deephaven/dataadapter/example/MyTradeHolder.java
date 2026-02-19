//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.example;

import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.KeyedRecordAdapter;
import io.deephaven.dataadapter.TableToRecordListener;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

public class MyTradeHolder {

    /**
     * Object that describes how to map rows of a table with columns "Price", "Size", and "Timestamp" into instances of
     * {@link MyTradeHolder}.
     */
    public static final RecordAdapterDescriptor<MyTradeHolder> myTradeHolderRecordAdapterDescriptor =
            RecordAdapterDescriptorBuilder
                    .create(MyTradeHolder::new)
                    .addStringColumnAdapter("Sym", MyTradeHolder::setSym)
                    .addDoubleColumnAdapter("Price", MyTradeHolder::setPrice)
                    .addIntColumnAdapter("Size", MyTradeHolder::setSize)
                    .addObjColumnAdapter("Timestamp", Instant.class, MyTradeHolder::setTimestamp)
                    .build();

    private String sym;
    private Instant timestamp;
    private double price;
    private int size;

    private static void example(Table tradesTable) {
        final KeyedRecordAdapter<String, MyTradeHolder> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        tradesTable,
                        myTradeHolderRecordAdapterDescriptor,
                        "USym",
                        String.class);

        // Get the last AAPL trade:
        final MyTradeHolder lastAaplTrade = keyedRecordAdapter.getRecord("AAPL");

        // Get the last trades for a few symbols:
        final Map<String, MyTradeHolder> lastTradesBySymbol = keyedRecordAdapter.getRecords("AAPL", "CAT", "SPY");


        // Or listen for new trades:
        final Consumer<MyTradeHolder> tradeConsumer = trade -> System.out.println(trade.toString());
        TableToRecordListener.create(tradesTable, myTradeHolderRecordAdapterDescriptor, tradeConsumer);
    }

    public String getSym() {
        return sym;
    }

    public void setSym(String sym) {
        this.sym = sym;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sym", sym)
                .append("timestamp", timestamp)
                .append("price", price)
                .append("size", size)
                .toString();
    }
}
