package io.deephaven.queryutil.dataadapter.example;

import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;
import io.deephaven.queryutil.dataadapter.KeyedRecordAdapter;
import io.deephaven.queryutil.dataadapter.TableToRecordListener;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.queryutil.dataadapter.rec.RecordUpdaters;
import org.apache.commons.lang3.builder.ToStringBuilder;

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
                    .addColumnAdapter("Sym", RecordUpdaters.getStringUpdater(MyTradeHolder::setSym))
                    .addColumnAdapter("Price", RecordUpdaters.getDoubleUpdater(MyTradeHolder::setPrice))
                    .addColumnAdapter("Size", RecordUpdaters.getIntUpdater(MyTradeHolder::setSize))
                    .addColumnAdapter("Timestamp",
                            RecordUpdaters.getReferenceTypeUpdater(DateTime.class, MyTradeHolder::setTimestamp))
                    .build();

    private String sym;
    private DateTime timestamp;
    private double price;
    private int size;

    private static void example(Table tradesTable) {
        final KeyedRecordAdapter<String, MyTradeHolder> keyedRecordAdapter =
                KeyedRecordAdapter.makeKeyedRecordAdapterSimpleKey(
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

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
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
