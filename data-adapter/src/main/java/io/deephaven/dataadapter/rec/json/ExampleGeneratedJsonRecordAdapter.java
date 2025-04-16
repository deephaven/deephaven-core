//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;

import java.util.ArrayList;

public class ExampleGeneratedJsonRecordAdapter extends BaseJsonRecordAdapter {

    public ExampleGeneratedJsonRecordAdapter(Table sourceTable, RecordAdapterDescriptor<ObjectNode> descriptor) {
        super(
                descriptor,
                TableDataArrayRetriever.makeDefault(descriptor.getColumnNames(), sourceTable),
                "ByteCol",
                "LongCol",
                "StrCol",
                "TimeCol");
    }

    @Override
    public void populateRecords(ObjectNode[] recordsArray, Object[] dataArrays) {
        final int nRecords = recordsArray.length;
        int ii;

        final byte[] col0 = (byte[]) dataArrays[0];
        final String colName0 = "ByteCol";
        for (ii = 0; ii < nRecords; ii++) {
            final byte val = col0[ii];
            if (io.deephaven.function.Basic.isNull(val)) {
                recordsArray[ii].putNull(colName0);
            } else {
                recordsArray[ii].put(
                        colName0,
                        val);
            }
        }

        final long[] col1 = (long[]) dataArrays[1];
        final String colName1 = "LongCol";
        for (ii = 0; ii < nRecords; ii++) {
            final long val = col1[ii];
            if (io.deephaven.function.Basic.isNull(val)) {
                recordsArray[ii].putNull(colName1);
            } else {
                recordsArray[ii].put(
                        colName1,
                        val);
            }
        }

        final String[] col2 = (String[]) dataArrays[2];
        final String colName2 = "StrCol";
        for (ii = 0; ii < nRecords; ii++) {
            final String val = col2[ii];
            if (io.deephaven.function.Basic.isNull(val)) {
                recordsArray[ii].putNull(colName2);
            } else {
                recordsArray[ii].put(
                        colName2,
                        val);
            }
        }

        final char[] col3 = (char[]) dataArrays[3];
        final String colName3 = "TimeCol";
        for (ii = 0; ii < nRecords; ii++) {
            final char val = col3[ii];
            if (io.deephaven.function.Basic.isNull(val)) {
                recordsArray[ii].putNull(colName3);
            } else {
                String mappedVal = Character.toString(val);
                recordsArray[ii].put(
                        colName3,
                        mappedVal);
            }
        }
    }
}
