/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InMemoryBlockTableWriter {
    private final Logger logger = LoggerFactory.getLogger(InMemoryBlockTableWriter.class);

    private final TableDefinition definition;
    private final int baseBlockSize;

    private final List<Table> tableBlocks = new ArrayList<>();

    private InMemoryBlockTable currentTable = null;

    public InMemoryBlockTableWriter(TableDefinition definition) throws IOException {
        this(definition, 1000);
    }

    public InMemoryBlockTableWriter(TableDefinition definition, int baseBlockSize) throws IOException {
        this.definition = definition;
        this.baseBlockSize = baseBlockSize;
        initialize();
    }

    private void initialize() throws IOException {
        currentTable = new InMemoryBlockTable(definition, baseBlockSize); // , initialCapacity);
    }

    public void cycleBlock(boolean end) throws IOException {
        cycleBlock(end, null);
    }

    public void cycleBlock(boolean end, String info) throws IOException {
        if (null == currentTable) {
            logger.info().append("Initialize block").endl();
            currentTable = new InMemoryBlockTable(definition, baseBlockSize);
        }
        if (currentTable.size() >= baseBlockSize) {
            logger.info().append("Cycle Block: ").append(info).append(tableBlocks.size()).endl();
            tableBlocks.add(currentTable.getTable());
            currentTable = new InMemoryBlockTable(definition, baseBlockSize); // , initialCapacity);
        }
        if (end) {
            logger.info().append("complete block").endl();
            tableBlocks.add(currentTable.getTable().head(currentTable.size()));
        }
    }



    private class InMemoryBlockTable {

        private int index = 0;
        private Table _imt;

        private InMemoryBlockTable(TableDefinition tabledef, int size) {
            _imt = new InMemoryTable(tabledef, size);
        }

        public void setValue(String columname, Object value) {
            try {
                // noinspection unchecked
                ((WritableSource) _imt.getColumnSource(columname)).set(index, value);
            } catch (ClassCastException e) {
                logger.error().append("Working on column: ").append(columname).endl();
                throw e;
            }
        }

        public Table getTable() {
            return _imt;
        }

        public int size() {
            return index;
        }

        public void writeRow() {
            index++;
        }
    }



    public void setValue(String columname, Object value) {
        currentTable.setValue(columname, value);
    }

    public Table getBlockTable() throws IOException {
        logger.info().append("getBlockTable() ...").endl();
        cycleBlock(true, null);
        Table res;
        try {
            logger.info().append("Before merge").endl();
            res = io.deephaven.db.tables.utils.TableTools.merge(tableBlocks);
            logger.info().append("After merge").endl();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        logger.info().append("... getBlockTable()").endl();
        return res.select();
    }

    public List<Table> getBlockTables() {
        return tableBlocks;
    }

    public void writeRow() {
        currentTable.writeRow();
    }
}
