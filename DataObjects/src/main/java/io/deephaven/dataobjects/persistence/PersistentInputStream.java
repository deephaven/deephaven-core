/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.dataobjects.DataObjectColumnSet;
import io.deephaven.dataobjects.DataObjectColumnSetManager;

import java.io.*;
import java.util.*;

public class PersistentInputStream extends DataObjectInputStream {

    private final HashMap<String, DataObjectColumnSet> colSets_;
    private final HashMap<String,ColumnsetConversionSchema> cachedConversionMap = new HashMap<>();

    public PersistentInputStream(InputStream in, ClassLoader classLoader) throws IOException, ClassNotFoundException {
        super(in, classLoader);
        colSets_ = new HashMap<>();
        final Object colSets[] = (Object[]) readObject();
        for (Object colSet1 : colSets) {
            final DataObjectColumnSet colSet = ((DataObjectColumnSet) colSet1);
            // Support for renamed columnSets - always use the new columnSet name if it's been renamed
            colSets_.put(DataObjectColumnSetManager.getInstance().getRenamedColumnSetName(colSet.getName()), colSet);
        }
    }

    public PersistentInputStream(InputStream in) throws IOException, ClassNotFoundException {
        this(in, null);
    }

    public DataObjectColumnSet getColumnSet(String colSetName) {
        return colSets_.get(colSetName);
    }

    public ColumnsetConversionSchema getConversionSchema(String colSetName) {
        return cachedConversionMap.computeIfAbsent(colSetName, n -> new ColumnsetConversionSchema(colSets_.get(n)));
    }
}
