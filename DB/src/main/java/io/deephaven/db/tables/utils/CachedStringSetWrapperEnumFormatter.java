/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.formatters.EnumFormatter;
import io.deephaven.db.tables.StringSetWrapper;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * This object extends the EnumFormatter class and affords the caching of string set results Be
 * warned this could create a large hash if the possible enum combinations get very large.
 */
public class CachedStringSetWrapperEnumFormatter extends EnumFormatter {

    protected TIntObjectHashMap<StringSetWrapper> indexToStringSetWrapper =
        new TIntObjectHashMap<>();

    public CachedStringSetWrapperEnumFormatter(String[] enums) {
        super(enums);
    }

    public StringSetWrapper formatStringSetWrapper(int index) {
        StringSetWrapper result = indexToStringSetWrapper.get(index);
        if (result == null) {
            int count = 1;
            result = new StringSetWrapper(enumsToString.size());
            while (count <= index) {
                if ((index & count) != 0) {
                    String data = enumsToString.get(count);
                    if (data != null) {
                        result.addStringToSet(data);
                    }
                }
                if (count == 1 << 31) {
                    break;
                }
                count = count << 1;
            }

            indexToStringSetWrapper.put(index, result);
        }
        return result;
    }
}
