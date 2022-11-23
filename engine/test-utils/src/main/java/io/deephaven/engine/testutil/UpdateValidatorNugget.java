/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.PrintWriter;
import java.io.StringWriter;

public class UpdateValidatorNugget implements EvalNuggetInterface {

    public UpdateValidatorNugget(final Table table) {
        this((QueryTable) table);
    }

    public UpdateValidatorNugget(final QueryTable table) {
        this.originalValue = table;
        this.validator = TableUpdateValidator.make(originalValue);

        originalValue.addUpdateListener(failureListener);
        validator.getResultTable().addUpdateListener(failureListener);
    }

    private final QueryTable originalValue;
    private final TableUpdateValidator validator;

    private Throwable exception = null;

    // We should listen for failures on the table, and if we get any, the test case is no good.
    private final TableUpdateListener failureListener =
            new InstrumentedTableUpdateListener("Failure Listener") {
                @Override
                public void onUpdate(TableUpdate update) {}

                @Override
                public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                    exception = originalException;
                    final StringWriter errors = new StringWriter();
                    originalException.printStackTrace(new PrintWriter(errors));
                    TestCase.fail(errors.toString());
                }
            };

    public void validate(final String msg) {
        Assert.assertNull(exception);
        Assert.assertEquals(0, validator.getResultTable().size());
    }

    public void show() {
        TableTools.showWithRowSet(originalValue, 100);
    }
}
