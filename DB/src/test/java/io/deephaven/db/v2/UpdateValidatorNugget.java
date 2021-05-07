/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
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

        originalValue.listenForUpdates(failureListener);
        validator.getResultTable().listenForUpdates(failureListener);
    }

    private final QueryTable originalValue;
    private final TableUpdateValidator validator;

    private Throwable exception = null;

    // We should listen for failures on the table, and if we get any, the test case is no good.
    private final ShiftAwareListener failureListener = new InstrumentedShiftAwareListener("Failure Listener") {
        @Override
        public void onUpdate(Update update) {}

        @Override
        public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
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
        TableTools.showWithIndex(originalValue, 100);
    }
}
