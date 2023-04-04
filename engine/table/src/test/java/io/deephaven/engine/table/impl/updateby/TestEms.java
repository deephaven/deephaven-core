package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.numerics.movingaverages.AbstractMa;
import io.deephaven.numerics.movingaverages.ByEma;
import io.deephaven.numerics.movingaverages.ByEmaSimple;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.time.DateTimeUtils.MINUTE;
import static io.deephaven.time.DateTimeUtils.convertDateTime;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Category(OutOfBandTest.class)
public class TestEms extends BaseUpdateByTest {
    /**
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
     */
    final String[] columns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "bigIntCol",
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    // region Zero Key Tests
    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedDateTimeGenerator(
                                convertDateTime("2022-03-09T09:00:00.000 NY"),
                                convertDateTime("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        final Table actualSkip = t.updateBy(UpdateByOperation.Ems(skipControl, 100, columns));
        final Table actualReset = t.updateBy(UpdateByOperation.Ems(resetControl, 100, columns));
        // final Table expected = t.update(getCastingFormulas(primitiveColumns))
        // .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
        // .update(getFormulas(primitiveColumns));
        // TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact);
    }
    // endregion


    // region Manual Verification functions
    public static double[] compute_ems_ticks(OperationControl control, long ticks, double[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        double[] result = new double[values.length];
        result[0] = isNull(values[0]) ? NULL_LONG : values[0];

        for (int i = 1; i < values.length; i++) {
            final boolean curValNull = isNull(values[i]);
            if (isNull(result[i - 1])) {
                result[i] = curValNull ? NULL_LONG : values[i];
            } else {
                if (curValNull) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = result[i - 1] * values[i];
                }
            }
        }

        return result;
    }

    // endregion
}
