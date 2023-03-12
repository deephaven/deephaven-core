package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

@RunWith(Parameterized.class)
public class UpdateOrSelectSessionTest extends DeephavenSessionTestBase {

    public static int myFunction() {
        return 42;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> methods() {
        return () -> Arrays.stream(UpdateOrSelect.values()).map(u -> new Object[] {u}).iterator();
    }

    enum UpdateOrSelect implements BiFunction<TableSpec, String[], TableSpec> {
        VIEW {
            @Override
            public TableSpec apply(TableSpec spec, String[] formulas) {
                return spec.view(formulas);
            }
        },
        UPDATE_VIEW {
            @Override
            public TableSpec apply(TableSpec spec, String[] formulas) {
                return spec.updateView(formulas);
            }
        },
        UPDATE {
            @Override
            public TableSpec apply(TableSpec spec, String[] formulas) {
                return spec.update(formulas);
            }
        },
        SELECT {
            @Override
            public TableSpec apply(TableSpec spec, String[] formulas) {
                return spec.select(formulas);
            }
        },
        LAZY_UPDATE {
            @Override
            public TableSpec apply(TableSpec spec, String[] formulas) {
                return spec.lazyUpdate(formulas);
            }
        },
    }

    private final UpdateOrSelect method;

    public UpdateOrSelectSessionTest(UpdateOrSelect method) {
        this.method = Objects.requireNonNull(method);
    }


    @Test
    public void allowStaticI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "X = i");
    }

    @Test
    public void allowStaticII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "X = ii");
    }

    @Test
    public void allowTimeTableI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "X = i");
    }

    @Test
    public void allowTimeTableII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "X = ii");
    }

    @Test
    public void allowSpecificFunctions() throws TableHandle.TableHandleException, InterruptedException {
        // This test isn't meant to be exhaustive
        allow(TableSpec.empty(1).view("Seconds=(long)1659095381"), "Nanos = secondsToNanos(Seconds)");
    }

    @Test
    public void disallowCustomFunctions() throws InterruptedException {
        disallow(TableSpec.empty(1), String.format("X = %s.myFunction()", UpdateOrSelectSessionTest.class.getName()));
    }

    @Test
    public void disallowNew() throws InterruptedException {
        disallow(TableSpec.empty(1), "X = new Object()");
    }

    @Test
    public void allowPreviousColumnName() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1).view("X = 12"), "X");
    }

    @Test
    public void allowPreviousColumnAssignment() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1).view("X = 12"), "Y = X");
    }

    @Test
    public void allowPreviousColumnAssignmentInline() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1), "X = 12", "Y = X");
    }

    @Test
    public void disallowFutureColumn() throws InterruptedException {
        disallow(TableSpec.empty(1), "Y = X", "X = 12");
    }

    @Test
    public void allowReassignmentColumn() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1), "X = 12", "Y = X", "X = 42");
    }

    @Test
    public void disallowNonExistentColumn() throws InterruptedException {
        disallow(TableSpec.empty(1), "Y = Z");
    }

    @Test
    public void allowValue() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1), "X = 12");
    }


    private void allow(TableSpec parent, String... formulas)
            throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle = session.batch().execute(method.apply(parent, formulas))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec parent, String... formulas) throws InterruptedException {
        try (final TableHandle handle = session.batch().execute(method.apply(parent, formulas))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

