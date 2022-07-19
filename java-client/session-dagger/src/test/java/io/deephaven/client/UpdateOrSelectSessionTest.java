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
        return () -> Arrays.stream(UpdateOrSelect.values()).map(u -> new Object[] { u }).iterator();
    }

    enum UpdateOrSelect implements BiFunction<TableSpec, String, TableSpec> {
        VIEW {
            @Override
            public TableSpec apply(TableSpec spec, String formula) {
                return spec.view(formula);
            }
        },
        UPDATE_VIEW {
            @Override
            public TableSpec apply(TableSpec spec, String formula) {
                return spec.updateView(formula);
            }
        },
        UPDATE {
            @Override
            public TableSpec apply(TableSpec spec, String formula) {
                return spec.update(formula);
            }
        },
        SELECT {
            @Override
            public TableSpec apply(TableSpec spec, String formula) {
                return spec.select(formula);
            }
        }
        // TODO Lazy via TableSpec
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
    public void disallowTickingI() throws InterruptedException {
        disallow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "Y = i");
    }

    @Test
    public void disallowTickingII() throws InterruptedException {
        disallow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "Y = ii");
    }

    @Test
    public void disallowCustomFunctions() throws InterruptedException {
        disallow(TableSpec.empty(1), String.format("X = %s.myFunction()", UpdateOrSelectSessionTest.class.getName()));
    }

    @Test
    public void disallowNew() throws InterruptedException {
        disallow(TableSpec.empty(1), "X = new Object()");
    }

    private void allow(TableSpec parent, String formula) throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle = session.batch().execute(method.apply(parent, formula))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec parent, String formula) throws InterruptedException {
        try (final TableHandle handle = session.batch().execute(method.apply(parent, formula))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

