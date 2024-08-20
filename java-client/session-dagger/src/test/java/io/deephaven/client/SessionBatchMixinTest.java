//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import org.junit.Test;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SessionBatchMixinTest extends DeephavenSessionTestBase {

    @Test
    public void noMixin() throws InterruptedException {
        try (final TableHandle ignored = session
                .batch(false)
                .executeLogic((TableCreationLogic) SessionBatchMixinTest::thisIsTheMethodName)) {
            failBecauseExceptionWasNotThrown(TableHandleException.class);
        } catch (TableHandleException e) {
            assertThat(Stream.of(e.getStackTrace()).map(StackTraceElement::getMethodName))
                    .doesNotContain("thisIsTheMethodName");
        }
    }

    @Test
    public void yesMixin() throws InterruptedException {
        try (final TableHandle ignored = session
                .batch(true)
                .executeLogic((TableCreationLogic) SessionBatchMixinTest::thisIsTheMethodName)) {
            failBecauseExceptionWasNotThrown(TableHandleException.class);
        } catch (TableHandleException e) {
            assertThat(Stream.of(e.getStackTrace()).map(StackTraceElement::getMethodName))
                    .contains("thisIsTheMethodName");
        }
    }

    static <T extends TableOperations<T, T>> T thisIsTheMethodName(TableCreator<T> creation) {
        T t1 = creation.timeTable(Duration.ofSeconds(1)).view("I=i");
        T t2 = t1.where("I % 3 == 0").tail(3);
        T t3 = t1.where("I % 5 == 0").tail(5);
        T t4 = t1.where("This is not a good filter").tail(6);
        return creation.merge(t2, t3, t4);
    }
}
