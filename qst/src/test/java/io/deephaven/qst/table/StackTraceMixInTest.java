package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class StackTraceMixInTest {

    static <T extends TableOperations<T, T>> T example1(TableCreator<T> creation) {
        return creation.timeTable(Duration.ofSeconds(1));
    }

    static <T extends TableOperations<T, T>> T example2(TableCreator<T> creation) {
        return example1(creation).view("I=i");
    }

    static <T extends TableOperations<T, T>> T example3(TableCreator<T> creation) {
        T t1 = example2(creation);
        T t2 = t1.where("I % 3 == 0").tail(3);
        T t3 = t1.where("I % 5 == 0").tail(5);
        return creation.merge(t2, t3);
    }

    static <T extends TableOperations<T, T>> T example4(TableCreator<T> creation) {
        T t1 = example2(creation);
        T t2 = t1.where("I % 3 == 0").tail(3);
        T t3 = t1.where("I % 5 == 0").tail(5);
        return creation.merge(Arrays.asList(t2, t3));
    }

    StackTraceMixInCreator<TableSpec, TableSpec> mixinCreator = StackTraceMixInCreator.of();

    @Test
    void mixinElementsTrimmed1() {
        // Note: we could further trim this stacktrace by 1 element if we wanted smarter logic, see
        // note in io.deephaven.qst.table.StackTraceMixInCreator.trimElements
        StackTraceMixIn<TableSpec, TableSpec> mixin = example1(mixinCreator);
        assertThat(mixin.elements()[2].getClassName())
                .isEqualTo(io.deephaven.qst.table.StackTraceMixInTest.class.getName());
        assertThat(mixin.elements()[2].getMethodName()).isEqualTo("example1");
        assertThat(mixin.elements()[2].getLineNumber()).isEqualTo(15);
    }

    @Test
    void mixinElementsTrimmed2() {
        // Note: we could further trim this stacktrace by 1 element if we wanted smarter logic, see
        // note in io.deephaven.qst.table.StackTraceMixInCreator.trimElements
        StackTraceMixIn<TableSpec, TableSpec> mixin = example2(mixinCreator);
        assertThat(mixin.elements()[2].getClassName())
                .isEqualTo(io.deephaven.qst.table.StackTraceMixInTest.class.getName());
        assertThat(mixin.elements()[2].getMethodName()).isEqualTo("example2");
        assertThat(mixin.elements()[2].getLineNumber()).isEqualTo(19);
    }

    @Test
    void mixinElementsTrimmed3() {
        // Note: we could further trim this stacktrace by 1 element if we wanted smarter logic, see
        // note in io.deephaven.qst.table.StackTraceMixInCreator.trimElements
        StackTraceMixIn<TableSpec, TableSpec> mixin = example3(mixinCreator);
        assertThat(mixin.elements()[2].getClassName())
                .isEqualTo(io.deephaven.qst.table.StackTraceMixInTest.class.getName());
        assertThat(mixin.elements()[2].getMethodName()).isEqualTo("example3");
        assertThat(mixin.elements()[2].getLineNumber()).isEqualTo(26);
    }

    @Test
    void mixinElementsTrimmed4() {
        StackTraceMixIn<TableSpec, TableSpec> mixin = example4(mixinCreator);
        assertThat(mixin.elements()[1].getClassName())
                .isEqualTo(io.deephaven.qst.table.StackTraceMixInTest.class.getName());
        assertThat(mixin.elements()[1].getMethodName()).isEqualTo("example4");
        assertThat(mixin.elements()[1].getLineNumber()).isEqualTo(33);
    }
}
