package io.deephaven.qst.type;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.qst.type.Type.booleanType;
import static io.deephaven.qst.type.Type.byteType;
import static io.deephaven.qst.type.Type.charType;
import static io.deephaven.qst.type.Type.doubleType;
import static io.deephaven.qst.type.Type.find;
import static io.deephaven.qst.type.Type.floatType;
import static io.deephaven.qst.type.Type.instantType;
import static io.deephaven.qst.type.Type.intType;
import static io.deephaven.qst.type.Type.knownTypes;
import static io.deephaven.qst.type.Type.longType;
import static io.deephaven.qst.type.Type.ofCustom;
import static io.deephaven.qst.type.Type.shortType;
import static io.deephaven.qst.type.Type.stringType;
import static org.assertj.core.api.Assertions.assertThat;

public class TypeTest {

    @Test
    void numberOfStaticTypes() {
        // A reminder that when the number of static types increases, we should
        // add tests in this class for it specifically
        assertThat(knownTypes()).hasSize(10);
    }

    @Test
    void findBooleans() {
        assertThat(find(boolean.class)).isEqualTo(booleanType());
        assertThat(find(Boolean.class)).isEqualTo(booleanType());
    }

    @Test
    void findBytes() {
        assertThat(find(byte.class)).isEqualTo(byteType());
        assertThat(find(Byte.class)).isEqualTo(byteType());
    }

    @Test
    void findChars() {
        assertThat(find(char.class)).isEqualTo(charType());
        assertThat(find(Character.class)).isEqualTo(charType());
    }

    @Test
    void findShorts() {
        assertThat(find(short.class)).isEqualTo(shortType());
        assertThat(find(Short.class)).isEqualTo(shortType());
    }

    @Test
    void findInts() {
        assertThat(find(int.class)).isEqualTo(intType());
        assertThat(find(Integer.class)).isEqualTo(intType());
    }

    @Test
    void findLongs() {
        assertThat(find(long.class)).isEqualTo(longType());
        assertThat(find(Long.class)).isEqualTo(longType());
    }

    @Test
    void findFloats() {
        assertThat(find(float.class)).isEqualTo(floatType());
        assertThat(find(Float.class)).isEqualTo(floatType());
    }

    @Test
    void findDoubles() {
        assertThat(find(double.class)).isEqualTo(doubleType());
        assertThat(find(Double.class)).isEqualTo(doubleType());
    }

    @Test
    void findString() {
        assertThat(find(String.class)).isEqualTo(stringType());
    }

    @Test
    void findInstant() {
        assertThat(find(Instant.class)).isEqualTo(instantType());
    }

    @Test
    void findCustom() {
        assertThat(find(Custom.class)).isEqualTo(ofCustom(Custom.class));
    }

    @Test
    void nonEqualityCheck() {
        final List<Type<?>> staticTypes = knownTypes().collect(Collectors.toList());
        final int L = staticTypes.size();
        for (int i = 0; i < L - 1; ++i) {
            for (int j = i + 1; j < L; ++j) {
                assertThat(staticTypes.get(i)).isNotEqualTo(staticTypes.get(j));
                assertThat(staticTypes.get(j)).isNotEqualTo(staticTypes.get(i));
            }
        }
    }

    interface Custom {

    }
}
