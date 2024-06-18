//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.json.ObjectField.RepeatedBehavior;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectFieldTest {
    @Test
    void basic() {
        final ObjectField field = ObjectField.of("Foo", IntValue.standard());
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntValue.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void caseInsensitiveMatch() {
        final ObjectField field = ObjectField.builder()
                .name("Foo")
                .options(IntValue.standard())
                .caseSensitive(false)
                .build();
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntValue.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isFalse();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void repeatedBehavior() {
        final ObjectField field = ObjectField.builder()
                .name("Foo")
                .options(IntValue.standard())
                .repeatedBehavior(RepeatedBehavior.ERROR)
                .build();
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntValue.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void alias() {
        final ObjectField field = ObjectField.builder()
                .name("SomeName")
                .options(IntValue.standard())
                .addAliases("someName")
                .build();
        assertThat(field.name()).isEqualTo("SomeName");
        assertThat(field.options()).isEqualTo(IntValue.standard());
        assertThat(field.aliases()).containsExactly("someName");
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void badAliasRepeated() {
        try {
            ObjectField.builder()
                    .name("SomeName")
                    .options(IntValue.standard())
                    .addAliases("SomeName")
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("name and aliases must be non-overlapping");
        }
    }

    @Test
    void badAliasCaseInsensitive() {
        try {
            // this is similar to the alias() test, but we are explicitly marking it as case-insensitive
            ObjectField.builder()
                    .name("SomeName")
                    .options(IntValue.standard())
                    .addAliases("someName")
                    .caseSensitive(false)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("name and aliases must be non-overlapping");
        }
    }
}
