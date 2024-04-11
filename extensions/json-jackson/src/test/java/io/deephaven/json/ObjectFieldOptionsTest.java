//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.json.ObjectFieldOptions.RepeatedBehavior;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectFieldOptionsTest {
    @Test
    void basic() {
        final ObjectFieldOptions field = ObjectFieldOptions.of("Foo", IntOptions.standard());
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntOptions.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void caseInsensitiveMatch() {
        final ObjectFieldOptions field = ObjectFieldOptions.builder()
                .name("Foo")
                .options(IntOptions.standard())
                .caseSensitive(false)
                .build();
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntOptions.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isFalse();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void repeatedBehavior() {
        final ObjectFieldOptions field = ObjectFieldOptions.builder()
                .name("Foo")
                .options(IntOptions.standard())
                .repeatedBehavior(RepeatedBehavior.ERROR)
                .build();
        assertThat(field.name()).isEqualTo("Foo");
        assertThat(field.options()).isEqualTo(IntOptions.standard());
        assertThat(field.aliases()).isEmpty();
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void alias() {
        final ObjectFieldOptions field = ObjectFieldOptions.builder()
                .name("SomeName")
                .options(IntOptions.standard())
                .addAliases("someName")
                .build();
        assertThat(field.name()).isEqualTo("SomeName");
        assertThat(field.options()).isEqualTo(IntOptions.standard());
        assertThat(field.aliases()).containsExactly("someName");
        assertThat(field.caseSensitive()).isTrue();
        assertThat(field.repeatedBehavior()).isEqualTo(RepeatedBehavior.ERROR);
    }

    @Test
    void badAliasRepeated() {
        try {
            ObjectFieldOptions.builder()
                    .name("SomeName")
                    .options(IntOptions.standard())
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
            ObjectFieldOptions.builder()
                    .name("SomeName")
                    .options(IntOptions.standard())
                    .addAliases("someName")
                    .caseSensitive(false)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("name and aliases must be non-overlapping");
        }
    }
}
