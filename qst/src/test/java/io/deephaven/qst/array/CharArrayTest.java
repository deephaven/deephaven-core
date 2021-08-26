package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CharArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(CharArray.of('1', null, '3').values()).containsExactly('1', Util.NULL_CHAR, '3');
    }

    @Test
    void rawInRawOut() {
        assertThat(CharArray.ofUnsafe('1', Util.NULL_CHAR, '3').values()).containsExactly('1',
                Util.NULL_CHAR, '3');
    }
}
