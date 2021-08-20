package io.deephaven.qst.type;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import io.deephaven.qst.type.CustomType;
import org.junit.jupiter.api.Test;

public class CustomTypeTest {

    @Test
    public void noGenericInt() {
        try {
            CustomType.of(int.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericInteger() {
        try {
            CustomType.of(Integer.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDoublePrimitive() {
        try {
            CustomType.of(double.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDouble() {
        try {
            CustomType.of(Double.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericString() {
        try {
            CustomType.of(String.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noArrayTypes() {
        try {
            CustomType.of(int[].class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
