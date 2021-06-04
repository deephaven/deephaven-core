package io.deephaven.qst;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import io.deephaven.qst.table.column.type.GenericType;
import org.junit.jupiter.api.Test;

public class GenericTypeTest {

  @Test
  public void noGenericInt() {
      try {
          GenericType.of(int.class);
          failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
      } catch (IllegalArgumentException e) {
          // expected
      }
  }

    @Test
    public void noGenericInteger() {
        try {
            GenericType.of(Integer.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDoublePrimitive() {
        try {
            GenericType.of(double.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericDouble() {
        try {
            GenericType.of(Double.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void noGenericString() {
        try {
            GenericType.of(String.class);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
