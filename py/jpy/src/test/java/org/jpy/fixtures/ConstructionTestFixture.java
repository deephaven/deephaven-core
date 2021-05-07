package org.jpy.fixtures;

public class ConstructionTestFixture {
  public static ConstructionTestFixture viaStatic(int len) {
    return new ConstructionTestFixture(len);
  }

  private final byte[] array;

  public ConstructionTestFixture(int len) {
    array = new byte[len];
  }
}
