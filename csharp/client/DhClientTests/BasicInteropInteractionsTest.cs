using Deephaven.DeephavenClient.Interop;
using Deephaven.DeephavenClient.Interop.TestApi;

namespace Deephaven.DhClientTests;

public class BasicInteropInteractionsTest {
  [Fact]
  public void TestInAndOutPrimitives() {
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_Add(3, 4, out var result);
    Assert.Equal(7, result);
  }

  [Fact]
  public void TestInAndOutPrimitiveArrays() {
    const int length = 3;
    var a = new Int32[length] { 10, 20, 30 };
    var b = new Int32[length] { -5, 10, -15 };
    var actualResult = new Int32[length];
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddArrays(a, b,
      length, actualResult);
    var expectedResult = new Int32[length] { 5, 30, 15 };
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutBooleans() {
    foreach (var a in new[] { false, true }) {
      foreach (var b in new[] { false, true }) {
        BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_Xor((InteropBool)a,
          (InteropBool)b, out var actualResult);
        var expectedResult = a ^ b;
        Assert.Equal(expectedResult, (bool)actualResult);
      }
    }
  }

  [Fact]
  public void TestInAndOutBooleanArrays() {
    var aList = new List<bool>();
    var bList = new List<bool>();
    var expectedResult = new List<bool>();
    foreach (var a in new[] { false, true }) {
      foreach (var b in new[] { false, true }) {
        aList.Add(a);
        bList.Add(b);
        expectedResult.Add(a ^ b);
      }
    }

    var aArray = aList.Select(e => (InteropBool)e).ToArray();
    var bArray = bList.Select(e => (InteropBool)e).ToArray();
    var size = aArray.Length;
    var actualResultArray = new InteropBool[size];
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_XorArrays(
      aArray, bArray, size, actualResultArray);

    var expectedResultArray = expectedResult.Select(e => (InteropBool)e).ToArray();

    Assert.Equal(expectedResultArray, actualResultArray);
  }

  [Fact]
  public void TestInAndOutStrings() {
    const string a = "Deep🎔";
    const string b = "haven";
    var expectedResult = a + b;
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_Concat(a, b,
      out var resultHandle, out var poolHandle);
    var pool = poolHandle.ExportAndDestroy();
    var actualResult = pool.Get(resultHandle);
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutStringArrays() {
    const int numItems = 30;
    var prefixes = new string[numItems];
    var suffixes = new string[numItems];
    var expectedResult = new string[numItems];

    for (int i = 0; i != numItems; ++i) {
      prefixes[i] = $"Deep[{i}";
      suffixes[i] = $"-🎔-{i}haven]";
      expectedResult[i] = prefixes[i] + suffixes[i];
    }

    var resultHandles = new StringHandle[numItems];
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_ConcatArrays(
      prefixes, suffixes, numItems, resultHandles, out var poolHandle);
    var pool = poolHandle.ExportAndDestroy();
    var actualResult = resultHandles.Select(pool.Get).ToArray();
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutStruct() {
    var a = new BasicStruct(100, 33.25);
    var b = new BasicStruct(12, 8.5);
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStruct(ref a, ref b, out var actualResult);
    var expectedResult = a.Add(b);
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutStructArrays() {
    const Int32 size = 37;
    var a = new BasicStruct[size];
    var b = new BasicStruct[size];
    var expectedResult = new BasicStruct[size];

    for (Int32 i = 0; i != size; ++i) {
      a[i] = new BasicStruct(i, 1234.5 + i);
      b[i] = new BasicStruct(100 + i, 824.3 + i);
      expectedResult[i] = a[i].Add(b[i]);
    }
    var actualResult = new BasicStruct[size];
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStructArrays(a, b, size, actualResult);

    for (Int32 i = 0; i != size; ++i) {
      Assert.Equal(expectedResult[i], actualResult[i]);
    }
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutNestedStruct() {
    var a1 = new BasicStruct(11, 22.22);
    var a2 = new BasicStruct(33, 44.44);
    var a = new NestedStruct(a1, a2);

    var b1 = new BasicStruct(55, 66.66);
    var b2 = new BasicStruct(77, 88.88);
    var b = new NestedStruct(b1, b2);

    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStruct(ref a, ref b, out var actualResult);
    var expectedResult = a.Add(b);
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestInAndOutNestedStructArray() {
    const Int32 size = 52;
    var a = new NestedStruct[size];
    var b = new NestedStruct[size];
    var expectedResult = new NestedStruct[size];

    for (Int32 i = 0; i != size; ++i) {
      var aa = new BasicStruct(i * 100, i * 11.11);
      var ab = new BasicStruct(i + 200, i * 22.22);
      var ba = new BasicStruct(i + 300, i * 33.33);
      var bb = new BasicStruct(i + 400, i * 44.44);
      a[i] = new NestedStruct(aa, ab);
      b[i] = new NestedStruct(ba, bb);
      expectedResult[i] = a[i].Add(b[i]);
    }

    var actualResult = new NestedStruct[size];
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStructArrays(a, b, size, actualResult);
    Assert.Equal(expectedResult, actualResult);
  }

  [Fact]
  public void TestErrorStatus() {
    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_SetErrorIfLessThan(10, 1, out var status1);
    var error1 = status1.GetError();
    Assert.Null(error1);

    BasicInteropInteractions.deephaven_dhcore_interop_testapi_BasicInteropInteractions_SetErrorIfLessThan(1, 10, out var status2);
    var error2 = status2.GetError();
    Assert.NotNull(error2);
  }
}
