//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public static class Utility {
  public static string FriendlyTypeName(Type t) {
    var sw = new StringWriter();
    FriendlyTypeNameRecurse(t, sw);
    return sw.ToString();
  }

  private static void FriendlyTypeNameRecurse(Type t, StringWriter sw) {
    sw.Write(t.Name);
    if (!t.IsGenericType) {
      return;
    }
    sw.Write('<');
    string separator = "";
    foreach (var arg in t.GetGenericArguments()) {
      sw.Write(separator);
      separator = ",";
      FriendlyTypeNameRecurse(arg, sw);
    }
    sw.Write('>');
  }

  public static bool TryConvertToBase52(long value, Span<char> dest) {
    if (value < 0) {
      return false;
    }
    const string digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    var destLength = dest.Length;
    for (var i = 0; i != destLength; ++i) {
      var resIndex = (value % digits.Length).ToIntExact();
      value /= digits.Length;
      dest[i] = digits[resIndex];
    }
    if (value != 0) {
      // Value too large to fit in dest.
      return false;
    }
    dest.Reverse();
    return true;
  }
}
