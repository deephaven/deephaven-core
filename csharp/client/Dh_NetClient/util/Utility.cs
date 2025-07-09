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
}
