//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Collections;

namespace Deephaven.Dh_NetClient;

public static class ExtensionMethods {
  public static bool IsEmpty(this string s) {
    return s.Length == 0;
  }

  public static int ToIntExact(this long value) {
    return checked((int)value);
  }

  public static int ToIntExact(this ulong value) {
    return checked((int)value);
  }

  public static bool StructurallyEquals(this IStructuralEquatable s1, object s2) {
    return s1.Equals(s2, StructuralComparisons.StructuralEqualityComparer);
  }
}

public static class TaskExtensions {
  private static async Task ForgetAwaited(Task task) {
    await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
  }

  public static void Forget(this Task task) {
    if (!task.IsCompleted || task.IsFaulted) {
      _ = ForgetAwaited(task);
    }
  }
}
