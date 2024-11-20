using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Deephaven.ExcelAddInInstaller.CustomActions {
  public class RegistryManager {
    public static bool TryCreate(Action<string> logger, out RegistryManager result, out string failureReason) {
      result = null;
      failureReason = "";

      var regView = Environment.Is64BitOperatingSystem ? RegistryView.Registry64 : RegistryView.Registry32;
      var regBase = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, regView);

      if (!TryOpenSubKey(regBase, RegistryKeys.Bitness.Key, false,
            out var bitnessKey, out failureReason) ||
          !TryOpenSubKey(Registry.CurrentUser, RegistryKeys.OpenEntries.Key, true,
            out var openKey, out failureReason)) {
        return false;
      }

      result = new RegistryManager(bitnessKey, openKey, logger);
      return true;
    }

    private static bool TryOpenSubKey(RegistryKey baseKey, string key, bool writable, out RegistryKey result, out string failureReason) {
      failureReason = "";
      result = baseKey.OpenSubKey(key, writable);
      if (result == null) {
        failureReason = $"Couldn't find registry key {RegistryKeys.Bitness.Key}";
        return false;
      }

      return true;
    }


    public static bool TryMakeAddInEntryFromPath(string path, out string result, out string failureReason) {
      result = "";
      failureReason = "";
      if (path.Contains("\"")) {
        failureReason = "Path contains illegal characters";
        return false;
      }
      result = $"/R \"{path}\"";
      return true;
    }

    private readonly RegistryKey _registryKeyForBitness;
    private readonly RegistryKey _registryKeyForOpen;
    private readonly Action<string> _logger;

    public RegistryManager(RegistryKey registryKeyForBitness, RegistryKey registryKeyForOpen,
      Action<string> logger) {
      _registryKeyForBitness = registryKeyForBitness;
      _registryKeyForOpen = registryKeyForOpen;
      _logger = logger;
    }

    /// <summary>
    /// Determine whether the installed version of Office is the 32-bit version or the 64-bit version.
    /// It's possible for example that the 32-bit version of Office can be installed on a 64-bit OS.
    /// </summary>
    /// <param name="is64Bit"></param>
    /// <param name="failureReason"></param>
    /// <returns></returns>
    public bool TryDetermineBitness(out bool is64Bit, out string failureReason) {
      is64Bit = false;
      failureReason = "";

      var bitnessValue = _registryKeyForBitness.GetValue(RegistryKeys.Bitness.Name);
      if (bitnessValue == null) {
        failureReason = $"Couldn't find entry for {RegistryKeys.Bitness.Name}";
        return false;
      }

      if (bitnessValue.Equals(RegistryKeys.Bitness.Value64)) {
        is64Bit = true;
        return true;
      }

      if (bitnessValue.Equals(RegistryKeys.Bitness.Value32)) {
        is64Bit = false;
        return true;
      }

      failureReason = $"Unexpected bitness value {bitnessValue}";
      return false;
    }

    /// <summary>
    /// The job of this method is to make whatever changes are needed so that the registry ends up in
    /// the desired state. The caller can express one of two desired states:
    /// 1. The caller wants the registry to end up with 0 instances of "addInEntry"
    /// 2. The caller wants the registry to end up with 1 instance of "addInEntry".
    ///
    /// Basically #1 means "delete the key if it's there, otherwise do nothing", and
    /// #2 means "add the key if it's not there, otherwise do nothing". This is true
    /// except for the fact that we also do some clean-up logic... For example if there's
    /// a gap between OPEN\d+ entries, we will close the gap, and if there are multiple
    /// entries for "addInEntry" we will reduce the final state to whatever the caller asked
    /// for (either 0 or 1 entries).
    ///
    /// Briefly if you want to install the addin, you can pass true for 'resultContainsAddInEntry'.
    /// If you want to remove the addin, you can pass false.
    /// </summary>
    /// <param name="addInEntry">The registry value for the OPEN\d+ key. This is normally something like /R "C:\path\to\addin.xll"
    /// including the space and quotation marks</param>
    /// <param name="resultContainsAddInEntry">true if you want the addInEntry present in the final result.
    /// False if you want it absent from the final result</param>
    /// <param name="failureReason">The human-readable reason the operation failed, if the method returns false</param>
    /// <returns>True if the operation succeeded. Otherwise, false</returns>
    public bool TryUpdateAddInKeys(string addInEntry, bool resultContainsAddInEntry, out string failureReason) {
      if (!TryGetOpenEntries(out var currentEntries, out failureReason)) {
        return false;
      }

      var resultMap = new SortedDictionary<int, BeforeAndAfter>();
      foreach (var kvp in currentEntries) {
        resultMap.LookupOrCreate(kvp.Item1).Before = kvp.Item2;
      }

      // The canonicalization step
      var allowOneEntry = resultContainsAddInEntry;
      var destKey = 0;
      foreach (var entry in currentEntries) {
        if (entry.Item2.Equals(addInEntry)) {
          if (!allowOneEntry) {
            continue;
          }

          allowOneEntry = false;
        }

        resultMap.LookupOrCreate(destKey++).After = entry.Item2;
      }

      // If there was no existing entry matching addInEntry, and the
      // caller asked for it, then we still need to add it.
      if (allowOneEntry) {
        resultMap.LookupOrCreate(destKey).After = addInEntry;
      }

      // The commit step
      foreach (var entry in resultMap) {
        var index = entry.Key;
        var ba = entry.Value;
        var valueName = IndexToKey(index);
        if (ba.After == null) {
          _logger($"Delete {valueName}");
          _registryKeyForOpen.DeleteValue(valueName);
          continue;
        }

        if (ba.Before == null) {
          _logger($"Set {valueName}={ba.After}");
          _registryKeyForOpen.SetValue(valueName, ba.After);
          continue;
        }

        if (ba.Before.Equals(ba.After)) {
          _logger($"Leave {valueName} alone: already set to {ba.Before}");
          continue;
        }

        _logger($"Rewrite {valueName} from {ba.Before} to {ba.After}");
        _registryKeyForOpen.SetValue(valueName, ba.After);
      }

      return true;
    }

    private bool TryGetOpenEntries(out List<Tuple<int, string>> entries, out string failureReason) {
      failureReason = "";
      entries = new List<Tuple<int, string>>();

      var entryKeys = _registryKeyForOpen.GetValueNames();
      foreach (var entryKey in entryKeys) {
        var value = _registryKeyForOpen.GetValue(entryKey);
        if (value == null) {
          failureReason = $"Entry is null for value {entryKey}";
          return false;
        }

        if (!TryParseKey(entryKey, out var key)) {
          continue;
        }

        var svalue = value as string;
        if (svalue == null) {
          failureReason = $"Entry is not a string for value {entryKey}";
          return false;
        }

        entries.Add(Tuple.Create(key, svalue));
      }

      return true;
    }

    public static bool TryParseKey(string key, out int index) {
      index = 0;
      var regex = new Regex(@"^OPEN(\d*)$", RegexOptions.Singleline);
      var match = regex.Match(key);
      if (!match.Success) {
        return false;
      }

      var digits = match.Groups[1].Value;
      index = digits.Length > 0 ? int.Parse(digits) : 0;
      return true;
    }

    public static string IndexToKey(int index) {
      return index == 0 ? "OPEN" : "OPEN" + index;
    }

    private class BeforeAndAfter {
      public string Before;
      public string After;
    }
  }
}

static class ExtensionMethods {
  public static V LookupOrCreate<K, V>(this IDictionary<K, V> dict, K key) where V : new() {
    if (!dict.TryGetValue(key, out var value)) {
      value = new V();
      dict[key] = value;
    }
    return value;
  }
}
