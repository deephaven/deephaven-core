//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public static class DeephavenConstants {
  public const sbyte NullByte = sbyte.MinValue;
  public const Int16 NullShort = Int16.MinValue;
  public const Int32 NullInt = Int32.MinValue;
  public const Int64 NullLong = Int64.MinValue;
  public const float NullFloat = -float.MaxValue;
  public const double NullDouble = -double.MaxValue;
  public const char NullChar = Char.MaxValue;
}

public static class DeephavenMetadataConstants {
 public static class Keys {
    public const string Type = "deephaven:type";
    public const string ComponentType = "deephaven:componentType";
  }

  public static class Types {
    public const string Bool = "java.lang.Boolean";
    public const string Char16 = "char";
    public const string Int8 = "byte";
    public const string Int16 = "short";
    public const string Int32 = "int";
    public const string Int64 = "long";
    public const string Float = "float";
    public const string Double = "double";
    public const string String = "java.lang.String";
    public const string DateTime = "java.time.ZonedDateTime";
    public const string LocalDate = "java.time.LocalDate";
    public const string LocalTime = "java.time.LocalTime";
  }
}
