/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
using System.Runtime.InteropServices;

namespace Deephaven.CppClientInterop.Native;

internal class DllLocations {
  public const string Dhclient = "Dhclient.dll";
  public const string Dhcore= "Dhcore.dll";
}

internal class Utf16String {
  [UnmanagedFunctionPointer(CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
  public delegate void AllocatorHelper(
    [In, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)] string[] inItems,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)] string[] outItems,
    int count);

  [DllImport(DllLocations.Dhcore, CharSet = CharSet.Unicode)]
  public static extern void deephaven_dhcore_interop_PlatformUtf16_register_allocator_helper(AllocatorHelper allocator);
}

public class WrappedException {
  [DllImport(DllLocations.Dhcore, CharSet = CharSet.Unicode)]
  public static extern string deephaven_client_WrappedException_What(NativePtr<WrappedException> self);
  [DllImport(DllLocations.Dhcore, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_WrappedException_dtor(NativePtr<WrappedException> self);
}

internal class ClientOptions {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_ctor(
    out NativePtr<ClientOptions> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_dtor(NativePtr<ClientOptions> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetDefaultAuthentication(NativePtr<ClientOptions> self,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetBasicAuthentication(NativePtr<ClientOptions> self,
    string username, string password, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetCustomAuthentication(NativePtr<ClientOptions> self,
    string authentication_key, string authentication_value, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetSessionType(NativePtr<ClientOptions> self,
    string session_type, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetUseTls(NativePtr<ClientOptions> self,
    bool use_tls, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetTlsRootCerts(NativePtr<ClientOptions> self,
    string tls_root_certs, out ErrorStatus status);
    
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetClientCertChain(NativePtr<ClientOptions> self,
    string client_cert_chain, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_SetClientPrivateKey(NativePtr<ClientOptions> self,
    string client_private_key, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_AddIntOption(NativePtr<ClientOptions> self,
    string opt, Int32 val, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_AddStringOption(NativePtr<ClientOptions> self,
    string opt, string val, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientOptions_AddExtraHeader(NativePtr<ClientOptions> self,
    string header_name, string header_value, out ErrorStatus status);
}

internal class TableHandleManager {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_dtor(NativePtr<TableHandleManager> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_EmptyTable(NativePtr<TableHandleManager> self,
    Int64 size, out NativePtr<TableHandle> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_FetchTable(NativePtr<TableHandleManager> self,
    string tableName, out NativePtr<TableHandle> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_TimeTable(NativePtr<TableHandleManager> self,
    NativePtr<DurationSpecifier> period, NativePtr<TimePointSpecifier> start_time,
    bool blink_table, out NativePtr<TableHandle> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_InputTable(NativePtr<TableHandleManager> self,
    NativePtr<TableHandle> initial_table, NativePtr<Todo> key_columns,
    Int64 num_key_columns, out NativePtr<TableHandle> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandleManager_RunScript(NativePtr<TableHandleManager> self,
    string code, out ErrorStatus errorStatus);
}

internal class Client {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_Client_Connect(string target,
    NativePtr<ClientOptions> options,
    out NativePtr<Client> result,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_Client_dtor(NativePtr<Client> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_Client_Close(NativePtr<Client> self,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_Client_GetManager(NativePtr<Client> self,
    out NativePtr<TableHandleManager> result,
    out ErrorStatus status);
}

internal class TableHandle {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_dtor(NativePtr<TableHandle> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_GetManager(NativePtr<TableHandle> self,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_Select(NativePtr<TableHandle> self,
    NativePtr<Todo> column_specs, Int64 num_column_specs, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_View(NativePtr<TableHandle> self,
    NativePtr<Todo> column_specs, Int64 num_column_specs, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_DropColumns(NativePtr<TableHandle> self,
    NativePtr<Todo> column_specs, Int64 num_column_specs, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_Update(NativePtr<TableHandle> self,
    [In] string[] columns, Int32 numColumns, out NativePtr<TableHandle> result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_BindToVariable(NativePtr<TableHandle> self,
    string variable, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode, CallingConvention = CallingConvention.StdCall)]
  public static extern void deephaven_client_TableHandle_ToString(NativePtr<TableHandle> self,
    Int32 wantHeaders, out string result, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_ToArrowTable(NativePtr<TableHandle> self,
    out NativePtr<Native.ArrowTable> arrowTable, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_ToClientTable(NativePtr<TableHandle> self,
    out NativePtr<Native.ClientTable> clientTable, out ErrorStatus status);

  [UnmanagedFunctionPointer(CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
  public delegate void NativeOnUpdate(NativePtr<Native.TickingUpdate> tickingUpdate);

  [UnmanagedFunctionPointer(CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
  public delegate void NativeOnFailure(string error);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_Subscribe(NativePtr<TableHandle> self,
    NativeOnUpdate nativeOnUpdate, NativeOnFailure nativeOnFailure,
    out NativePtr<Native.SubscriptionHandle> nativeSubscriptionHandle, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TableHandle_Unsubscribe(NativePtr<TableHandle> self,
    NativePtr<Native.SubscriptionHandle> nativeSubscriptionHandle, out ErrorStatus status);
}

internal class TickingUpdate {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TickingUpdate_dtor(NativePtr<Native.TickingUpdate> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_TickingUpdate_Current(NativePtr<Native.TickingUpdate> self,
    out NativePtr<Native.ClientTable> result, out ErrorStatus status);
}

internal class SubscriptionHandle {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_SubscriptionHandle_dtor(NativePtr<SubscriptionHandle> self);
}

internal class ArrowTable {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_dtor(NativePtr<Native.ArrowTable> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetDimensions(
    NativePtr<Native.ArrowTable> self, out Int32 numColumns, out Int64 numRows, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetSchema(
    NativePtr<Native.ArrowTable> self, Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] string[] columns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] Int32[] columnTypes,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetCharColumn(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] char[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetInt8Column(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] SByte[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetInt16Column(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int16[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetInt32Column(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int32[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetInt64Column(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int64[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetFloatColumn(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] float[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetDoubleColumn(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] double[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetBooleanAsInt32Column(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetStringColumn(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] string[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ArrowTable_GetDateTimeAsLongColumn(
    NativePtr<Native.ArrowTable> self,
    Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int64[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? nullFlags,
    Int64 numRows,
    out ErrorStatus status);
}

internal class ClientTable {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTable_dtor(NativePtr<Native.ClientTable> self);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTable_GetDimensions(
    NativePtr<Native.ClientTable> self, out Int32 numColumns, out Int64 numWRows, out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTable_Schema(
    NativePtr<Native.ClientTable> self, Int32 numColumns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] string[] columns,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] Int32[] columnTypes,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetCharColumn(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] char[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetInt8Column(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] SByte[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetInt16Column(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int16[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetInt32Column(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int32[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetInt64Column(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int64[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetFloatColumn(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] float[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetDoubleColumn(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] double[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetBooleanAsInt32Column(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[] data, // Note! Windows default marshaling for bool is int32
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetStringColumn(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] string[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);

  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_ClientTableHelper_GetDateTimeAsLongColumn(
    NativePtr<Native.ClientTable> self,
    Int32 columnIndex,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] Int64[] data,
    [Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 4)] bool[]? optionalDestNullFlags,
    Int64 numRows,
    out ErrorStatus status);
}

internal class DurationSpecifier {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_DurationSpecifier_ctor_nanos(Int64 nanos,
    out NativePtr<DurationSpecifier> result, out ErrorStatus status);
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_DurationSpecifier_ctor_duration(string duration,
    out NativePtr<DurationSpecifier> result, out ErrorStatus status);
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_DurationSpecifier_dtor(NativePtr<DurationSpecifier> self);
}

internal class TimePointSpecifier {
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_TimePointSpecifier_ctor_nanos(Int64 nanos,
    out NativePtr<TimePointSpecifier> result, out ErrorStatus status);
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_TimePointSpecifier_ctor_duration(string duration,
    out NativePtr<TimePointSpecifier> result, out ErrorStatus status);
  [DllImport(DllLocations.Dhclient, CharSet = CharSet.Unicode)]
  public static extern void deephaven_client_utility_TimePointSpecifier_dtor(NativePtr<TimePointSpecifier> self);
}

// TODO(kosak)
public class Todo {

}
