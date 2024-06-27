/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include "deephaven/client/client_options.h"
#include "deephaven/dhcore/interop/interop_util.h"

extern "C" {
void deephaven_client_ClientOptions_ctor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self);
void deephaven_client_ClientOptions_SetDefaultAuthentication(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetBasicAuthentication(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *username, const char *password,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetCustomAuthentication(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *authentication_key, const char *authentication_value,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetSessionType(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *session_type,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetUseTls(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    deephaven::dhcore::interop::InteropBool use_tls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetTlsRootCerts(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *tls_root_certs,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetClientCertChain(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *client_cert_chain,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_SetClientPrivateKey(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *client_private_key,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_AddIntOption(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *opt, int32_t val,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_AddStringOption(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *opt, const char *val,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientOptions_AddExtraHeader(
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> self,
    const char *header_name, const char *header_value,
    deephaven::dhcore::interop::ErrorStatus *status);
}  // extern "C"
