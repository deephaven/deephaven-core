/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/interop/client_options_interop.h"

#include "deephaven/client/client.h"
#include "deephaven/client/client_options.h"
#include "deephaven/dhcore/interop/interop_util.h"

using deephaven::dhcore::interop::ErrorStatus;
using deephaven::dhcore::interop::InteropBool;
using deephaven::dhcore::interop::NativePtr;

using deephaven::client::ClientOptions;

extern "C" {
void deephaven_client_ClientOptions_ctor(NativePtr<ClientOptions> *result,
    ErrorStatus *status) {
  status->Run([=]() {
    result->Reset(new ClientOptions());
  });
}

void deephaven_client_ClientOptions_dtor(NativePtr<ClientOptions> self) {
  delete self;
}

void deephaven_client_ClientOptions_SetDefaultAuthentication(NativePtr<ClientOptions> self,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetDefaultAuthentication();
  });
}

void deephaven_client_ClientOptions_SetBasicAuthentication(NativePtr<ClientOptions> self,
    const char *username, const char *password,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetBasicAuthentication(username, password);
  });
}

void deephaven_client_ClientOptions_SetCustomAuthentication(NativePtr<ClientOptions> self,
    const char *authentication_key, const char *authentication_value,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetCustomAuthentication(authentication_key, authentication_value);
  });
}

void deephaven_client_ClientOptions_SetSessionType(NativePtr<ClientOptions> self,
    const char *session_type,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetSessionType(session_type);
  });
}

void deephaven_client_ClientOptions_SetUseTls(NativePtr<ClientOptions> self,
    InteropBool use_tls,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetUseTls((bool)use_tls);
  });

}

void deephaven_client_ClientOptions_SetTlsRootCerts(NativePtr<ClientOptions> self,
    const char *tls_root_certs,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetTlsRootCerts(tls_root_certs);
  });
}

void deephaven_client_ClientOptions_SetClientCertChain(NativePtr<ClientOptions> self,
    const char *client_cert_chain,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetClientCertChain(client_cert_chain);
  });

}

void deephaven_client_ClientOptions_SetClientPrivateKey(NativePtr<ClientOptions> self,
    const char *client_private_key,
    ErrorStatus *status) {
  status->Run([=]() {
    self->SetClientPrivateKey(client_private_key);
  });

}

void deephaven_client_ClientOptions_AddIntOption(NativePtr<ClientOptions> self,
    const char *opt, int32_t val,
    ErrorStatus *status) {
  status->Run([=]() {
    self->AddIntOption(opt, val);
  });

}

void deephaven_client_ClientOptions_AddStringOption(NativePtr<ClientOptions> self,
    const char *opt, const char *val,
    ErrorStatus *status) {
  status->Run([=]() {
    self->AddStringOption(opt, val);
  });
}

void deephaven_client_ClientOptions_AddExtraHeader(NativePtr<ClientOptions> self,
    const char *header_name, const char *header_value,
    ErrorStatus *status) {
  status->Run([=]() {
    self->AddExtraHeader(header_name, header_value);
  });
}
}  // extern "C"
