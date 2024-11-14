/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <grpc/support/log.h>
#include <arrow/flight/client.h>

#include "deephaven/client/client_options.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/utility/misc_types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven_core/proto/ticket.pb.h"
#include "deephaven_core/proto/ticket.grpc.pb.h"
#include "deephaven_core/proto/application.pb.h"
#include "deephaven_core/proto/application.grpc.pb.h"
#include "deephaven_core/proto/config.pb.h"
#include "deephaven_core/proto/config.grpc.pb.h"
#include "deephaven_core/proto/console.pb.h"
#include "deephaven_core/proto/console.grpc.pb.h"
#include "deephaven_core/proto/inputtable.pb.h"
#include "deephaven_core/proto/inputtable.grpc.pb.h"
#include "deephaven_core/proto/session.pb.h"
#include "deephaven_core/proto/session.grpc.pb.h"
#include "deephaven_core/proto/table.pb.h"
#include "deephaven_core/proto/table.grpc.pb.h"

namespace deephaven::client::server {
namespace internal {
struct TicketLess {
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;

  bool operator()(const Ticket &lhs, const Ticket &rhs) const {
    return lhs.ticket() < rhs.ticket();
  }
};

}  // namespace internal

class Server : public std::enable_shared_from_this<Server> {
  struct Private {
  };

  using ApplicationService = io::deephaven::proto::backplane::grpc::ApplicationService;
  using AddTableResponse = io::deephaven::proto::backplane::grpc::AddTableResponse;
  using AsOfJoinTablesRequest = io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest;
  using AuthenticationConstantsResponse = io::deephaven::proto::backplane::grpc::AuthenticationConstantsResponse;
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  using ConfigurationConstantsResponse = io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse;
  using ConfigService = io::deephaven::proto::backplane::grpc::ConfigService;
  using DeleteTableResponse = io::deephaven::proto::backplane::grpc::DeleteTableResponse;
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using HandshakeResponse = io::deephaven::proto::backplane::grpc::HandshakeResponse;
  using InputTableService = io::deephaven::proto::backplane::grpc::InputTableService;
  using ReleaseResponse = io::deephaven::proto::backplane::grpc::ReleaseResponse;
  using SelectOrUpdateRequest = io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
  using SessionService = io::deephaven::proto::backplane::grpc::SessionService;
  using SortDescriptor = io::deephaven::proto::backplane::grpc::SortDescriptor;
  using Ticket = io::deephaven::proto::backplane::grpc::Ticket;
  using TableService = io::deephaven::proto::backplane::grpc::TableService;
  using UpdateByOperation = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;
  using BindTableToVariableResponse = io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
  using ConsoleService = io::deephaven::proto::backplane::script::grpc::ConsoleService;
  using StartConsoleResponse = io::deephaven::proto::backplane::script::grpc::StartConsoleResponse;
  using ExecuteCommandResponse = io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;

  using ClientOptions = deephaven::client::ClientOptions;
  using Executor = deephaven::client::utility::Executor;

public:
  [[nodiscard]]
  static std::shared_ptr<Server> CreateFromTarget(
      const std::string &target,
      const ClientOptions &client_options);
  Server(const Server &other) = delete;
  Server &operator=(const Server &other) = delete;
  Server(Private,
      std::unique_ptr<ApplicationService::Stub> application_stub,
      std::unique_ptr<ConsoleService::Stub> console_stub,
      std::unique_ptr<SessionService::Stub> session_stub,
      std::unique_ptr<TableService::Stub> table_stub,
      std::unique_ptr<ConfigService::Stub> config_stub,
      std::unique_ptr<InputTableService::Stub> input_table_stub,
      std::unique_ptr<arrow::flight::FlightClient> flight_client,
      ClientOptions::extra_headers_t extra_headers,
      std::string session_token,
      std::chrono::milliseconds expiration_interval,
      std::chrono::system_clock::time_point next_handshake_time);
  ~Server();

  [[nodiscard]]
  ApplicationService::Stub *ApplicationStub() const { return applicationStub_.get(); }

  [[nodiscard]]
  ConfigService::Stub *ConfigStub() const { return configStub_.get(); }

  [[nodiscard]]
  ConsoleService::Stub *ConsoleStub() const { return consoleStub_.get(); }

  [[nodiscard]]
  InputTableService::Stub *InputTableStub() const { return input_table_stub_.get(); }

  [[nodiscard]]
  SessionService::Stub *SessionStub() const { return sessionStub_.get(); }

  [[nodiscard]]
  TableService::Stub *TableStub() const { return tableStub_.get(); }

  // TODO(kosak): decide on the multithreaded story here
  [[nodiscard]]
  arrow::flight::FlightClient *FlightClient() const { return flightClient_.get(); }

  void Shutdown();

  /**
   *  Allocates a new Ticket from client-managed namespace.
   */
  [[nodiscard]]
  Ticket NewTicket();

  void Release(Ticket ticket);

  void SendRpc(const std::function<grpc::Status(grpc::ClientContext*)> &callback) {
    SendRpc(callback, false);
  }

  void ForEachHeaderNameAndValue(
      const std::function<void(const std::string &, const std::string &)> &fun);

  // TODO(kosak): make this private
  void SetExpirationInterval(std::chrono::milliseconds interval);

  // Useful as a log line prefix for messages coming from this server.
  const std::string &me() { return me_; }

private:
  void SendRpc(const std::function<grpc::Status(grpc::ClientContext*)> &callback,
      bool disregard_cancellation_state);
  void ReleaseUnchecked(Ticket ticket);

  static const char *const kAuthorizationKey;

  static void SendKeepaliveMessages(const std::shared_ptr<Server> &self);
  [[nodiscard]]
  bool KeepaliveHelper();

  const std::string me_;  // useful printable object name for logging
  std::unique_ptr<ApplicationService::Stub> applicationStub_;
  std::unique_ptr<ConsoleService::Stub> consoleStub_;
  std::unique_ptr<SessionService::Stub> sessionStub_;
  std::unique_ptr<TableService::Stub> tableStub_;
  std::unique_ptr<ConfigService::Stub> configStub_;
  std::unique_ptr<InputTableService::Stub> input_table_stub_;
  std::unique_ptr<arrow::flight::FlightClient> flightClient_;
  const ClientOptions::extra_headers_t extraHeaders_;


  std::mutex mutex_;
  std::condition_variable condVar_;
  int32_t nextFreeTicketId_ = 1;
  bool cancelled_ = false;
  std::set<Ticket, internal::TicketLess> outstanding_tickets_;
  std::string sessionToken_;
  std::chrono::milliseconds expirationInterval_;
  std::chrono::system_clock::time_point nextHandshakeTime_;
  std::thread keepAliveThread_;
};
}  // namespace deephaven::client::server
