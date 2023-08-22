/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/proto/ticket.pb.h"
#include "deephaven/proto/ticket.grpc.pb.h"
#include "deephaven/proto/application.pb.h"
#include "deephaven/proto/application.grpc.pb.h"
#include "deephaven/proto/config.pb.h"
#include "deephaven/proto/config.grpc.pb.h"
#include "deephaven/proto/console.pb.h"
#include "deephaven/proto/console.grpc.pb.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven::client::server {
struct CompletionQueueCallback {
public:
  explicit CompletionQueueCallback(std::chrono::system_clock::time_point send_time);
  CompletionQueueCallback(const CompletionQueueCallback &other) = delete;
  CompletionQueueCallback(CompletionQueueCallback &&other) = delete;
  virtual ~CompletionQueueCallback();

  virtual void OnSuccess() = 0;
  virtual void OnFailure(std::exception_ptr eptr) = 0;

  std::chrono::system_clock::time_point sendTime_;
  grpc::ClientContext ctx_;
  grpc::Status status_;
};

template<typename Response>
struct ServerResponseHolder final : public CompletionQueueCallback {
  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;

public:
  ServerResponseHolder(std::chrono::system_clock::time_point send_time,
      std::shared_ptr<SFCallback<Response>> callback) : CompletionQueueCallback(send_time),
      callback_(std::move(callback)) {}

  ~ServerResponseHolder() final = default;

  void OnSuccess() final {
    callback_->OnSuccess(std::move(response_));
  }

  void OnFailure(std::exception_ptr eptr) final {
    callback_->OnFailure(std::move(eptr));
  }

  std::shared_ptr<SFCallback<Response>> callback_;
  Response response_;
};

class Server : public std::enable_shared_from_this<Server> {
  struct Private {
  };

  using ApplicationService = io::deephaven::proto::backplane::grpc::ApplicationService;
  using AsOfJoinTablesRequest = io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest;
  using AuthenticationConstantsResponse = io::deephaven::proto::backplane::grpc::AuthenticationConstantsResponse;
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  using ConfigurationConstantsResponse = io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse;
  using ConfigService = io::deephaven::proto::backplane::grpc::ConfigService;
  using ExportedTableCreationResponse = io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
  using HandshakeResponse = io::deephaven::proto::backplane::grpc::HandshakeResponse;
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
  using DurationSpecifier = deephaven::client::utility::DurationSpecifier;
  using TimePointSpecifier = deephaven::client::utility::TimePointSpecifier;

  using ClientOptions = deephaven::client::ClientOptions;
  using Executor = deephaven::client::utility::Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  using EtcCallback = SFCallback<ExportedTableCreationResponse>;

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

  void GetConfigurationConstantsAsync(
      std::shared_ptr<SFCallback<ConfigurationConstantsResponse>> callback);

  void StartConsoleAsync(std::string session_type, std::shared_ptr<SFCallback<StartConsoleResponse>> callback);

  void ExecuteCommandAsync(Ticket console_id, std::string code,
      std::shared_ptr<SFCallback<ExecuteCommandResponse>> callback);

  void GetExportedTableCreationResponseAsync(Ticket ticket, std::shared_ptr<EtcCallback> callback);

  void EmptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  //  std::shared_ptr<TableHandle> historicalTableAsync(std::shared_ptr<std::string> nameSpace,
  //      std::shared_ptr<std::string> tableName, std::shared_ptr<ItdCallback> itdCallback);
  //
  //  std::shared_ptr<TableHandle> tempTableAsync(std::shared_ptr<std::vector<std::shared_ptr<ColumnHolder>>> columnHolders,
  //      std::shared_ptr<ItdCallback> itdCallback);

  void TimeTableAsync(DurationSpecifier period, TimePointSpecifier start_time, bool blink_table,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);
  //
  //  std::shared_ptr<TableHandle> snapshotAsync(std::shared_ptr<TableHandle> leftTableHandle,
  //      std::shared_ptr<TableHandle> rightTableHandle,
  //      bool doInitialSnapshot, std::shared_ptr<std::vector<std::shared_ptr<std::string>>> stampColumns,
  //      std::shared_ptr<ItdCallback> itdCallback);

  void SelectAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void UpdateAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void ViewAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void UpdateViewAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void DropColumnsAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void WhereAsync(Ticket parent_ticket, std::string condition, std::shared_ptr<EtcCallback> etc_callback,
      Ticket result);

  void SortAsync(Ticket parent_ticket, std::vector<SortDescriptor> sort_descriptors,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  //  std::shared_ptr<TableHandle> preemptiveAsync(std::shared_ptr<TableHandle> parentTableHandle,
  //      int32_t sampleIntervalMs, std::shared_ptr<ItdCallback> itdCallback);

  void ComboAggregateDescriptorAsync(Ticket parent_ticket,
      std::vector<ComboAggregateRequest::Aggregate> aggregates,
      std::vector<std::string> group_by_columns, bool force_combo,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void HeadOrTailByAsync(Ticket parent_ticket, bool head, int64_t n,
      std::vector<std::string> column_specs, std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void HeadOrTailAsync(Ticket parent_ticket, bool head, int64_t n, std::shared_ptr<EtcCallback> etc_callback,
      Ticket result);

  void UngroupAsync(Ticket parent_ticket, bool null_fill, std::vector<std::string> group_by_columns,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void MergeAsync(std::vector<Ticket> source_tickets, std::string key_column,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void CrossJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void NaturalJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void ExactJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
      std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void AsOfJoinAsync(AsOfJoinTablesRequest::MatchRule match_rule, Ticket left_table_ticket,
      Ticket right_table_ticket, std::vector<std::string> columns_to_match,
      std::vector<std::string> columns_to_add, std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void UpdateByAsync(Ticket source, std::vector<UpdateByOperation> operations,
      std::vector<std::string> group_by_columns,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result);

  void BindToVariableAsync(const Ticket &console_id, const Ticket &table_id, std::string variable,
      std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback);

  void ReleaseAsync(Ticket ticket, std::shared_ptr<SFCallback<ReleaseResponse>> callback);

  void FetchTableAsync(std::string table_name, std::shared_ptr<EtcCallback> callback, Ticket result);

  template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
  void SendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> response_callback,
      TStub *stub, const TPtrToMember &pm);

  void ForEachHeaderNameAndValue(std::function<void(const std::string &, const std::string &)> fun);

  // TODO(kosak): make this private
  void SetExpirationInterval(std::chrono::milliseconds interval);

  // Useful as a log line prefix for messages coming from this server.
  const std::string &me() { return me_; }

private:
  static const char *const kAuthorizationKey;
  // A pointer to member of TableService::Stub, taking (context, request, cq) and returning a
  // unique_ptr.
  using selectOrUpdateMethod_t =
      std::unique_ptr<::grpc::ClientAsyncResponseReader<ExportedTableCreationResponse>>(
          TableService::Stub::*)(::grpc::ClientContext *context,
              const SelectOrUpdateRequest &request, ::grpc::CompletionQueue *cq);

  void SelectOrUpdateHelper(Ticket parent_ticket, std::vector<std::string> column_specs,
      std::shared_ptr<EtcCallback> etc_callback, Ticket result, selectOrUpdateMethod_t method);

  static void ProcessCompletionQueueLoop(const std::shared_ptr<Server> &self);
  [[nodiscard]]
  bool ProcessNextCompletionQueueItem();

  static void SendKeepaliveMessages(const std::shared_ptr<Server> &self);
  [[nodiscard]]
  bool KeepaliveHelper();

  const std::string me_;  // useful printable object name for logging
  std::unique_ptr<ApplicationService::Stub> applicationStub_;
  std::unique_ptr<ConsoleService::Stub> consoleStub_;
  std::unique_ptr<SessionService::Stub> sessionStub_;
  std::unique_ptr<TableService::Stub> tableStub_;
  std::unique_ptr<ConfigService::Stub> configStub_;
  std::unique_ptr<arrow::flight::FlightClient> flightClient_;
  const ClientOptions::extra_headers_t extraHeaders_;
  grpc::CompletionQueue completionQueue_;

  std::atomic<int32_t> nextFreeTicketId_;

  std::mutex mutex_;
  std::condition_variable condVar_;
  bool cancelled_ = false;
  std::string sessionToken_;
  std::chrono::milliseconds expirationInterval_;
  std::chrono::system_clock::time_point nextHandshakeTime_;
  std::thread completionQueueThread_;
  std::thread keepAliveThread_;
};

template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
void Server::SendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> response_callback,
    TStub *stub, const TPtrToMember &pm) {
  using deephaven::dhcore::utility::TimePointToStr;
  using deephaven::dhcore::utility::TypeName;
  static const auto kTypeName = TypeName(req);
  auto now = std::chrono::system_clock::now();
  gpr_log(GPR_DEBUG,
      "Server[%p]: "
      "Sending RPC %s "
      "at time %s.",
      static_cast<void*>(this),
      kTypeName.c_str(),
      TimePointToStr(now).c_str());
          
  // Keep this in a unique_ptr at first, in case we leave early due to cancellation or exception.
  auto response = std::make_unique<ServerResponseHolder<TResp>>(now, std::move(response_callback));
  ForEachHeaderNameAndValue([&response](const std::string &name, const std::string &value) {
    response->ctx_.AddMetadata(name, value);
  });

  // Per the GRPC documentation for CompletionQueue::Shutdown(), we must not add items to the CompletionQueue after
  // it has been shut down. So we do a test and enqueue while under lock.
  std::unique_lock guard(mutex_);
  if (!cancelled_) {
    auto rpc = (stub->*pm)(&response->ctx_, req, &completionQueue_);
    // It is the responsibility of "processNextCompletionQueueItem" to deallocate the storage pointed
    // to by 'response'.
    auto *rp = response.release();
    rpc->Finish(&rp->response_, &rp->status_, rp);
    return;
  }

  // If we get here, we are cancelled. So instead of enqueuing the request, we need to signal failure to the callback.
  // This can be done without holding the lock.
  // TODO(kosak): a slight code savings can be achieved if this error code is moved to a non-template context,
  // since it is not dependent on any template arguments.
  guard.unlock();
  const char *message = "Server cancelled. All further RPCs are being rejected";
  auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
  response->OnFailure(std::move(eptr));
}
}  // namespace deephaven::client::server
