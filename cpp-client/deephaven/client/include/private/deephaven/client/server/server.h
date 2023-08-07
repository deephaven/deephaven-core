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
  explicit CompletionQueueCallback(std::chrono::system_clock::time_point sendTime);
  CompletionQueueCallback(const CompletionQueueCallback &other) = delete;
  CompletionQueueCallback(CompletionQueueCallback &&other) = delete;
  virtual ~CompletionQueueCallback();

  virtual void onSuccess() = 0;
  virtual void onFailure(std::exception_ptr eptr) = 0;

  std::chrono::system_clock::time_point sendTime_;
  grpc::ClientContext ctx_;
  grpc::Status status_;
};

template<typename Response>
struct ServerResponseHolder final : public CompletionQueueCallback {
  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;

public:
  ServerResponseHolder(std::chrono::system_clock::time_point sendTime,
      std::shared_ptr<SFCallback<Response>> callback) : CompletionQueueCallback(sendTime),
      callback_(std::move(callback)) {}

  ~ServerResponseHolder() final = default;

  void onSuccess() final {
    callback_->onSuccess(std::move(response_));
  }

  void onFailure(std::exception_ptr eptr) final {
    callback_->onFailure(std::move(eptr));
  }

  std::shared_ptr<SFCallback<Response>> callback_;
  Response response_;
};

class Server : public std::enable_shared_from_this<Server> {
  struct Private {
  };

  typedef io::deephaven::proto::backplane::grpc::ApplicationService ApplicationService;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::AuthenticationConstantsResponse AuthenticationConstantsResponse;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse ConfigurationConstantsResponse;
  typedef io::deephaven::proto::backplane::grpc::ConfigService ConfigService;
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::HandshakeResponse HandshakeResponse;
  typedef io::deephaven::proto::backplane::grpc::ReleaseResponse ReleaseResponse;
  typedef io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest SelectOrUpdateRequest;
  typedef io::deephaven::proto::backplane::grpc::SessionService SessionService;
  typedef io::deephaven::proto::backplane::grpc::SortDescriptor SortDescriptor;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef io::deephaven::proto::backplane::grpc::TableService TableService;
  typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation UpdateByOperation;
  typedef io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse BindTableToVariableResponse;
  typedef io::deephaven::proto::backplane::script::grpc::ConsoleService ConsoleService;
  typedef io::deephaven::proto::backplane::script::grpc::StartConsoleResponse StartConsoleResponse;
  typedef io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse ExecuteCommandResponse;

  typedef deephaven::client::ClientOptions ClientOptions;
  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  typedef SFCallback<ExportedTableCreationResponse> EtcCallback;

public:
  static std::shared_ptr<Server> createFromTarget(
      const std::string &target,
      const ClientOptions &client_options);
  Server(const Server &other) = delete;
  Server &operator=(const Server &other) = delete;
  Server(Private,
      std::unique_ptr<ApplicationService::Stub> applicationStub,
      std::unique_ptr<ConsoleService::Stub> consoleStub,
      std::unique_ptr<SessionService::Stub> sessionStub,
      std::unique_ptr<TableService::Stub> tableStub,
      std::unique_ptr<ConfigService::Stub> configStub,
      std::unique_ptr<arrow::flight::FlightClient> flightClient,
      ClientOptions::extra_headers_t extraHeaders,
      std::string sessionToken,
      std::chrono::milliseconds expirationInterval,
      std::chrono::system_clock::time_point nextHandshakeTime);
  ~Server();

  ApplicationService::Stub *applicationStub() const { return applicationStub_.get(); }

  ConfigService::Stub *configStub() const { return configStub_.get(); }

  ConsoleService::Stub *consoleStub() const { return consoleStub_.get(); }

  SessionService::Stub *sessionStub() const { return sessionStub_.get(); }

  TableService::Stub *tableStub() const { return tableStub_.get(); }

  // TODO(kosak): decide on the multithreaded story here
  arrow::flight::FlightClient *flightClient() const { return flightClient_.get(); }

  void shutdown();

  /**
   *  Allocates a new Ticket from client-managed namespace.
   */
  Ticket newTicket();

  void getConfigurationConstantsAsync(
      std::shared_ptr<SFCallback<ConfigurationConstantsResponse>> callback);

  void startConsoleAsync(std::string sessionType, std::shared_ptr<SFCallback<StartConsoleResponse>> callback);

  void executeCommandAsync(Ticket consoleId, std::string code,
      std::shared_ptr<SFCallback<ExecuteCommandResponse>> callback);

  void getExportedTableCreationResponseAsync(Ticket ticket, std::shared_ptr<EtcCallback> callback);

  void emptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  //  std::shared_ptr<TableHandle> historicalTableAsync(std::shared_ptr<std::string> nameSpace,
  //      std::shared_ptr<std::string> tableName, std::shared_ptr<ItdCallback> itdCallback);
  //
  //  std::shared_ptr<TableHandle> tempTableAsync(std::shared_ptr<std::vector<std::shared_ptr<ColumnHolder>>> columnHolders,
  //      std::shared_ptr<ItdCallback> itdCallback);

  void timeTableAsync(int64_t startTimeNanos, int64_t periodNanos, std::shared_ptr<EtcCallback> etcCallback,
      Ticket result);
  //
  //  std::shared_ptr<TableHandle> snapshotAsync(std::shared_ptr<TableHandle> leftTableHandle,
  //      std::shared_ptr<TableHandle> rightTableHandle,
  //      bool doInitialSnapshot, std::shared_ptr<std::vector<std::shared_ptr<std::string>>> stampColumns,
  //      std::shared_ptr<ItdCallback> itdCallback);

  void selectAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void updateAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void viewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void updateViewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void dropColumnsAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void whereAsync(Ticket parentTicket, std::string condition, std::shared_ptr<EtcCallback> etcCallback,
      Ticket result);

  void sortAsync(Ticket parentTicket, std::vector<SortDescriptor> sortDescriptors,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  //  std::shared_ptr<TableHandle> preemptiveAsync(std::shared_ptr<TableHandle> parentTableHandle,
  //      int32_t sampleIntervalMs, std::shared_ptr<ItdCallback> itdCallback);

  void comboAggregateDescriptorAsync(Ticket parentTicket,
      std::vector<ComboAggregateRequest::Aggregate> aggregates,
      std::vector<std::string> groupByColumns, bool forceCombo,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void headOrTailByAsync(Ticket parentTicket, bool head, int64_t n,
      std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void headOrTailAsync(Ticket parentTicket, bool head, int64_t n, std::shared_ptr<EtcCallback> etcCallback,
      Ticket result);

  void ungroupAsync(Ticket parentTicket, bool nullFill, std::vector<std::string> groupByColumns,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void mergeAsync(std::vector<Ticket> sourceTickets, std::string keyColumn,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void crossJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void naturalJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void exactJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void asOfJoinAsync(AsOfJoinTablesRequest::MatchRule matchRule, Ticket leftTableTicket,
      Ticket rightTableTicket, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd, std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void updateByAsync(Ticket source, std::vector<UpdateByOperation> operations,
      std::vector<std::string> groupByColumns,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result);

  void bindToVariableAsync(const Ticket &consoleId, const Ticket &tableId, std::string variable,
      std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback);

  void releaseAsync(Ticket ticket, std::shared_ptr<SFCallback<ReleaseResponse>> callback);

  void fetchTableAsync(std::string tableName, std::shared_ptr<EtcCallback> callback, Ticket result);

  template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
  void sendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> responseCallback,
      TStub *stub, const TPtrToMember &pm);

  void forEachHeaderNameAndValue(std::function<
      void(const std::string &, const std::string &)> fun);

  // TODO: make this private
  void setExpirationInterval(std::chrono::milliseconds interval);

  // Useful as a log line prefix for messages coming from this server.
  const std::string &me() { return me_; }

private:
  static const char *const authorizationKey;
  typedef std::unique_ptr<::grpc::ClientAsyncResponseReader<ExportedTableCreationResponse>>
  (TableService::Stub::*selectOrUpdateMethod_t)(::grpc::ClientContext *context,
      const SelectOrUpdateRequest &request, ::grpc::CompletionQueue *cq);

  void selectOrUpdateHelper(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, Ticket result, selectOrUpdateMethod_t method);

  static void processCompletionQueueLoop(const std::shared_ptr<Server> &self);
  bool processNextCompletionQueueItem();

  static void sendKeepaliveMessages(const std::shared_ptr<Server> &self);
  bool keepaliveHelper();
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
void Server::sendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> responseCallback,
    TStub *stub, const TPtrToMember &pm) {
  using deephaven::dhcore::utility::timePointToStr;
  using deephaven::dhcore::utility::typeName;
  static const auto tName = typeName(req);
  auto now = std::chrono::system_clock::now();
  gpr_log(GPR_DEBUG,
          "Server[%p]: "
          "Sending RPC %s "
          "at time %s.",
          (void*) this,
          tName.c_str(),
          timePointToStr(now).c_str());
          
  // Keep this in a unique_ptr at first, in case we leave early due to cancellation or exception.
  auto response = std::make_unique<ServerResponseHolder<TResp>>(now, std::move(responseCallback));
  forEachHeaderNameAndValue([&response](const std::string &name, const std::string &value) {
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
  response->onFailure(std::move(eptr));
}
}  // namespace deephaven::client::server
