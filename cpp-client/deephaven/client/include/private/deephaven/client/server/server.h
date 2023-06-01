/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <chrono>
#include <future>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <arrow/flight/client.h>

#include "deephaven/client/utility/executor.h"
#include "deephaven/dhcore/utility/callbacks.h"
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
  typedef io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest SelectOrUpdateRequest;
  typedef io::deephaven::proto::backplane::grpc::SessionService SessionService;
  typedef io::deephaven::proto::backplane::grpc::SortDescriptor SortDescriptor;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef io::deephaven::proto::backplane::grpc::TableService TableService;
  typedef io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse BindTableToVariableResponse;
  typedef io::deephaven::proto::backplane::script::grpc::ConsoleService ConsoleService;
  typedef io::deephaven::proto::backplane::script::grpc::StartConsoleResponse StartConsoleResponse;
  typedef io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse ExecuteCommandResponse;

  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::dhcore::utility::SFCallback<T>;
  typedef SFCallback<ExportedTableCreationResponse> EtcCallback;

public:
  static std::shared_ptr<Server> createFromTarget(const std::string &target, const std::string &authorizationValue);
  Server(const Server &other) = delete;
  Server &operator=(const Server &other) = delete;
  Server(Private,
      std::unique_ptr<ApplicationService::Stub> applicationStub,
      std::unique_ptr<ConsoleService::Stub> consoleStub,
      std::unique_ptr<SessionService::Stub> sessionStub,
      std::unique_ptr<TableService::Stub> tableStub,
      std::unique_ptr<ConfigService::Stub> configStub,
      std::unique_ptr<arrow::flight::FlightClient> flightClient,
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

  Ticket newTicket();
  std::tuple<Ticket, arrow::flight::FlightDescriptor> newTicketAndFlightDescriptor();

  void getConfigurationConstantsAsync(
      std::shared_ptr<SFCallback<ConfigurationConstantsResponse>> callback);

  void startConsoleAsync(std::string sessionType, std::shared_ptr<SFCallback<StartConsoleResponse>> callback);

  void executeCommandAsync(Ticket consoleId, std::string code,
      std::shared_ptr<SFCallback<ExecuteCommandResponse>> callback);

  Ticket emptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etcCallback);

  //  std::shared_ptr<TableHandle> historicalTableAsync(std::shared_ptr<std::string> nameSpace,
  //      std::shared_ptr<std::string> tableName, std::shared_ptr<ItdCallback> itdCallback);
  //
  //  std::shared_ptr<TableHandle> tempTableAsync(std::shared_ptr<std::vector<std::shared_ptr<ColumnHolder>>> columnHolders,
  //      std::shared_ptr<ItdCallback> itdCallback);

  Ticket timeTableAsync(int64_t startTimeNanos, int64_t periodNanos,
      std::shared_ptr<EtcCallback> etcCallback);
  //
  //  std::shared_ptr<TableHandle> snapshotAsync(std::shared_ptr<TableHandle> leftTableHandle,
  //      std::shared_ptr<TableHandle> rightTableHandle,
  //      bool doInitialSnapshot, std::shared_ptr<std::vector<std::shared_ptr<std::string>>> stampColumns,
  //      std::shared_ptr<ItdCallback> itdCallback);

  Ticket selectAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket updateAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket viewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket updateViewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket dropColumnsAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket whereAsync(Ticket parentTicket, std::string condition,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket sortAsync(Ticket parentTicket, std::vector<SortDescriptor> sortDescriptors,
      std::shared_ptr<EtcCallback> etcCallback);

  //  std::shared_ptr<TableHandle> preemptiveAsync(std::shared_ptr<TableHandle> parentTableHandle,
  //      int32_t sampleIntervalMs, std::shared_ptr<ItdCallback> itdCallback);

  Ticket comboAggregateDescriptorAsync(Ticket parentTicket,
      std::vector<ComboAggregateRequest::Aggregate> aggregates,
      std::vector<std::string> groupByColumns, bool forceCombo,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket headOrTailByAsync(Ticket parentTicket, bool head, int64_t n,
      std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback);

  Ticket headOrTailAsync(Ticket parentTicket,
      bool head, int64_t n, std::shared_ptr<EtcCallback> etcCallback);

  Ticket ungroupAsync(Ticket parentTicket, bool nullFill, std::vector<std::string> groupByColumns,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket mergeAsync(std::vector<Ticket> sourceTickets, std::string keyColumn,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket crossJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket naturalJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket exactJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
      std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
      std::shared_ptr<EtcCallback> etcCallback);

  Ticket asOfJoinAsync(AsOfJoinTablesRequest::MatchRule matchRule, Ticket leftTableTicket,
      Ticket rightTableTicket, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd, std::shared_ptr<EtcCallback> etcCallback);

  void bindToVariableAsync(const Ticket &consoleId, const Ticket &tableId, std::string variable,
      std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback);

  Ticket fetchTableAsync(std::string tableName, std::shared_ptr<EtcCallback> callback);

  template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
  void sendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> responseCallback,
      TStub *stub, const TPtrToMember &pm);

  std::pair<std::string, std::string> getAuthHeader() const;

  // TODO: make this private
  void setExpirationInterval(std::chrono::milliseconds interval);

private:
  typedef std::unique_ptr<::grpc::ClientAsyncResponseReader<ExportedTableCreationResponse>>
  (TableService::Stub::*selectOrUpdateMethod_t)(::grpc::ClientContext *context,
      const SelectOrUpdateRequest &request, ::grpc::CompletionQueue *cq);

  Ticket selectOrUpdateHelper(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, selectOrUpdateMethod_t method);

  void addSessionToken(grpc::ClientContext *ctx);

  static void processCompletionQueueForever(const std::shared_ptr<Server> &self);
  bool processNextCompletionQueueItem();

  static void sendKeepaliveMessages(const std::shared_ptr<Server> &self);
  bool keepaliveHelper();

  std::unique_ptr<ApplicationService::Stub> applicationStub_;
  std::unique_ptr<ConsoleService::Stub> consoleStub_;
  std::unique_ptr<SessionService::Stub> sessionStub_;
  std::unique_ptr<TableService::Stub> tableStub_;
  std::unique_ptr<ConfigService::Stub> configStub_;
  std::unique_ptr<arrow::flight::FlightClient> flightClient_;
  grpc::CompletionQueue completionQueue_;

  std::atomic<int32_t> nextFreeTicketId_;

  std::mutex mutex_;
  std::condition_variable condVar_;
  bool cancelled_ = false;
  std::string sessionToken_;
  std::chrono::milliseconds expirationInterval_;
  std::chrono::system_clock::time_point nextHandshakeTime_;
};

template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
void Server::sendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> responseCallback,
    TStub *stub, const TPtrToMember &pm) {
  auto now = std::chrono::system_clock::now();
  // Keep this in a unique_ptr at first, for cleanup in case addAuthToken throws an exception.
  auto response = std::make_unique<ServerResponseHolder<TResp>>(now, std::move(responseCallback));
  addSessionToken(&response->ctx_);
  auto rpc = (stub->*pm)(&response->ctx_, req, &completionQueue_);
  // It is the responsibility of "processNextCompletionQueueItem" to deallocate the storage pointed
  // to by 'response'.
  auto *rp = response.release();
  rpc->Finish(&rp->response_, &rp->status_, rp);
}
}  // namespace deephaven::client::server
