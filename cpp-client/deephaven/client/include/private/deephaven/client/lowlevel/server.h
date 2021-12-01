#pragma once

#include <future>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <arrow/flight/client.h>

#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/proto/ticket.pb.h"
#include "deephaven/proto/ticket.grpc.pb.h"
#include "deephaven/proto/application.pb.h"
#include "deephaven/proto/application.grpc.pb.h"
#include "deephaven/proto/console.pb.h"
#include "deephaven/proto/console.grpc.pb.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven {
namespace client {
namespace lowlevel {
struct CompletionQueueCallback {
  typedef deephaven::client::utility::FailureCallback FailureCallback;

public:
  explicit CompletionQueueCallback(std::shared_ptr<FailureCallback> failureCallback);
  CompletionQueueCallback(const CompletionQueueCallback &other) = delete;
  CompletionQueueCallback(CompletionQueueCallback &&other) = delete;
  virtual ~CompletionQueueCallback();

  virtual void onSuccess() = 0;

  std::shared_ptr<FailureCallback> failureCallback_;
  grpc::ClientContext ctx_;
  grpc::Status status_;
};

template<typename Response>
struct ServerResponseHolder final : public CompletionQueueCallback {
  template<typename T>
  using SFCallback = deephaven::client::utility::SFCallback<T>;

public:
  explicit ServerResponseHolder(std::shared_ptr<SFCallback<Response>> callback) :
      CompletionQueueCallback(std::move(callback)) {}

  ~ServerResponseHolder() final = default;

  void onSuccess() final {
    // The type is valid because this is how we set it in the constructor.
    auto *typedCallback = static_cast<SFCallback<Response>*>(failureCallback_.get());
    typedCallback->onSuccess(std::move(response_));
  }

  Response response_;
};

class Server {
  struct Private {};

  typedef io::deephaven::proto::backplane::grpc::ApplicationService ApplicationService;
  typedef io::deephaven::proto::backplane::grpc::AsOfJoinTablesRequest AsOfJoinTablesRequest;
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  typedef io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest SelectOrUpdateRequest;
  typedef io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse ExportedTableCreationResponse;
  typedef io::deephaven::proto::backplane::grpc::HandshakeResponse HandshakeResponse;
  typedef io::deephaven::proto::backplane::grpc::SessionService SessionService;
  typedef io::deephaven::proto::backplane::grpc::SortDescriptor SortDescriptor;
  typedef io::deephaven::proto::backplane::grpc::Ticket Ticket;
  typedef io::deephaven::proto::backplane::grpc::TableService TableService;
  typedef io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse BindTableToVariableResponse;
  typedef io::deephaven::proto::backplane::script::grpc::ConsoleService ConsoleService;
  typedef io::deephaven::proto::backplane::script::grpc::StartConsoleResponse StartConsoleResponse;

  typedef deephaven::client::utility::Executor Executor;

  template<typename T>
  using SFCallback = deephaven::client::utility::SFCallback<T>;
  typedef SFCallback<ExportedTableCreationResponse> EtcCallback;

public:
  static std::shared_ptr<Server> createFromTarget(const std::string &target);
  Server(const Server &other) = delete;
  Server &operator=(const Server &other) = delete;
  Server(Private,
      std::unique_ptr<ApplicationService::Stub> applicationStub,
      std::unique_ptr<ConsoleService::Stub> consoleStub,
      std::unique_ptr<SessionService::Stub> sessionStub,
      std::unique_ptr<TableService::Stub> tableStub,
      std::unique_ptr<arrow::flight::FlightClient> flightClient);
  ~Server();

  ApplicationService::Stub *applicationStub() const { return applicationStub_.get(); }
  ConsoleService::Stub *consoleStub() const { return consoleStub_.get(); }
  SessionService::Stub *sessionStub() const { return sessionStub_.get(); }
  TableService::Stub *tableStub() const { return tableStub_.get(); }
  // TODO(kosak): decide on the multithreaded story here
  arrow::flight::FlightClient *flightClient() const { return flightClient_.get(); }

  Ticket newTicket();
  std::tuple<Ticket, arrow::flight::FlightDescriptor> newTicketAndFlightDescriptor();

  void setAuthentication(std::string metadataHeader, std::string sessionToken);

  void newSessionAsync(std::shared_ptr<SFCallback<HandshakeResponse>> callback);
  void startConsoleAsync(std::shared_ptr<SFCallback<StartConsoleResponse>> callback);

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
      TStub *stub, const TPtrToMember &pm, bool needAuth);

  std::pair<std::string, std::string> makeBlessing() const;

private:
  typedef std::unique_ptr< ::grpc::ClientAsyncResponseReader<ExportedTableCreationResponse>>
    (TableService::Stub:: *selectOrUpdateMethod_t)(::grpc::ClientContext* context, const SelectOrUpdateRequest &request, ::grpc::CompletionQueue* cq);

  Ticket selectOrUpdateHelper(Ticket parentTicket, std::vector<std::string> columnSpecs,
      std::shared_ptr<EtcCallback> etcCallback, selectOrUpdateMethod_t method);

  void addMetadata(grpc::ClientContext *ctx);

  static void processCompletionQueueForever(const std::shared_ptr<Server> &self);
  bool processNextCompletionQueueItem();

  std::unique_ptr<ApplicationService::Stub> applicationStub_;
  std::unique_ptr<ConsoleService::Stub> consoleStub_;
  std::unique_ptr<SessionService::Stub> sessionStub_;
  std::unique_ptr<TableService::Stub> tableStub_;
  std::unique_ptr<arrow::flight::FlightClient> flightClient_;
  grpc::CompletionQueue completionQueue_;

  bool haveAuth_ = false;
  std::string metadataHeader_;
  std::string sessionToken_;

  std::atomic<int32_t> nextFreeTicketId_;

  // We occasionally need a shared pointer to ourself.
  std::weak_ptr<Server> self_;
};

template<typename TReq, typename TResp, typename TStub, typename TPtrToMember>
void Server::sendRpc(const TReq &req, std::shared_ptr<SFCallback<TResp>> responseCallback,
    TStub *stub, const TPtrToMember &pm, bool needAuth) {
  using deephaven::client::utility::streamf;
  // It is the responsibility of "processNextCompletionQueueItem" to deallocate this.
  auto *response = new ServerResponseHolder<TResp>(std::move(responseCallback));
  if (needAuth) {
    addMetadata(&response->ctx_);
  }
  auto rpc = (stub->*pm)(&response->ctx_, req, &completionQueue_);
  rpc->Finish(&response->response_, &response->status_, response);
}
}  // namespace lowlevel
}  // namespace client
}  // namespace deephaven
