#include "deephaven/client/lowlevel/server.h"

#include <exception>
#include <grpcpp/grpcpp.h>
#include <regex>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>

#include "deephaven/client/utility/utility.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

using namespace std;
using arrow::flight::FlightClient;
using deephaven::client::utility::bit_cast;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using io::deephaven::proto::backplane::grpc::CrossJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::DropColumnsRequest;
using io::deephaven::proto::backplane::grpc::EmptyTableRequest;
using io::deephaven::proto::backplane::grpc::ExactJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::FetchTableRequest;
using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailByRequest;
using io::deephaven::proto::backplane::grpc::LeftJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::MergeTablesRequest;
using io::deephaven::proto::backplane::grpc::NaturalJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
using io::deephaven::proto::backplane::grpc::SortTableRequest;
using io::deephaven::proto::backplane::grpc::TimeTableRequest;
using io::deephaven::proto::backplane::grpc::UnstructuredFilterTableRequest;
using io::deephaven::proto::backplane::grpc::UngroupRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::StartConsoleRequest;

namespace deephaven {
namespace client {
namespace lowlevel {
namespace {
Ticket makeScopeReference(std::string_view tableName);
void moveVectorData(std::vector<std::string> src, google::protobuf::RepeatedPtrField<std::string> *dest);
}  // namespace

std::shared_ptr<Server> Server::createFromTarget(const std::string &target) {
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto as = ApplicationService::NewStub(channel);
  auto cs = ConsoleService::NewStub(channel);
  auto ss = SessionService::NewStub(channel);
  auto ts = TableService::NewStub(channel);

  auto flightTarget = "grpc://" + target;
  streamf(std::cerr, "TODO(kosak): Converting %o to %o for Arrow Flight\n", target, flightTarget);
  arrow::flight::Location location;
  auto rc1 = arrow::flight::Location::Parse(flightTarget, &location);
  if (!rc1.ok()) {
    auto message = stringf("Location::Parse(%o) failed, error = %o", flightTarget, rc1.ToString());
    throw std::runtime_error(message);
  }

  std::unique_ptr<arrow::flight::FlightClient> fc;
  auto rc2 = arrow::flight::FlightClient::Connect(location, &fc);
  if (!rc2.ok()) {
    auto message = stringf("FlightClient::Connect() failed, error = %o", rc2.ToString());
    throw std::runtime_error(message);
  }

  auto result = std::make_shared<Server>(Private(), std::move(as), std::move(cs),
      std::move(ss), std::move(ts), std::move(fc));
  result->self_ = result;

  std::thread t(&processCompletionQueueForever, result);
  t.detach();

  return result;
}

Server::Server(Private,
    std::unique_ptr<ApplicationService::Stub> applicationStub,
    std::unique_ptr<ConsoleService::Stub> consoleStub,
    std::unique_ptr<SessionService::Stub> sessionStub,
    std::unique_ptr<TableService::Stub> tableStub,
    std::unique_ptr<arrow::flight::FlightClient> flightClient) :
    applicationStub_(std::move(applicationStub)),
    consoleStub_(std::move(consoleStub)),
    sessionStub_(std::move(sessionStub)),
    tableStub_(std::move(tableStub)),
    flightClient_(std::move(flightClient)),
    nextFreeTicketId_(1) {}

Server::~Server() = default;

Ticket Server::newTicket() {
  auto ticketId = nextFreeTicketId_++;
  constexpr auto ticketSize = sizeof(ticketId);
  static_assert(ticketSize == 4, "Unexpected ticket size");
  char buffer[ticketSize + 1];
  buffer[0] = 'e';
  memcpy(buffer + 1, &ticketId, ticketSize);
  Ticket result;
  *result.mutable_ticket() = std::string(buffer, sizeof(buffer));
  return result;
}


void Server::setAuthentication(std::string metadataHeader, std::string sessionToken) {
  if (haveAuth_) {
    throw std::runtime_error("Can't reset authentication token");
  }
  haveAuth_ = true;
  metadataHeader_ = std::move(metadataHeader);
  sessionToken_ = std::move(sessionToken);
}

void Server::newSessionAsync(std::shared_ptr<SFCallback<HandshakeResponse>> callback) {
  HandshakeRequest req;
  req.set_auth_protocol(1);
  sendRpc(req, std::move(callback), sessionStub(), &SessionService::Stub::AsyncNewSession,
      false);
}

void Server::startConsoleAsync(std::shared_ptr<SFCallback<StartConsoleResponse>> callback) {
  auto ticket = newTicket();
  StartConsoleRequest req;
  *req.mutable_result_id() = std::move(ticket);
  req.set_session_type("groovy");
  sendRpc(req, std::move(callback), consoleStub(), &ConsoleService::Stub::AsyncStartConsole,
      true);
}

Ticket Server::emptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  EmptyTableRequest req;
  *req.mutable_result_id() = result;
  req.set_size(size);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncEmptyTable, true);
  return result;
}

Ticket Server::fetchTableAsync(std::string tableName, std::shared_ptr<EtcCallback> callback) {
  auto result = newTicket();
  FetchTableRequest req;
  *req.mutable_source_id()->mutable_ticket() = makeScopeReference(tableName);
  *req.mutable_result_id() = result;
  sendRpc(req, std::move(callback), tableStub(), &TableService::Stub::AsyncFetchTable, true);
  return result;
}

Ticket Server::timeTableAsync(int64_t startTimeNanos, int64_t periodNanos,
    std::shared_ptr<EtcCallback> callback) {
  auto result = newTicket();
  TimeTableRequest req;
  *req.mutable_result_id() = result;
  req.set_start_time_nanos(startTimeNanos);
  req.set_period_nanos(periodNanos);
  sendRpc(req, std::move(callback), tableStub(), &TableService::Stub::AsyncTimeTable, true);
  return result;
}

Ticket Server::selectAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs), std::move(etcCallback),
      &TableService::Stub::AsyncSelect);
}

Ticket Server::updateAsync(Ticket parentTicket,
    std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs), std::move(etcCallback),
      &TableService::Stub::AsyncUpdate);
}

Ticket Server::viewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs), std::move(etcCallback),
      &TableService::Stub::AsyncView);
}

Ticket Server::updateViewAsync(Ticket parentTicket,
    std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs), std::move(etcCallback),
      &TableService::Stub::AsyncUpdateView);
}

Ticket Server::selectOrUpdateHelper(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback, selectOrUpdateMethod_t method) {
  auto result = newTicket();
  SelectOrUpdateRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &cs : columnSpecs) {
    *req.mutable_column_specs()->Add() = std::move(cs);
  }
  sendRpc(req, std::move(etcCallback), tableStub(), method, true);
  return result;
}

Ticket Server::dropColumnsAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  DropColumnsRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  moveVectorData(std::move(columnSpecs), req.mutable_column_names());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncDropColumns, true);
  return result;
}

Ticket Server::whereAsync(Ticket parentTicket, std::string condition,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  UnstructuredFilterTableRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  *req.mutable_filters()->Add() = std::move(condition);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncUnstructuredFilter,
      true);
  return result;
}

Ticket Server::sortAsync(Ticket parentTicket, std::vector<SortDescriptor> sortDescriptors,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  SortTableRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &sd : sortDescriptors) {
    *req.mutable_sorts()->Add() = std::move(sd);
  }
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncSort, true);
  return result;
}

Ticket Server::comboAggregateDescriptorAsync(Ticket parentTicket,
    std::vector<ComboAggregateRequest::Aggregate> aggregates,
    std::vector<std::string> groupByColumns, bool forceCombo,
    std::shared_ptr<EtcCallback> etcCallback) {

  auto result = newTicket();
  ComboAggregateRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &agg : aggregates) {
    *req.mutable_aggregates()->Add() = std::move(agg);
  }
  for (auto &gbc : groupByColumns) {
    *req.mutable_group_by_columns()->Add() = std::move(gbc);
  }
  req.set_force_combo(forceCombo);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncComboAggregate, true);
  return result;
}

Ticket Server::headOrTailByAsync(Ticket parentTicket, bool head,
    int64_t n, std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  HeadOrTailByRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_num_rows(n);
  for (auto &cs : columnSpecs) {
    req.mutable_group_by_column_specs()->Add(std::move(cs));
  }
  const auto &which = head ? &TableService::Stub::AsyncHeadBy : &TableService::Stub::AsyncTailBy;
  sendRpc(req, std::move(etcCallback), tableStub(), which, true);
  return result;
}

Ticket Server::headOrTailAsync(Ticket parentTicket,
    bool head, int64_t n, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  HeadOrTailRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_num_rows(n);
  const auto &which = head ? &TableService::Stub::AsyncHead : &TableService::Stub::AsyncTail;
  sendRpc(req, std::move(etcCallback), tableStub(), which, true);
  return result;
}

Ticket Server::ungroupAsync(Ticket parentTicket, bool nullFill,
    std::vector<std::string> groupByColumns, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  UngroupRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_null_fill(nullFill);
  moveVectorData(std::move(groupByColumns), req.mutable_columns_to_ungroup());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncUngroup, true);
  return result;
}

Ticket Server::mergeAsync(std::vector<Ticket> sourceTickets, std::string keyColumn,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  MergeTablesRequest req;
  *req.mutable_result_id() = result;
  for (auto &t : sourceTickets) {
    *req.mutable_source_ids()->Add()->mutable_ticket() = std::move(t);
  }
  req.set_key_column(std::move(keyColumn));
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncMergeTables, true);
  return result;
}

Ticket Server::crossJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  CrossJoinTablesRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_left_id()->mutable_ticket() = std::move(leftTableTicket);
  *req.mutable_right_id()->mutable_ticket() = std::move(rightTableTicket);
  moveVectorData(std::move(columnsToMatch), req.mutable_columns_to_match());
  moveVectorData(std::move(columnsToAdd), req.mutable_columns_to_add());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncCrossJoinTables, true);
  return result;
}

Ticket Server::naturalJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  NaturalJoinTablesRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_left_id()->mutable_ticket() = std::move(leftTableTicket);
  *req.mutable_right_id()->mutable_ticket() = std::move(rightTableTicket);
  moveVectorData(std::move(columnsToMatch), req.mutable_columns_to_match());
  moveVectorData(std::move(columnsToAdd), req.mutable_columns_to_add());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncNaturalJoinTables, true);
  return result;
}

Ticket Server::exactJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  ExactJoinTablesRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_left_id()->mutable_ticket() = std::move(leftTableTicket);
  *req.mutable_right_id()->mutable_ticket() = std::move(rightTableTicket);
  moveVectorData(std::move(columnsToMatch), req.mutable_columns_to_match());
  moveVectorData(std::move(columnsToAdd), req.mutable_columns_to_add());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncExactJoinTables, true);
  return result;
}

Ticket Server::leftJoinAsync(Ticket leftTableTicket, Ticket rightTableTicket,
    std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  LeftJoinTablesRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_left_id()->mutable_ticket() = std::move(leftTableTicket);
  *req.mutable_right_id()->mutable_ticket() = std::move(rightTableTicket);
  moveVectorData(std::move(columnsToMatch), req.mutable_columns_to_match());
  moveVectorData(std::move(columnsToAdd), req.mutable_columns_to_add());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncLeftJoinTables, true);
  return result;
}

Ticket Server::asOfJoinAsync(AsOfJoinTablesRequest::MatchRule matchRule, Ticket leftTableTicket,
    Ticket rightTableTicket, std::vector<std::string> columnsToMatch,
    std::vector<std::string> columnsToAdd, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  AsOfJoinTablesRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_left_id()->mutable_ticket() = std::move(leftTableTicket);
  *req.mutable_right_id()->mutable_ticket() = std::move(rightTableTicket);
  moveVectorData(std::move(columnsToMatch), req.mutable_columns_to_match());
  moveVectorData(std::move(columnsToAdd), req.mutable_columns_to_add());
  req.set_as_of_match_rule(matchRule);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncAsOfJoinTables, true);
  return result;
}

void Server::bindToVariableAsync(const Ticket &consoleId, const Ticket &tableId, std::string variable,
    std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback) {
  BindTableToVariableRequest req;
  *req.mutable_console_id() = consoleId;
  req.set_variable_name(std::move(variable));
  *req.mutable_table_id() = tableId;

  sendRpc(req, std::move(callback), consoleStub(), &ConsoleService::Stub::AsyncBindTableToVariable, true);
}

std::pair<std::string, std::string> Server::makeBlessing() const {
  return std::make_pair(metadataHeader_, sessionToken_);
}

void Server::addMetadata(grpc::ClientContext *ctx) {
  if (!haveAuth_) {
    throw std::runtime_error("Caller needed authorization but I don't have it");
  }
  ctx->AddMetadata(metadataHeader_, sessionToken_);
}

void Server::processCompletionQueueForever(const std::shared_ptr<Server> &self) {
  std::cerr << "Completion queue thread waking up\n";
  while (true) {
    if (!self->processNextCompletionQueueItem()) {
      break;
    }
  }
  std::cerr << "Completion queue thread shutting down\n";
}

bool Server::processNextCompletionQueueItem() {
  void *tag;
  bool ok;
  auto gotEvent = completionQueue_.Next(&tag, &ok);
  // streamf(std::cerr, "gotEvent is %o, tag is %o, ok is %o\n", gotEvent, tag, ok);
  if (!gotEvent) {
    return false;
  }

  try {
    // Destruct/deallocate on the way out.
    std::unique_ptr<CompletionQueueCallback> cqcb(static_cast<CompletionQueueCallback *>(tag));

    if (!ok) {
      cqcb->failureCallback_->onFailure(
          std::make_exception_ptr(std::runtime_error("Some GRPC network or connection error")));
      return true;
    }

    const auto &stat = cqcb->status_;
    if (!stat.ok()) {
      auto message = stringf("Error %o. Message: %o", stat.error_code(), stat.error_message());
      cqcb->failureCallback_->onFailure(std::make_exception_ptr(std::runtime_error(message)));
      return true;
    }

    cqcb->onSuccess();
  } catch (const std::exception &e) {
    std::cerr << "Caught exception on callback, aborting: " << e.what() << "\n";
    return false;
  } catch (...) {
    std::cerr << "Caught exception on callback, aborting\n";
    return false;
  }
  return true;
}

CompletionQueueCallback::CompletionQueueCallback(std::shared_ptr<FailureCallback> failureCallback) :
    failureCallback_(std::move(failureCallback)) {}
CompletionQueueCallback::~CompletionQueueCallback() = default;

namespace {
Ticket makeScopeReference(std::string_view tableName) {
  Ticket result;
  result.mutable_ticket()->reserve(2 + tableName.size());
  result.mutable_ticket()->append("s/");
  result.mutable_ticket()->append(tableName);
  return result;
}

void moveVectorData(std::vector<std::string> src,
    google::protobuf::RepeatedPtrField<std::string> *dest) {
  for (auto &s : src) {
    dest->Add(std::move(s));
  }
}
}  // namespace
}  // namespace lowlevel
}  // namespace client
}  // namespace deephaven
