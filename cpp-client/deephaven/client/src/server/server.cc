/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/server/server.h"

#include <charconv>
#include <exception>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <regex>
#include <arrow/flight/client_auth.h>
#include <arrow/flight/client.h>
#include <arrow/flight/client_middleware.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>

#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/proto/config.pb.h"
#include "deephaven/proto/config.grpc.pb.h"
#include "deephaven/proto/console.pb.h"
#include "deephaven/proto/console.grpc.pb.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

using namespace std;
using arrow::flight::FlightClient;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::bit_cast;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;
using io::deephaven::proto::backplane::grpc::AuthenticationConstantsRequest;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsRequest;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse;
using io::deephaven::proto::backplane::grpc::ConfigService;
using io::deephaven::proto::backplane::grpc::CrossJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::DropColumnsRequest;
using io::deephaven::proto::backplane::grpc::EmptyTableRequest;
using io::deephaven::proto::backplane::grpc::ExactJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::FetchTableRequest;
using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailByRequest;
using io::deephaven::proto::backplane::grpc::MergeTablesRequest;
using io::deephaven::proto::backplane::grpc::NaturalJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
using io::deephaven::proto::backplane::grpc::SortTableRequest;
using io::deephaven::proto::backplane::grpc::TimeTableRequest;
using io::deephaven::proto::backplane::grpc::UnstructuredFilterTableRequest;
using io::deephaven::proto::backplane::grpc::UngroupRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;
using io::deephaven::proto::backplane::script::grpc::StartConsoleRequest;

namespace deephaven::client::server {
namespace {
Ticket makeScopeReference(std::string_view tableName);
void moveVectorData(std::vector<std::string> src,
    google::protobuf::RepeatedPtrField<std::string> *dest);

std::optional<std::chrono::milliseconds> extractExpirationInterval(
    const ConfigurationConstantsResponse &ccResp);

const char *authorizationKey = "authorization";
const char *timeoutKey = "http.session.durationMs";

// (Potentially) re-send a handshake this often *until* the server responds to the handshake.
// The server will typically respond quickly so the resend will typically never happen.
const size_t handshakeResendIntervalMillis = 5 * 1000;
}  // namespace

std::shared_ptr<Server> Server::createFromTarget(const std::string &target, const std::string &authorizationValue) {
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto as = ApplicationService::NewStub(channel);
  auto cs = ConsoleService::NewStub(channel);
  auto ss = SessionService::NewStub(channel);
  auto ts = TableService::NewStub(channel);
  auto cfs = ConfigService::NewStub(channel);

  // TODO(kosak): Warn about this string conversion or do something more general.
  auto flightTarget = "grpc://" + target;
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

  std::string sessionToken;
  std::chrono::milliseconds expirationInterval;
  auto sendTime = std::chrono::system_clock::now();
  {
    ConfigurationConstantsRequest ccReq;
    ConfigurationConstantsResponse ccResp;
    grpc::ClientContext ctx;
    ctx.AddMetadata(authorizationKey, authorizationValue);
    auto result = cfs->GetConfigurationConstants(&ctx, ccReq, &ccResp);

    if (!result.ok()) {
      auto message = stringf("Can't get configuration constants. Error %o: %o",
          result.error_code(), result.error_message());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    const auto &md = ctx.GetServerInitialMetadata();
    auto ip = md.find(authorizationKey);
    if (ip == md.end()) {
      throw std::runtime_error(
          DEEPHAVEN_DEBUG_MSG("Configuration response didn't contain authorization token"));
    }
    sessionToken.assign(ip->second.begin(), ip->second.end());

    // Get expiration interval.
    auto expInt = extractExpirationInterval(ccResp);
    if (expInt.has_value()) {
      expirationInterval = *expInt;
    } else {
      expirationInterval = std::chrono::seconds(10);
    }
  }

  auto nextHandshakeTime = sendTime + expirationInterval;

  auto result = std::make_shared<Server>(Private(), std::move(as), std::move(cs),
      std::move(ss), std::move(ts), std::move(cfs), std::move(fc), std::move(sessionToken),
      expirationInterval, nextHandshakeTime);
  std::thread t1(&processCompletionQueueForever, result);
  std::thread t2(&sendKeepaliveMessages, result);
  t1.detach();
  t2.detach();
  return result;
}

Server::Server(Private,
    std::unique_ptr<ApplicationService::Stub> applicationStub,
    std::unique_ptr<ConsoleService::Stub> consoleStub,
    std::unique_ptr<SessionService::Stub> sessionStub,
    std::unique_ptr<TableService::Stub> tableStub,
    std::unique_ptr<ConfigService::Stub> configStub,
    std::unique_ptr<arrow::flight::FlightClient> flightClient,
    std::string sessionToken, std::chrono::milliseconds expirationInterval,
    std::chrono::system_clock::time_point nextHandshakeTime) :
    applicationStub_(std::move(applicationStub)),
    consoleStub_(std::move(consoleStub)),
    sessionStub_(std::move(sessionStub)),
    tableStub_(std::move(tableStub)),
    configStub_(std::move(configStub)),
    flightClient_(std::move(flightClient)),
    nextFreeTicketId_(1),
    sessionToken_(std::move(sessionToken)),
    expirationInterval_(expirationInterval),
    nextHandshakeTime_(nextHandshakeTime) {
}

Server::~Server() = default;

namespace {
Ticket makeNewTicket(int32_t ticketId) {
  constexpr auto ticketSize = sizeof(ticketId);
  static_assert(ticketSize == 4, "Unexpected ticket size");
  char buffer[ticketSize + 1];
  buffer[0] = 'e';
  memcpy(buffer + 1, &ticketId, ticketSize);
  Ticket result;
  *result.mutable_ticket() = std::string(buffer, sizeof(buffer));
  return result;
}
}  // namespace

Ticket Server::newTicket() {
  auto ticketId = nextFreeTicketId_++;
  return makeNewTicket(ticketId);
}

std::tuple<Ticket, arrow::flight::FlightDescriptor> Server::newTicketAndFlightDescriptor() {
  auto ticketId = nextFreeTicketId_++;
  auto ticket = makeNewTicket(ticketId);
  auto fd = arrow::flight::FlightDescriptor::Path({"export", std::to_string(ticketId)});
  return std::make_tuple(std::move(ticket), std::move(fd));
}

void Server::getConfigurationConstantsAsync(
    std::shared_ptr<SFCallback<ConfigurationConstantsResponse>> callback) {
  ConfigurationConstantsRequest req;
  sendRpc(req, std::move(callback), configStub(),
      &ConfigService::Stub::AsyncGetConfigurationConstants);
}

void Server::startConsoleAsync(std::string sessionType, std::shared_ptr<SFCallback<StartConsoleResponse>> callback) {
  auto ticket = newTicket();
  StartConsoleRequest req;
  *req.mutable_result_id() = std::move(ticket);
  *req.mutable_session_type() = std::move(sessionType);
  sendRpc(req, std::move(callback), consoleStub(), &ConsoleService::Stub::AsyncStartConsole);
}

void Server::executeCommandAsync(Ticket consoleId, std::string code,
    std::shared_ptr<SFCallback<ExecuteCommandResponse>> callback) {
  ExecuteCommandRequest req;
  *req.mutable_console_id() = std::move(consoleId);
  *req.mutable_code() = std::move(code);
  sendRpc(req, std::move(callback), consoleStub(), &ConsoleService::Stub::AsyncExecuteCommand);
}

Ticket Server::emptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  EmptyTableRequest req;
  *req.mutable_result_id() = result;
  req.set_size(size);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncEmptyTable);
  return result;
}

Ticket Server::fetchTableAsync(std::string tableName, std::shared_ptr<EtcCallback> callback) {
  auto result = newTicket();
  FetchTableRequest req;
  *req.mutable_source_id()->mutable_ticket() = makeScopeReference(tableName);
  *req.mutable_result_id() = result;
  sendRpc(req, std::move(callback), tableStub(), &TableService::Stub::AsyncFetchTable);
  return result;
}

Ticket Server::timeTableAsync(int64_t startTimeNanos, int64_t periodNanos,
    std::shared_ptr<EtcCallback> callback) {
  auto result = newTicket();
  TimeTableRequest req;
  *req.mutable_result_id() = result;
  req.set_start_time_nanos(startTimeNanos);
  req.set_period_nanos(periodNanos);
  sendRpc(req, std::move(callback), tableStub(), &TableService::Stub::AsyncTimeTable);
  return result;
}

Ticket Server::selectAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs),
      std::move(etcCallback),
      &TableService::Stub::AsyncSelect);
}

Ticket Server::updateAsync(Ticket parentTicket,
    std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs),
      std::move(etcCallback),
      &TableService::Stub::AsyncUpdate);
}

Ticket Server::viewAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs),
      std::move(etcCallback),
      &TableService::Stub::AsyncView);
}

Ticket Server::updateViewAsync(Ticket parentTicket,
    std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  return selectOrUpdateHelper(std::move(parentTicket), std::move(columnSpecs),
      std::move(etcCallback),
      &TableService::Stub::AsyncUpdateView);
}

Ticket Server::selectOrUpdateHelper(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback, selectOrUpdateMethod_t method) {
  auto result = newTicket();
  SelectOrUpdateRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &cs: columnSpecs) {
    *req.mutable_column_specs()->Add() = std::move(cs);
  }
  sendRpc(req, std::move(etcCallback), tableStub(), method);
  return result;
}

Ticket Server::dropColumnsAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  DropColumnsRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  moveVectorData(std::move(columnSpecs), req.mutable_column_names());
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncDropColumns);
  return result;
}

Ticket Server::whereAsync(Ticket parentTicket, std::string condition,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  UnstructuredFilterTableRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  *req.mutable_filters()->Add() = std::move(condition);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncUnstructuredFilter);
  return result;
}

Ticket Server::sortAsync(Ticket parentTicket, std::vector<SortDescriptor> sortDescriptors,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  SortTableRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &sd: sortDescriptors) {
    *req.mutable_sorts()->Add() = std::move(sd);
  }
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncSort);
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
  for (auto &agg: aggregates) {
    *req.mutable_aggregates()->Add() = std::move(agg);
  }
  for (auto &gbc: groupByColumns) {
    *req.mutable_group_by_columns()->Add() = std::move(gbc);
  }
  req.set_force_combo(forceCombo);
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncComboAggregate);
  return result;
}

Ticket Server::headOrTailByAsync(Ticket parentTicket, bool head,
    int64_t n, std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  HeadOrTailByRequest req;
  *req.mutable_result_id() = result;
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_num_rows(n);
  for (auto &cs: columnSpecs) {
    req.mutable_group_by_column_specs()->Add(std::move(cs));
  }
  const auto &which = head ? &TableService::Stub::AsyncHeadBy : &TableService::Stub::AsyncTailBy;
  sendRpc(req, std::move(etcCallback), tableStub(), which);
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
  sendRpc(req, std::move(etcCallback), tableStub(), which);
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
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncUngroup);
  return result;
}

Ticket Server::mergeAsync(std::vector<Ticket> sourceTickets, std::string keyColumn,
    std::shared_ptr<EtcCallback> etcCallback) {
  auto result = newTicket();
  MergeTablesRequest req;
  *req.mutable_result_id() = result;
  for (auto &t: sourceTickets) {
    *req.mutable_source_ids()->Add()->mutable_ticket() = std::move(t);
  }
  req.set_key_column(std::move(keyColumn));
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncMergeTables);
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
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncCrossJoinTables);
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
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncNaturalJoinTables);
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
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncExactJoinTables);
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
  sendRpc(req, std::move(etcCallback), tableStub(), &TableService::Stub::AsyncAsOfJoinTables);
  return result;
}

void
Server::bindToVariableAsync(const Ticket &consoleId, const Ticket &tableId, std::string variable,
    std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback) {
  BindTableToVariableRequest req;
  *req.mutable_console_id() = consoleId;
  req.set_variable_name(std::move(variable));
  *req.mutable_table_id() = tableId;

  sendRpc(req, std::move(callback), consoleStub(), &ConsoleService::Stub::AsyncBindTableToVariable);
}

std::pair<std::string, std::string> Server::getAuthHeader() const {
  return std::make_pair(authorizationKey, sessionToken_);
}

void Server::addSessionToken(grpc::ClientContext *ctx) {
  std::lock_guard guard(mutex_);
  ctx->AddMetadata(authorizationKey, sessionToken_);
}

void Server::processCompletionQueueForever(const std::shared_ptr<Server> &self) {
  while (true) {
    if (!self->processNextCompletionQueueItem()) {
      break;
    }
  }
}

bool Server::processNextCompletionQueueItem() {
  void *tag;
  bool ok;
  auto gotEvent = completionQueue_.Next(&tag, &ok);
  if (!gotEvent) {
    return false;
  }

  try {
    // Destruct/deallocate on the way out.
    std::unique_ptr<CompletionQueueCallback> cqcb(static_cast<CompletionQueueCallback *>(tag));

    if (!ok) {
      auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(
          "Some GRPC network or connection error")));
      cqcb->onFailure(std::move(eptr));
      return true;
    }

    const auto &stat = cqcb->status_;
    if (!stat.ok()) {
      auto message = stringf("Error %o. Message: %o", stat.error_code(), stat.error_message());
      auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
      cqcb->onFailure(std::move(eptr));
      return true;
    }

    // Authorization token and timeout housekeeping
    const auto &metadata = cqcb->ctx_.GetServerInitialMetadata();
    auto ip = metadata.find(authorizationKey);
    {
      std::unique_lock lock(mutex_);
      if (ip != metadata.end()) {
        const auto &val = ip->second;
        sessionToken_.assign(val.begin(), val.end());
      }
      nextHandshakeTime_ = cqcb->sendTime_ + expirationInterval_;
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

namespace {
class KeepAliveCallback final : public SFCallback<ConfigurationConstantsResponse> {
public:
  explicit KeepAliveCallback(std::shared_ptr<Server> server) : server_(std::move(server)) {}

  void onSuccess(ConfigurationConstantsResponse resp) final {
    auto expInt = extractExpirationInterval(resp);
    if (expInt.has_value()) {
      server_->setExpirationInterval(*expInt);
    }
  }

  void onFailure(std::exception_ptr ep) final {
    // TODO
    std::cerr << "Keepalive failed\n";
  }

  std::shared_ptr<Server> server_;
};
}  // namespace

void Server::sendKeepaliveMessages(const std::shared_ptr<Server> &self) {
  while (true) {
    if (!self->keepaliveHelper()) {
      break;
    }
  }
}

bool Server::keepaliveHelper() {
  // Wait for timeout or cancellation
  {
    std::unique_lock guard(mutex_);
    std::chrono::system_clock::time_point now;
    while (true) {
      (void) condVar_.wait_until(guard, nextHandshakeTime_);
      if (cancelled_) {
        return false;
      }
      now = std::chrono::system_clock::now();
      // We can have spurious wakeups and also nextHandshakeTime_ can change while we are waiting.
      // So don't leave the while loop until wall clock time has moved past nextHandshakeTime_.
      if (now >= nextHandshakeTime_) {
        break;
      }
    }

    // Pessimistically set nextHandshakeTime_ to a short interval from now (say about 5 seconds).
    // If there are no responses from the server in the meantime (including no response to the very
    // handshake we are about to send), then we will send another handshake after this interval.
    nextHandshakeTime_ = now + std::chrono::milliseconds(handshakeResendIntervalMillis);
  }

  // Send a 'GetConfigurationConstants' as a handshake. On the way out, we note our local time.
  // When (if) the server responds to this, the nextHandshakeTime_ will be set to that local time
  // plus the expirationInterval_ (which will typically be an interval like 5 minutes).
  auto callback = std::make_shared<KeepAliveCallback>(shared_from_this());
  getConfigurationConstantsAsync(std::move(callback));
  return true;
}

void Server::setExpirationInterval(std::chrono::milliseconds interval) {
  std::unique_lock guard(mutex_);
  expirationInterval_ = interval;

  // In the unlikely event that the server reduces the expirationInterval_ (probably never happens),
  // we need to wake up the keepalive thread so it can assess what to do.
  auto expirationTimeEstimate = std::chrono::system_clock::now() + expirationInterval_;
  if (expirationTimeEstimate < nextHandshakeTime_) {
    nextHandshakeTime_ = expirationTimeEstimate;
    condVar_.notify_all();
  }
}

CompletionQueueCallback::CompletionQueueCallback(std::chrono::system_clock::time_point sendTime) :
    sendTime_(sendTime) {}
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
  for (auto &s: src) {
    dest->Add(std::move(s));
  }
}

std::optional<std::chrono::milliseconds> extractExpirationInterval(
    const ConfigurationConstantsResponse &ccResp) {
  auto ip2 = ccResp.config_values().find(timeoutKey);
  if (ip2 == ccResp.config_values().end() || !ip2->second.has_string_value()) {
    return {};
  }
  const auto &targetValue = ip2->second.string_value();
  uint64_t millis;
  const auto *begin = targetValue.data();
  const auto *end = begin + targetValue.size();
  auto [ptr, ec] = std::from_chars(begin, end, millis);
  if (ec != std::errc() || ptr != end) {
    auto message = stringf("Failed to parse %o as an integer", targetValue);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  // As a matter of policy we use half of whatever the server tells us is the expiration time.
  return std::chrono::milliseconds(millis / 2);
}
}  // namespace
}  // namespace deephaven::client::server
