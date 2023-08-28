/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/server/server.h"

#include <charconv>
#include <exception>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <grpc/support/log.h>
#include <arrow/flight/client_auth.h>
#include <arrow/flight/client.h>
#include <arrow/flight/client_middleware.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>

#include "deephaven/client/impl/util.h"
#include "deephaven/dhcore/utility/utility.h"

using namespace std;
using arrow::flight::FlightClient;
using deephaven::client::impl::MoveVectorData;
using deephaven::dhcore::utility::SFCallback;
using deephaven::dhcore::utility::Bit_cast;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;
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
using io::deephaven::proto::backplane::grpc::ReleaseRequest;
using io::deephaven::proto::backplane::grpc::ReleaseResponse;
using io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
using io::deephaven::proto::backplane::grpc::SortTableRequest;
using io::deephaven::proto::backplane::grpc::TimeTableRequest;
using io::deephaven::proto::backplane::grpc::UpdateByRequest;
using io::deephaven::proto::backplane::grpc::UnstructuredFilterTableRequest;
using io::deephaven::proto::backplane::grpc::UngroupRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;
using io::deephaven::proto::backplane::script::grpc::StartConsoleRequest;

typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation UpdateByOperation;

namespace deephaven::client::server {

const char *const Server::kAuthorizationKey = "authorization";

namespace {
Ticket makeScopeReference(std::string_view tableName);

std::optional<std::chrono::milliseconds> extractExpirationInterval(
    const ConfigurationConstantsResponse &ccResp);

const char *timeoutKey = "http.session.durationMs";

// (Potentially) re-send a handshake this often *until* the server responds to the handshake.
// The server will typically respond quickly so the resend will typically never happen.
const size_t handshakeResendIntervalMillis = 5 * 1000;
}  // namespace

namespace {
std::shared_ptr<grpc::ChannelCredentials> getCredentials(
      const bool useTls,
      const std::string &tlsRootCerts,
      const std::string &clientCertChain,
      const std::string &clientPrivateKey) {
  if (!useTls) {
    return grpc::InsecureChannelCredentials();
  }
  grpc::SslCredentialsOptions options;
  if (!tlsRootCerts.empty()) {
    options.pem_root_certs = tlsRootCerts;
  }
  if (!clientCertChain.empty()) {
    options.pem_cert_chain = clientCertChain;
  }
  if (!clientPrivateKey.empty()) {
    options.pem_private_key = clientPrivateKey;
  }
  return grpc::SslCredentials(options);
}
}  // namespace

std::shared_ptr<Server> Server::CreateFromTarget(
      const std::string &target,
      const ClientOptions &client_options) {
  if (!client_options.UseTls() && !client_options.TlsRootCerts().empty()) {
    const char *message = "Server::CreateFromTarget: ClientOptions: UseTls is false but pem provided";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  grpc::ChannelArguments channel_args;
  auto options = arrow::flight::FlightClientOptions::Defaults();
  for (const auto &opt : client_options.IntOptions()) {
    channel_args.SetInt(opt.first, opt.second);
    options.generic_options.emplace_back(opt.first, opt.second);
  }
  for (const auto &opt : client_options.StringOptions()) {
    channel_args.SetString(opt.first, opt.second);
    options.generic_options.emplace_back(opt.first, opt.second);
  }

  auto credentials = getCredentials(
      client_options.UseTls(),
      client_options.TlsRootCerts(),
      client_options.ClientCertChain(),
      client_options.ClientPrivateKey());
  auto channel = grpc::CreateCustomChannel(
      target, 
      credentials,
      channel_args);
  gpr_log(GPR_DEBUG,
        "%s: "
        "grpc::Channel[%p] created, "
        "target=%s",
        "Server::CreateFromTarget",
        (void*) channel.get(),
        target.c_str());

  auto as = ApplicationService::NewStub(channel);
  auto cs = ConsoleService::NewStub(channel);
  auto ss = SessionService::NewStub(channel);
  auto ts = TableService::NewStub(channel);
  auto cfs = ConfigService::NewStub(channel);

  // TODO(kosak): Warn about this string conversion or do something more general.
  auto flightTarget = ((client_options.UseTls()) ? "grpc+tls://" : "grpc://") + target;
  arrow::flight::Location location;

  auto rc1 = arrow::flight::Location::Parse(flightTarget, &location);
  if (!rc1.ok()) {
    auto message = Stringf("Location::Parse(%o) failed, error = %o",
        flightTarget, rc1.ToString());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  if (!client_options.TlsRootCerts().empty()) {
    options.tls_root_certs = client_options.TlsRootCerts();
  }
  if (!client_options.ClientCertChain().empty()) {
    options.cert_chain = client_options.ClientCertChain();
  }
  if (!client_options.ClientPrivateKey().empty()) {
    options.private_key = client_options.ClientPrivateKey();
  }

  std::unique_ptr<arrow::flight::FlightClient> fc;
  auto rc2 = arrow::flight::FlightClient::Connect(location, options, &fc);
  if (!rc2.ok()) {
    auto message = Stringf("FlightClient::Connect() failed, error = %o", rc2.ToString());
    throw std::runtime_error(message);
  }
  gpr_log(GPR_DEBUG,
          "%s: "
          "FlightClient[%p] created, "
          "target=%s",
          "Server::CreateFromTarget",
          (void*) fc.get(),
          target.c_str());

  std::string sessionToken;
  std::chrono::milliseconds expirationInterval;
  auto sendTime = std::chrono::system_clock::now();
  {
    ConfigurationConstantsRequest ccReq;
    ConfigurationConstantsResponse ccResp;
    grpc::ClientContext ctx;
    ctx.AddMetadata(kAuthorizationKey, client_options.AuthorizationValue());
    for (const auto &header : client_options.ExtraHeaders()) {
      ctx.AddMetadata(header.first, header.second);
    }

    auto result = cfs->GetConfigurationConstants(&ctx, ccReq, &ccResp);

    if (!result.ok()) {
      auto message = Stringf("Can't get configuration constants. Error %o: %o",
          result.error_code(), result.error_message());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    const auto &md = ctx.GetServerInitialMetadata();
    auto ip = md.find(kAuthorizationKey);
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
      std::move(ss), std::move(ts), std::move(cfs), std::move(fc), client_options.ExtraHeaders(),
      std::move(sessionToken), expirationInterval, nextHandshakeTime);
  result->completionQueueThread_ = std::thread(&ProcessCompletionQueueLoop, result);
  result->keepAliveThread_ = std::thread(&SendKeepaliveMessages, result);
  return result;
}

Server::Server(Private,
    std::unique_ptr<ApplicationService::Stub> application_stub,
    std::unique_ptr<ConsoleService::Stub> console_stub,
    std::unique_ptr<SessionService::Stub> session_stub,
    std::unique_ptr<TableService::Stub> table_stub,
    std::unique_ptr<ConfigService::Stub> config_stub,
    std::unique_ptr<arrow::flight::FlightClient> flight_client,
    ClientOptions::extra_headers_t extra_headers,
    std::string session_token, std::chrono::milliseconds expiration_interval,
    std::chrono::system_clock::time_point next_handshake_time) :
    me_(deephaven::dhcore::utility::ObjectId(
        "client::server::Server", this)),
    applicationStub_(std::move(application_stub)),
    consoleStub_(std::move(console_stub)),
    sessionStub_(std::move(session_stub)),
    tableStub_(std::move(table_stub)),
    configStub_(std::move(config_stub)),
    flightClient_(std::move(flight_client)),
    extraHeaders_(std::move(extra_headers)),
    nextFreeTicketId_(1),
    sessionToken_(std::move(session_token)),
    expirationInterval_(expiration_interval),
    nextHandshakeTime_(next_handshake_time) {
  gpr_log(GPR_DEBUG, "%s: Created.", me_.c_str());
}

Server::~Server() {
  gpr_log(GPR_DEBUG, "%s: Destroyed.", me_.c_str());
}

void Server::Shutdown() {
  gpr_log(GPR_DEBUG, "%s: Server Shutdown requested.", me_.c_str());

  std::unique_lock<std::mutex> guard(mutex_);
  if (cancelled_) {
    guard.unlock(); // to be nice
    gpr_log(GPR_ERROR, "%s: Already cancelled.", me_.c_str());
    return;
  }
  cancelled_ = true;
  guard.unlock();

  // This will cause the completion queue thread to shut down.
  completionQueue_.Shutdown();
  // This will cause the handshake thread to shut down (because cancelled_ is true).
  condVar_.notify_all();

  completionQueueThread_.join();
  keepAliveThread_.join();
}

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

Ticket Server::NewTicket() {
  auto ticketId = nextFreeTicketId_++;
  return makeNewTicket(ticketId);
}

void Server::GetConfigurationConstantsAsync(
    std::shared_ptr<SFCallback<ConfigurationConstantsResponse>> callback) {
  ConfigurationConstantsRequest req;
  SendRpc(req, std::move(callback), ConfigStub(),
      &ConfigService::Stub::AsyncGetConfigurationConstants);
}

void Server::StartConsoleAsync(std::string session_type, std::shared_ptr<SFCallback<StartConsoleResponse>> callback) {
  auto ticket = NewTicket();
  StartConsoleRequest req;
  *req.mutable_result_id() = std::move(ticket);
  *req.mutable_session_type() = std::move(session_type);
  SendRpc(req, std::move(callback), ConsoleStub(), &ConsoleService::Stub::AsyncStartConsole);
}

void Server::ExecuteCommandAsync(Ticket console_id, std::string code,
    std::shared_ptr<SFCallback<ExecuteCommandResponse>> callback) {
  ExecuteCommandRequest req;
  *req.mutable_console_id() = std::move(console_id);
  *req.mutable_code() = std::move(code);
  SendRpc(req, std::move(callback), ConsoleStub(), &ConsoleService::Stub::AsyncExecuteCommand);
}

void Server::GetExportedTableCreationResponseAsync(Ticket ticket, std::shared_ptr<EtcCallback> callback) {
  SendRpc(ticket, std::move(callback), TableStub(), &TableService::Stub::AsyncGetExportedTableCreationResponse);
}

void Server::EmptyTableAsync(int64_t size, std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  EmptyTableRequest req;
  *req.mutable_result_id() = std::move(result);
  req.set_size(size);
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncEmptyTable);
}

void Server::FetchTableAsync(std::string tableName, std::shared_ptr<EtcCallback> callback, Ticket result) {
  FetchTableRequest req;
  *req.mutable_source_id()->mutable_ticket() = makeScopeReference(tableName);
  *req.mutable_result_id() = std::move(result);
  SendRpc(req, std::move(callback), TableStub(), &TableService::Stub::AsyncFetchTable);
}

void Server::TimeTableAsync(DurationSpecifier period, TimePointSpecifier start_time,
    bool blink_table, std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  struct DurationVisitor {
    void operator()(std::chrono::nanoseconds nsecs) const {
      req->set_period_nanos(nsecs.count());
    }
    void operator()(int64_t nsecs) const {
      req->set_period_nanos(nsecs);
    }
    void operator()(std::string duration_text) const {
      *req->mutable_period_string() = std::move(duration_text);
    }

    TimeTableRequest *req = nullptr;
  };

  struct TimePointVisitor {
    void operator()(std::chrono::system_clock::time_point start) const {
      auto as_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch());
      req->set_start_time_nanos(as_duration.count());
    }
    void operator()(int64_t nsecs) const {
      req->set_start_time_nanos(nsecs);
    }
    void operator()(std::string start_time_text) const {
      *req->mutable_start_time_string() = std::move(start_time_text);
    }

    TimeTableRequest *req = nullptr;
  };

  TimeTableRequest req;
  *req.mutable_result_id() = std::move(result);
  std::visit(DurationVisitor{&req}, std::move(period));
  std::visit(TimePointVisitor{&req}, std::move(start_time));
  req.set_blink_table(blink_table);
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncTimeTable);
}

void Server::SelectAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  SelectOrUpdateHelper(std::move(parent_ticket), std::move(column_specs), std::move(etc_callback), std::move(result),
      &TableService::Stub::AsyncSelect);
}

void Server::UpdateAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  SelectOrUpdateHelper(std::move(parent_ticket), std::move(column_specs), std::move(etc_callback), std::move(result),
      &TableService::Stub::AsyncUpdate);
}

void Server::ViewAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  SelectOrUpdateHelper(std::move(parent_ticket), std::move(column_specs), std::move(etc_callback), std::move(result),
      &TableService::Stub::AsyncView);
}

void Server::UpdateViewAsync(Ticket parent_ticket, std::vector<std::string> column_specs,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  SelectOrUpdateHelper(std::move(parent_ticket), std::move(column_specs), std::move(etc_callback), std::move(result),
      &TableService::Stub::AsyncUpdateView);
}

void Server::SelectOrUpdateHelper(Ticket parent_ticket, std::vector<std::string> column_specs,
    std::shared_ptr<EtcCallback> etcCallback, Ticket result, selectOrUpdateMethod_t method) {
  SelectOrUpdateRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parent_ticket);
  for (auto &cs: column_specs) {
    *req.mutable_column_specs()->Add() = std::move(cs);
  }
  SendRpc(req, std::move(etcCallback), TableStub(), method);
}

void Server::DropColumnsAsync(Ticket parentTicket, std::vector<std::string> columnSpecs,
    std::shared_ptr<EtcCallback> etcCallback, Ticket result) {
  DropColumnsRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  MoveVectorData(std::move(columnSpecs), req.mutable_column_names());
  SendRpc(req, std::move(etcCallback), TableStub(), &TableService::Stub::AsyncDropColumns);
}

void Server::WhereAsync(Ticket parentTicket, std::string condition,std::shared_ptr<EtcCallback> etcCallback,
    Ticket result) {
  UnstructuredFilterTableRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  *req.mutable_filters()->Add() = std::move(condition);
  SendRpc(req, std::move(etcCallback), TableStub(), &TableService::Stub::AsyncUnstructuredFilter);
}

void Server::SortAsync(Ticket parentTicket, std::vector<SortDescriptor> sortDescriptors,
    std::shared_ptr<EtcCallback> etcCallback, Ticket result) {
  SortTableRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &sd: sortDescriptors) {
    *req.mutable_sorts()->Add() = std::move(sd);
  }
  SendRpc(req, std::move(etcCallback), TableStub(), &TableService::Stub::AsyncSort);
}

void Server::ComboAggregateDescriptorAsync(Ticket parentTicket,
    std::vector<ComboAggregateRequest::Aggregate> aggregates,
    std::vector<std::string> groupByColumns, bool forceCombo,
    std::shared_ptr<EtcCallback> etcCallback,
    Ticket result) {

  ComboAggregateRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  for (auto &agg: aggregates) {
    *req.mutable_aggregates()->Add() = std::move(agg);
  }
  for (auto &gbc: groupByColumns) {
    *req.mutable_group_by_columns()->Add() = std::move(gbc);
  }
  req.set_force_combo(forceCombo);
  SendRpc(req, std::move(etcCallback), TableStub(), &TableService::Stub::AsyncComboAggregate);
}

void Server::HeadOrTailByAsync(Ticket parentTicket, bool head,
    int64_t n, std::vector<std::string> columnSpecs, std::shared_ptr<EtcCallback> etcCallback,
    Ticket result) {
  HeadOrTailByRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_num_rows(n);
  for (auto &cs: columnSpecs) {
    req.mutable_group_by_column_specs()->Add(std::move(cs));
  }
  const auto &which = head ? &TableService::Stub::AsyncHeadBy : &TableService::Stub::AsyncTailBy;
  SendRpc(req, std::move(etcCallback), TableStub(), which);
}

void Server::HeadOrTailAsync(Ticket parentTicket, bool head, int64_t n, std::shared_ptr<EtcCallback> etcCallback,
    Ticket result) {
  HeadOrTailRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parentTicket);
  req.set_num_rows(n);
  const auto &which = head ? &TableService::Stub::AsyncHead : &TableService::Stub::AsyncTail;
  SendRpc(req, std::move(etcCallback), TableStub(), which);
}

void Server::UngroupAsync(Ticket parent_ticket, bool null_fill, std::vector<std::string> group_by_columns,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  UngroupRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(parent_ticket);
  req.set_null_fill(null_fill);
  MoveVectorData(std::move(group_by_columns), req.mutable_columns_to_ungroup());
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncUngroup);
}

void Server::MergeAsync(std::vector<Ticket> source_tickets, std::string key_column,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  MergeTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  for (auto &t: source_tickets) {
    *req.mutable_source_ids()->Add()->mutable_ticket() = std::move(t);
  }
  req.set_key_column(std::move(key_column));
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncMergeTables);
}

void Server::CrossJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  CrossJoinTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_left_id()->mutable_ticket() = std::move(left_table_ticket);
  *req.mutable_right_id()->mutable_ticket() = std::move(right_table_ticket);
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncCrossJoinTables);
}

void Server::NaturalJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  NaturalJoinTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_left_id()->mutable_ticket() = std::move(left_table_ticket);
  *req.mutable_right_id()->mutable_ticket() = std::move(right_table_ticket);
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncNaturalJoinTables);
}

void Server::ExactJoinAsync(Ticket left_table_ticket, Ticket right_table_ticket,
    std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add,
    std::shared_ptr<EtcCallback> etc_callback, Ticket result) {
  ExactJoinTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_left_id()->mutable_ticket() = std::move(left_table_ticket);
  *req.mutable_right_id()->mutable_ticket() = std::move(right_table_ticket);
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncExactJoinTables);
}

void Server::AsOfJoinAsync(AsOfJoinTablesRequest::MatchRule match_rule, Ticket left_table_ticket,
    Ticket right_table_ticket, std::vector<std::string> columns_to_match,
    std::vector<std::string> columns_to_add, std::shared_ptr<EtcCallback> etc_callback,
    Ticket result) {
  AsOfJoinTablesRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_left_id()->mutable_ticket() = std::move(left_table_ticket);
  *req.mutable_right_id()->mutable_ticket() = std::move(right_table_ticket);
  MoveVectorData(std::move(columns_to_match), req.mutable_columns_to_match());
  MoveVectorData(std::move(columns_to_add), req.mutable_columns_to_add());
  req.set_as_of_match_rule(match_rule);
  SendRpc(req, std::move(etc_callback), TableStub(), &TableService::Stub::AsyncAsOfJoinTables);
}

void Server::UpdateByAsync(Ticket source, std::vector<UpdateByOperation> operations,
    std::vector<std::string> groupByColumns,
    std::shared_ptr<EtcCallback> etcCallback, Ticket result) {
  UpdateByRequest req;
  *req.mutable_result_id() = std::move(result);
  *req.mutable_source_id()->mutable_ticket() = std::move(source);
  MoveVectorData(std::move(operations), req.mutable_operations());
  MoveVectorData(std::move(groupByColumns), req.mutable_group_by_columns());
  SendRpc(req, std::move(etcCallback), TableStub(), &TableService::Stub::AsyncUpdateBy);
}

void
Server::BindToVariableAsync(const Ticket &consoleId, const Ticket &tableId, std::string variable,
    std::shared_ptr<SFCallback<BindTableToVariableResponse>> callback) {
  BindTableToVariableRequest req;
  *req.mutable_console_id() = consoleId;
  req.set_variable_name(std::move(variable));
  *req.mutable_table_id() = tableId;

  SendRpc(req, std::move(callback), ConsoleStub(), &ConsoleService::Stub::AsyncBindTableToVariable);
}

void Server::ReleaseAsync(Ticket ticket, std::shared_ptr<SFCallback<ReleaseResponse>> callback) {
  ReleaseRequest req;
  *req.mutable_id() = std::move(ticket);
  SendRpc(req, std::move(callback), SessionStub(), &SessionService::Stub::AsyncRelease);
}

void Server::ProcessCompletionQueueLoop(const std::shared_ptr<Server> &self) {
  while (true) {
    if (!self->ProcessNextCompletionQueueItem()) {
      break;
    }
  }
  gpr_log(GPR_INFO, "%s: Process completion queue thread exiting.",
          self->me_.c_str());
}

bool Server::ProcessNextCompletionQueueItem() {
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
      cqcb->OnFailure(std::move(eptr));
      return true;
    }

    const auto &stat = cqcb->status_;
    if (!stat.ok()) {
      auto message = Stringf("Error %o. Message: %o", stat.error_code(), stat.error_message());
      auto eptr = std::make_exception_ptr(std::runtime_error(DEEPHAVEN_DEBUG_MSG(message)));
      cqcb->OnFailure(std::move(eptr));
      return true;
    }

    // Authorization token and timeout housekeeping
    const auto &metadata = cqcb->ctx_.GetServerInitialMetadata();
    auto ip = metadata.find(kAuthorizationKey);
    {
      std::unique_lock lock(mutex_);
      if (ip != metadata.end()) {
        const auto &val = ip->second;
        sessionToken_.assign(val.begin(), val.end());
      }
      nextHandshakeTime_ = cqcb->sendTime_ + expirationInterval_;
    }
    cqcb->OnSuccess();
  } catch (const std::exception &e) {
    gpr_log(GPR_ERROR, "%s: Caught std exception on callback: "
            "'%s', aborting.",
            me_.c_str(),
            e.what());
    return false;
  } catch (...) {
    gpr_log(GPR_ERROR, "%s: Caught exception on callback, aborting.", me_.c_str());
    return false;
  }
  return true;
}

namespace {
class KeepAliveCallback final : public SFCallback<ConfigurationConstantsResponse> {
public:
  explicit KeepAliveCallback(std::shared_ptr<Server> server) : server_(std::move(server)) {}

  void OnSuccess(ConfigurationConstantsResponse resp) final {
    auto expInt = extractExpirationInterval(resp);
    if (expInt.has_value()) {
      server_->SetExpirationInterval(*expInt);
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    gpr_log(GPR_ERROR, "%s: Keepalive failed.", server_->me().c_str());
  }

  std::shared_ptr<Server> server_;
};
}  // namespace

void Server::SendKeepaliveMessages(const std::shared_ptr<Server> &self) {
  while (true) {
    if (!self->KeepaliveHelper()) {
      break;
    }
  }

  gpr_log(GPR_INFO, "%s: Keepalive thread exiting.", self->me_.c_str());
}

bool Server::KeepaliveHelper() {
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
  GetConfigurationConstantsAsync(std::move(callback));
  return true;
}

void Server::SetExpirationInterval(std::chrono::milliseconds interval) {
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

void Server::ForEachHeaderNameAndValue(function<void(const string &, const string &)> fun) {
  mutex_.lock();
  auto tokenCopy = sessionToken_;
  mutex_.unlock();
  fun(kAuthorizationKey, tokenCopy);
  for (const auto &header : extraHeaders_) {
    fun(header.first, header.second);
  }
}

CompletionQueueCallback::CompletionQueueCallback(std::chrono::system_clock::time_point send_time) :
    sendTime_(send_time) {}
CompletionQueueCallback::~CompletionQueueCallback() = default;

namespace {
Ticket makeScopeReference(std::string_view tableName) {
  Ticket result;
  result.mutable_ticket()->reserve(2 + tableName.size());
  result.mutable_ticket()->append("s/");
  result.mutable_ticket()->append(tableName);
  return result;
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
    auto message = Stringf("Failed to parse %o as an integer", targetValue);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  // As a matter of policy we use half of whatever the server tells us is the expiration time.
  return std::chrono::milliseconds(millis / 2);
}
}  // namespace
}  // namespace deephaven::client::server
