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

using arrow::flight::FlightClient;
using deephaven::client::impl::MoveVectorData;
using deephaven::dhcore::utility::Bit_cast;
using deephaven::dhcore::utility::GetWhat;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;
using io::deephaven::proto::backplane::grpc::AddTableRequest;
using io::deephaven::proto::backplane::grpc::AddTableResponse;
using io::deephaven::proto::backplane::grpc::AjRajTablesRequest;
using io::deephaven::proto::backplane::grpc::AuthenticationConstantsRequest;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsRequest;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse;
using io::deephaven::proto::backplane::grpc::ConfigService;
using io::deephaven::proto::backplane::grpc::CreateInputTableRequest;
using io::deephaven::proto::backplane::grpc::CrossJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::DeleteTableRequest;
using io::deephaven::proto::backplane::grpc::DeleteTableResponse;
using io::deephaven::proto::backplane::grpc::DropColumnsRequest;
using io::deephaven::proto::backplane::grpc::EmptyTableRequest;
using io::deephaven::proto::backplane::grpc::ExactJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::ExportedTableCreationResponse;
using io::deephaven::proto::backplane::grpc::FetchTableRequest;
using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailRequest;
using io::deephaven::proto::backplane::grpc::HeadOrTailByRequest;
using io::deephaven::proto::backplane::grpc::LeftJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::MergeTablesRequest;
using io::deephaven::proto::backplane::grpc::NaturalJoinTablesRequest;
using io::deephaven::proto::backplane::grpc::ReleaseRequest;
using io::deephaven::proto::backplane::grpc::ReleaseResponse;
using io::deephaven::proto::backplane::grpc::SelectDistinctRequest;
using io::deephaven::proto::backplane::grpc::SelectOrUpdateRequest;
using io::deephaven::proto::backplane::grpc::SortTableRequest;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::grpc::TimeTableRequest;
using io::deephaven::proto::backplane::grpc::WhereInRequest;
using io::deephaven::proto::backplane::grpc::UpdateByRequest;
using io::deephaven::proto::backplane::grpc::UnstructuredFilterTableRequest;
using io::deephaven::proto::backplane::grpc::UngroupRequest;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandRequest;
using io::deephaven::proto::backplane::script::grpc::ExecuteCommandResponse;
using io::deephaven::proto::backplane::script::grpc::StartConsoleRequest;

using UpdateByOperation = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;

namespace deephaven::client::server {

const char *const Server::kAuthorizationKey = "authorization";

namespace {
std::optional<std::chrono::milliseconds> ExtractExpirationInterval(
    const ConfigurationConstantsResponse &cc_Resp);

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
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
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
        "grpc::Channel(%p) created, "
        "target=%s",
        "Server::CreateFromTarget",
        static_cast<void*>(channel.get()),
        target.c_str());

  auto as = ApplicationService::NewStub(channel);
  auto cs = ConsoleService::NewStub(channel);
  auto ss = SessionService::NewStub(channel);
  auto ts = TableService::NewStub(channel);
  auto cfs = ConfigService::NewStub(channel);
  auto its = InputTableService::NewStub(channel);

  // TODO(kosak): Warn about this string conversion or do something more general.
  auto flightTarget = ((client_options.UseTls()) ? "grpc+tls://" : "grpc://") + target;
  arrow::flight::Location location;

  auto rc1 = arrow::flight::Location::Parse(flightTarget, &location);
  if (!rc1.ok()) {
    auto message = Stringf("Location::Parse(%o) failed, error = %o",
        flightTarget, rc1.ToString());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
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
          "FlightClient(%p) created, "
          "target=%s",
          "Server::CreateFromTarget",
          static_cast<void*>(fc.get()),
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
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    const auto &md = ctx.GetServerInitialMetadata();
    auto ip = md.find(kAuthorizationKey);
    if (ip == md.end()) {
      throw std::runtime_error(
          DEEPHAVEN_LOCATION_STR("Configuration response didn't contain authorization token"));
    }
    sessionToken.assign(ip->second.begin(), ip->second.end());

    // Get expiration interval.
    auto expInt = ExtractExpirationInterval(ccResp);
    if (expInt.has_value()) {
      expirationInterval = *expInt;
    } else {
      expirationInterval = std::chrono::seconds(10);
    }
  }

  auto nextHandshakeTime = sendTime + expirationInterval;

  auto result = std::make_shared<Server>(Private(), std::move(as), std::move(cs),
      std::move(ss), std::move(ts), std::move(cfs), std::move(its), std::move(fc),
      client_options.ExtraHeaders(), std::move(sessionToken), expirationInterval, nextHandshakeTime);
  result->keepAliveThread_ = std::thread(&SendKeepaliveMessages, result);
  gpr_log(GPR_DEBUG,
      "%s: "
      "Server(%p) created, "
      "target=%s",
      "Server::CreateFromTarget",
      (void*) result.get(),
      target.c_str());
  return result;
}

Server::Server(Private,
    std::unique_ptr<ApplicationService::Stub> application_stub,
    std::unique_ptr<ConsoleService::Stub> console_stub,
    std::unique_ptr<SessionService::Stub> session_stub,
    std::unique_ptr<TableService::Stub> table_stub,
    std::unique_ptr<ConfigService::Stub> config_stub,
    std::unique_ptr<InputTableService::Stub> input_table_stub,
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
    input_table_stub_(std::move(input_table_stub)),
    flightClient_(std::move(flight_client)),
    extraHeaders_(std::move(extra_headers)),
    nextFreeTicketId_(1),
    sessionToken_(std::move(session_token)),
    expirationInterval_(expiration_interval),
    nextHandshakeTime_(next_handshake_time) {
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
  auto tickets_to_release = std::move(outstanding_tickets_);
  outstanding_tickets_.clear();
  guard.unlock();

  for (const auto &ticket : tickets_to_release) {
    try {
      ReleaseUnchecked(ticket);
    } catch (...) {
      auto what = GetWhat(std::current_exception());
      gpr_log(GPR_INFO, "Server::Shutdown() is ignoring thrown exception: %s", what.c_str());
    }
  }

  // This will cause the handshake thread to shut down (because cancelled_ is true).
  condVar_.notify_all();

  keepAliveThread_.join();
}

namespace {
Ticket MakeNewTicket(int32_t ticket_id) {
  constexpr auto kTicketSize = sizeof(ticket_id);
  static_assert(kTicketSize == 4, "Unexpected ticket size");
  char buffer[kTicketSize + 1];
  buffer[0] = 'e';
  memcpy(buffer + 1, &ticket_id, kTicketSize);
  Ticket result;
  *result.mutable_ticket() = std::string(buffer, sizeof(buffer));
  return result;
}
}  // namespace

Ticket Server::NewTicket() {
  std::unique_lock guard(mutex_);
  auto ticket_id = nextFreeTicketId_++;
  auto ticket = MakeNewTicket(ticket_id);
  outstanding_tickets_.insert(ticket);
  return ticket;
}

void Server::Release(Ticket ticket) {
  // TODO(kosak): In a future version we might queue up these released tickets and release
  // them asynchronously, in order to give clients a little performance bump without
  // adding too much unexpected asynchronicity to their programs.
  std::unique_lock guard(mutex_);
  if (cancelled_) {
    return;
  }
  if (outstanding_tickets_.erase(ticket) == 0) {
    const char *message = "Server was asked to release a ticket that it is not managing.";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  guard.unlock();
  ReleaseUnchecked(std::move(ticket));
}

void Server::ReleaseUnchecked(Ticket ticket) {
  ReleaseRequest req;
  ReleaseResponse resp;
  *req.mutable_id() = std::move(ticket);
  SendRpc([&](grpc::ClientContext *ctx) {
    return sessionStub_->Release(ctx, req, &resp);
  }, true);  // 'true' to disregard cancellation state.
}

// 'disregard_cancellation_state' is usually false. We set it to true during Shutdown(), so that
// we can release outstanding TableHandles even after the cancelled_ flag is set.
void Server::SendRpc(const std::function<grpc::Status(grpc::ClientContext *)> &callback,
    bool disregard_cancellation_state) {
  using deephaven::dhcore::utility::TimePointToStr;
  auto now = std::chrono::system_clock::now();
  gpr_log(GPR_DEBUG,
      "Server(%p): "
      "Sending RPC "
      "at time %s.",
      static_cast<void *>(this),
      TimePointToStr(now).c_str());

  grpc::ClientContext ctx;
  ForEachHeaderNameAndValue([&ctx](const std::string &name, const std::string &value) {
    ctx.AddMetadata(name, value);
  });

  if (!disregard_cancellation_state) {
    std::unique_lock guard(mutex_);
    if (cancelled_) {
      const char *message = "Server cancelled. All further RPCs are being rejected";
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    guard.unlock();
  }

  auto status = callback(&ctx);
  if (!status.ok()) {
    auto message = Stringf("Error %o. Message: %o", status.error_code(), status.error_message());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
}

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
  ConfigurationConstantsRequest req;
  ConfigurationConstantsResponse resp;
  try {
    SendRpc([&](grpc::ClientContext *ctx) {
      return configStub_->GetConfigurationConstants(ctx, req, &resp);
    });
  } catch (...) {
    gpr_log(GPR_ERROR, "%s: Keepalive failed.", me().c_str());
    return true;
  }

  auto exp_int = ExtractExpirationInterval(resp);
  if (exp_int.has_value()) {
    SetExpirationInterval(*exp_int);
  }
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

void Server::ForEachHeaderNameAndValue(
    const std::function<void(const std::string &, const std::string &)> &fun) {
  mutex_.lock();
  auto tokenCopy = sessionToken_;
  mutex_.unlock();
  fun(kAuthorizationKey, tokenCopy);
  for (const auto &header : extraHeaders_) {
    fun(header.first, header.second);
  }
}

namespace {
std::optional<std::chrono::milliseconds> ExtractExpirationInterval(
    const ConfigurationConstantsResponse &cc_resp) {
  auto ip2 = cc_resp.config_values().find(timeoutKey);
  if (ip2 == cc_resp.config_values().end() || !ip2->second.has_string_value()) {
    return {};
  }
  const auto &target_value = ip2->second.string_value();
  uint64_t millis;
  const auto *begin = target_value.data();
  const auto *end = begin + target_value.size();
  auto [ptr, ec] = std::from_chars(begin, end, millis);
  if (ec != std::errc() || ptr != end) {
    auto message = Stringf("Failed to parse %o as an integer", target_value);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  // As a matter of policy we use half of whatever the server tells us is the expiration time.
  return std::chrono::milliseconds(millis / 2);
}
}  // namespace
}  // namespace deephaven::client::server
