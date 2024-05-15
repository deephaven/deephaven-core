/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/server/server.h"

#include <charconv>
#include <exception>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <grpc/support/log.h>
#include <arrow/flight/client_auth.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>

#include "deephaven/client/impl/util.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using arrow::flight::FlightClient;
using deephaven::dhcore::utility::GetWhat;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsRequest;
using io::deephaven::proto::backplane::grpc::ConfigurationConstantsResponse;
using io::deephaven::proto::backplane::grpc::ReleaseRequest;
using io::deephaven::proto::backplane::grpc::ReleaseResponse;
using io::deephaven::proto::backplane::grpc::Ticket;

using UpdateByOperation = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;

namespace deephaven::client::server {

const char *const Server::kAuthorizationKey = "authorization";

namespace {
std::optional<std::chrono::milliseconds> ExtractExpirationInterval(
    const ConfigurationConstantsResponse &cc_resp);

constexpr const char *kTimeoutKey = "http.session.durationMs";

// A handshake resend interval to use as a default if our normal interval calculation
// fails, e.g. due to GRPC errors.
constexpr const auto kHandshakeResendInterval = std::chrono::seconds(5);
}  // namespace

namespace {
std::shared_ptr<grpc::ChannelCredentials> GetCredentials(
      const bool use_tls,
      const std::string &tls_root_certs,
      const std::string &client_root_chain,
      const std::string &client_private_key) {
  if (!use_tls) {
    return grpc::InsecureChannelCredentials();
  }
  grpc::SslCredentialsOptions options;
  if (!tls_root_certs.empty()) {
    options.pem_root_certs = tls_root_certs;
  }
  if (!client_root_chain.empty()) {
    options.pem_cert_chain = client_root_chain;
  }
  if (!client_private_key.empty()) {
    options.pem_private_key = client_private_key;
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

  auto credentials = GetCredentials(
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
  auto flight_target = ((client_options.UseTls()) ? "grpc+tls://" : "grpc://") + target;

  auto location_res = arrow::flight::Location::Parse(flight_target);
  if (!location_res.ok()) {
    auto message = fmt::format("Location::Parse({}) failed, error = {}",
        flight_target, location_res.status().ToString());
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

  auto client_res = arrow::flight::FlightClient::Connect(*location_res, options);
  if (!client_res.ok()) {
    auto message = fmt::format("FlightClient::Connect() failed, error = {}", client_res.status().ToString());
    throw std::runtime_error(message);
  }
  gpr_log(GPR_DEBUG,
          "%s: "
          "FlightClient(%p) created, "
          "target=%s",
          "Server::CreateFromTarget",
          static_cast<void*>(client_res->get()),
          target.c_str());

  std::string session_token;
  std::chrono::milliseconds expiration_interval;
  auto send_time = std::chrono::system_clock::now();
  {
    ConfigurationConstantsRequest cc_req;
    ConfigurationConstantsResponse cc_resp;
    grpc::ClientContext ctx;
    ctx.AddMetadata(kAuthorizationKey, client_options.AuthorizationValue());
    for (const auto &header : client_options.ExtraHeaders()) {
      ctx.AddMetadata(header.first, header.second);
    }

    auto result = cfs->GetConfigurationConstants(&ctx, cc_req, &cc_resp);

    if (!result.ok()) {
      auto message = fmt::format("Can't get configuration constants. Error {}: {}",
          static_cast<int>(result.error_code()), result.error_message());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    const auto &md = ctx.GetServerInitialMetadata();
    auto ip = md.find(kAuthorizationKey);
    if (ip == md.end()) {
      throw std::runtime_error(
          DEEPHAVEN_LOCATION_STR("Configuration response didn't contain authorization token"));
    }
    session_token.assign(ip->second.begin(), ip->second.end());

    // Get expiration interval.
    auto exp_int = ExtractExpirationInterval(cc_resp);
    if (exp_int.has_value()) {
      expiration_interval = *exp_int;
    } else {
      expiration_interval = std::chrono::seconds(10);
    }
  }

  auto next_handshake_time = send_time + expiration_interval;

  auto result = std::make_shared<Server>(Private(), std::move(as), std::move(cs),
      std::move(ss), std::move(ts), std::move(cfs), std::move(its), std::move(*client_res),
      client_options.ExtraHeaders(), std::move(session_token), expiration_interval, next_handshake_time);
  result->keepAliveThread_ = std::thread(&SendKeepaliveMessages, result);
  gpr_log(GPR_DEBUG,
      "%s: "
      "Server(%p) created, "
      "target=%s",
      "Server::CreateFromTarget",
      static_cast<void*>(result.get()),
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
  }

  auto status = callback(&ctx);
  if (!status.ok()) {
    auto message = fmt::format("Error {}. Message: {}", static_cast<int>(status.error_code()),
        status.error_message());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  // Authorization token and timeout housekeeping
  const auto &metadata = ctx.GetServerInitialMetadata();

  auto ip = metadata.find(kAuthorizationKey);
  std::unique_lock lock(mutex_);
  if (ip != metadata.end()) {
    const auto &val = ip->second;
    sessionToken_.assign(val.begin(), val.end());
  }
  nextHandshakeTime_ = now + expirationInterval_;
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
    // Set a default nextHandshakeTime_. This will likely be overwritten by SendRpc, if it succeeds.
    nextHandshakeTime_ = now + kHandshakeResendInterval;
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
  // we might need to update the nextHandshakeTime_.
  auto expiration_time_estimate = std::chrono::system_clock::now() + expirationInterval_;
  if (expiration_time_estimate < nextHandshakeTime_) {
    nextHandshakeTime_ = expiration_time_estimate;
    condVar_.notify_all();
  }
}

void Server::ForEachHeaderNameAndValue(
    const std::function<void(const std::string &, const std::string &)> &fun) {
  mutex_.lock();
  auto token_copy = sessionToken_;
  mutex_.unlock();
  fun(kAuthorizationKey, token_copy);
  for (const auto &header : extraHeaders_) {
    fun(header.first, header.second);
  }
}

namespace {
std::optional<std::chrono::milliseconds> ExtractExpirationInterval(
    const ConfigurationConstantsResponse &cc_resp) {
  auto ip2 = cc_resp.config_values().find(kTimeoutKey);
  if (ip2 == cc_resp.config_values().end() || !ip2->second.has_string_value()) {
    return {};
  }
  const auto &target_value = ip2->second.string_value();
  uint64_t millis;
  const auto *begin = target_value.data();
  const auto *end = begin + target_value.size();
  auto [ptr, ec] = std::from_chars(begin, end, millis);
  if (ec != std::errc() || ptr != end) {
    auto message = fmt::format("Failed to parse {} as an integer", target_value);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  // As a matter of policy we use half of whatever the server tells us is the expiration time.
  return std::chrono::milliseconds(millis / 2);
}
}  // namespace
}  // namespace deephaven::client::server
