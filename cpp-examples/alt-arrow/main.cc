/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string_view>
#include <tuple>
#include <vector>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/key_value_metadata.h>
#include <grpcpp/grpcpp.h>
#include "deephaven/proto/console.pb.h"
#include "deephaven/proto/console.grpc.pb.h"
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"
#include "deephaven/proto/ticket.pb.h"
#include "deephaven/proto/ticket.grpc.pb.h"

using io::deephaven::proto::backplane::grpc::HandshakeRequest;
using io::deephaven::proto::backplane::grpc::HandshakeResponse;
using io::deephaven::proto::backplane::grpc::SessionService;
using io::deephaven::proto::backplane::grpc::Ticket;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableRequest;
using io::deephaven::proto::backplane::script::grpc::BindTableToVariableResponse;
using io::deephaven::proto::backplane::script::grpc::ConsoleService;
using io::deephaven::proto::backplane::script::grpc::StartConsoleRequest;
using io::deephaven::proto::backplane::script::grpc::StartConsoleResponse;

// a macro to expand out __FUNCTION__, __FILE__, and __LINE__ and expression to some readable thing
#define DEEPHAVEN_STRINGIFY_HELPER(X) #X
#define DEEPHAVEN_STRINGIFY(X) DEEPHAVEN_STRINGIFY_HELPER(X)
#define DEEPHAVEN_EXPR_MSG(ARGS...) __FILE__ ":" DEEPHAVEN_STRINGIFY(__LINE__), ARGS

namespace {
class Server {
  struct Private {};

public:
  static std::shared_ptr<Server> create(std::string_view target);
  Server(Private, std::string &&metadataHeader, std::string &&sessionToken,
      std::unique_ptr<ConsoleService::Stub> &&consoleService,
      std::unique_ptr<arrow::flight::FlightClient> &&flightClient);
  ~Server();

  void bindToVariable(const Ticket &ticket, std::string_view variableName);

  void addAuthHeadersToFlightCallOptions(arrow::flight::FlightCallOptions *options);
  void addAuthHeadersToContext(grpc::ClientContext *context);

  std::tuple<Ticket, arrow::flight::FlightDescriptor> newTicketAndFlightDescriptor();

  arrow::flight::FlightClient *flightClient() {
    return flightClient_.get();
  }

private:
  void startConsole();

  std::string metadataHeader_;
  std::string sessionToken_;
  std::unique_ptr<ConsoleService::Stub> consoleService_;
  std::unique_ptr<arrow::flight::FlightClient> flightClient_;
  Ticket consoleId_;
  int32_t nextFreeTicketId_ = 1;
};

void doit(Server *server);

void okOrThrow(std::string_view where, const arrow::Status &status);
void okOrThrow(std::string_view where, const grpc::Status &status);

template<typename T>
T valueOrThrow(std::string_view where, arrow::Result<T> result);

std::string makeMessage(std::string_view where, const std::string &statusMessage);
}  // namespace

// This example shows how to use the Arrow Flight client to make a simple table.
int main() {
  const char *target = "localhost:10000";

  try {
    auto server = Server::create(target);
    doit(server.get());
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void doit(Server *server) {
  // 1. Build schema
  arrow::SchemaBuilder schemaBuilder;

  // 2a. Add "X" column (type: int) to schema
  {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(md->Set("deephaven:type", "int")));
    auto field = std::make_shared<arrow::Field>("X",
        std::make_shared<arrow::Int32Type>(), true, std::move(md));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(field)));
  }

  // 2b. Add "Y" column (type: double) to schema
  {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(md->Set("deephaven:type", "double")));
    auto field = std::make_shared<arrow::Field>("Y",
        std::make_shared<arrow::DoubleType>(), true, std::move(md));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(field)));
  }

  // 2c. Add "Z" column (type: string) to schema
  {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    okOrThrow(DEEPHAVEN_EXPR_MSG(md->Set("deephaven:type", "java.lang.String")));
    auto field = std::make_shared<arrow::Field>("Z",
        std::make_shared<arrow::StringType>(), true, std::move(md));
    okOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.AddField(field)));
  }

  // 3. Schema is done
  auto schema = valueOrThrow(DEEPHAVEN_EXPR_MSG(schemaBuilder.Finish()));

  // 4. Set up test data
  std::vector<int32_t> xs{1, 10, 6060842, -12345};
  std::vector<double> ys{3.14159, 87, 123.456, 8888.4};
  std::vector<std::string> zs{"hello", "there", "Deephaven", "!"};

  auto numRows = xs.size();
  if (numRows != ys.size() || numRows != zs.size()) {
    throw std::runtime_error("size mismatch");
  }

  // 5. Populate column builders
  arrow::Int32Builder xBuilder;
  arrow::DoubleBuilder yBuilder;
  arrow::StringBuilder zBuilder;
  okOrThrow(DEEPHAVEN_EXPR_MSG(xBuilder.AppendValues(xs)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(yBuilder.AppendValues(ys)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(zBuilder.AppendValues(zs)));
  auto xCol = valueOrThrow(DEEPHAVEN_EXPR_MSG(xBuilder.Finish()));
  auto yCol = valueOrThrow(DEEPHAVEN_EXPR_MSG(yBuilder.Finish()));
  auto zCol = valueOrThrow(DEEPHAVEN_EXPR_MSG(zBuilder.Finish()));

  // 6. Gather Arrow columns
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      std::move(xCol),
      std::move(yCol),
      std::move(zCol)
  };

  // 7. Allocate an Arrow flight descriptor
  auto [tableTicket, fd] = server->newTicketAndFlightDescriptor();

  // 8. DoPut takes FlightCallOptions, which need to at least contain the Deephaven
  // authentication headers for this session.
  arrow::flight::FlightCallOptions options;
  server->addAuthHeadersToFlightCallOptions(&options);

  // 9. Perform the doPut
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
  okOrThrow(DEEPHAVEN_EXPR_MSG(server->flightClient()->DoPut(options, fd, schema, &fsw, &fmr)));

  // 10. Make a RecordBatch containing both the schema and the data
  auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(numRows), std::move(columns));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*batch)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));

  // 10. Read back a metadata message (ignored), then close the Writer
  std::shared_ptr<arrow::Buffer> buf;
  okOrThrow(DEEPHAVEN_EXPR_MSG(fmr->ReadMetadata(&buf)));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));

  // 11. Bind it to a variable so you can see it in the console
  server->bindToVariable(tableTicket, "showme");
}

std::shared_ptr<Server> Server::create(std::string_view target) {
  auto flightTarget = "grpc://" + std::string(target);
  arrow::flight::Location location;
  okOrThrow(DEEPHAVEN_EXPR_MSG(arrow::flight::Location::Parse(flightTarget, &location)));

  std::unique_ptr<arrow::flight::FlightClient> flightClient;
  okOrThrow(DEEPHAVEN_EXPR_MSG(arrow::flight::FlightClient::Connect(location, &flightClient)));

  std::string targetAsString(target);
  auto channel = grpc::CreateChannel(targetAsString, grpc::InsecureChannelCredentials());
  auto cs = ConsoleService::NewStub(channel);
  auto ss = SessionService::NewStub(channel);

  HandshakeRequest hsReq;
  HandshakeResponse hsResp;
  hsReq.set_auth_protocol(1);
  grpc::ClientContext ctx1;
  okOrThrow(DEEPHAVEN_EXPR_MSG(ss->NewSession(&ctx1, hsReq, &hsResp)));

  std::string mh = std::move(*hsResp.mutable_metadata_header());
  std::string st = std::move(*hsResp.mutable_session_token());
  std::transform(mh.begin(), mh.end(), mh.begin(), ::tolower);
  auto server = std::make_shared<Server>(Private(), std::move(mh), std::move(st),
      std::move(cs), std::move(flightClient));

  server->startConsole();
  return server;
}

Server::Server(Private, std::string &&metadataHeader, std::string &&sessionToken,
    std::unique_ptr<ConsoleService::Stub> &&consoleService,
    std::unique_ptr<arrow::flight::FlightClient> &&flightClient)
  : metadataHeader_(std::move(metadataHeader)), sessionToken_(std::move(sessionToken)),
    consoleService_(std::move(consoleService)), flightClient_(std::move(flightClient)) {}

Server::~Server() = default;

void Server::startConsole() {
  auto [scTicket, dummy] = newTicketAndFlightDescriptor();
  StartConsoleRequest scReq;
  StartConsoleResponse scResp;
  *scReq.mutable_result_id() = std::move(scTicket);
  scReq.set_session_type("python");
  grpc::ClientContext ctx;
  addAuthHeadersToContext(&ctx);
  okOrThrow(DEEPHAVEN_EXPR_MSG(consoleService_->StartConsole(&ctx, scReq, &scResp)));
  consoleId_ = std::move(*scResp.mutable_result_id());
}

void Server::bindToVariable(const Ticket &ticket, std::string_view variableName) {
  grpc::ClientContext ctx;
  addAuthHeadersToContext(&ctx);
  BindTableToVariableRequest req;
  BindTableToVariableResponse resp;
  *req.mutable_console_id() = consoleId_;
  req.set_variable_name(std::string(variableName));
  *req.mutable_table_id() = ticket;
  okOrThrow(DEEPHAVEN_EXPR_MSG(consoleService_->BindTableToVariable(&ctx, req, &resp)));
}

void Server::addAuthHeadersToFlightCallOptions(arrow::flight::FlightCallOptions *options) {
  auto item = std::make_pair(metadataHeader_, sessionToken_);
  options->headers.push_back(std::move(item));
}

void Server::addAuthHeadersToContext(grpc::ClientContext *context) {
  context->AddMetadata(metadataHeader_, sessionToken_);
}

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

std::tuple<Ticket, arrow::flight::FlightDescriptor> Server::newTicketAndFlightDescriptor() {
  auto ticketId = nextFreeTicketId_++;
  Ticket ticket = makeNewTicket(ticketId);
  arrow::flight::FlightDescriptor fd = arrow::flight::FlightDescriptor::Path({"export", std::to_string(ticketId)});
  return std::make_tuple<Ticket, arrow::flight::FlightDescriptor>(std::move(ticket), std::move(fd));
}

void okOrThrow(std::string_view where, const arrow::Status &status) {
  if (!status.ok()) {
    throw std::runtime_error(makeMessage(where, status.ToString()));
  }
}

void okOrThrow(std::string_view where, const grpc::Status &status) {
  if (!status.ok()) {
    throw std::runtime_error(makeMessage(where, status.error_message()));
  }
}

template<typename T>
T valueOrThrow(std::string_view where, arrow::Result<T> result) {
  if (!result.ok()) {
    throw std::runtime_error(makeMessage(where, result.status().ToString()));
  }
  return std::move(*result);
}

std::string makeMessage(std::string_view where, const std::string &statusMessage) {
  std::string result(statusMessage);
  result += " at ";
  result += where;
  return result;
}
}  // namespace
