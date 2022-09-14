/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/subscribe_thread.h"

#include <flatbuffers/detached_buffer.h>
#include "deephaven/client/ticking.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/subscription/index_decoder.h"
#include "deephaven/client/subscription/update_processor.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/utility/misc.h"
#include "deephaven/flatbuf/Barrage_generated.h"

using deephaven::client::TickingCallback;
using deephaven::client::subscription::UpdateProcessor;
using deephaven::client::utility::Callback;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::Executor;
using deephaven::client::utility::okOrThrow;
using deephaven::client::server::Server;
using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::ColumnConversionMode;
using io::deephaven::proto::backplane::grpc::Ticket;

namespace deephaven::client::subscription {
namespace {
class SubscribeState final : public Callback<> {
  typedef deephaven::client::server::Server Server;

public:
  SubscribeState(std::shared_ptr<Server> server, std::vector<int8_t> ticketBytes,
      std::shared_ptr<ColumnDefinitions> colDefs,
      std::promise<std::shared_ptr<SubscriptionHandle>> promise,
      std::shared_ptr <TickingCallback> callback);
  void invoke() final;

private:
  std::shared_ptr<SubscriptionHandle> invokeHelper();

  std::shared_ptr<Server> server_;
  std::vector<int8_t> ticketBytes_;
  std::shared_ptr<ColumnDefinitions> colDefs_;
  std::promise<std::shared_ptr<SubscriptionHandle>> promise_;
  std::shared_ptr<TickingCallback> callback_;
};

// A simple extension to arrow::Buffer that owns its DetachedBuffer storage
class OwningBuffer final : public arrow::Buffer {
public:
  explicit OwningBuffer(flatbuffers::DetachedBuffer buffer);
  ~OwningBuffer() final;

private:
  flatbuffers::DetachedBuffer buffer_;
};
}  // namespace

std::shared_ptr<SubscriptionHandle> startSubscribeThread(
    std::shared_ptr<Server> server,
    Executor *flightExecutor,
    std::shared_ptr<ColumnDefinitions> columnDefinitions,
    const Ticket &ticket,
    std::shared_ptr<TickingCallback> callback) {
  std::promise<std::shared_ptr<SubscriptionHandle>> promise;
  auto future = promise.get_future();
  std::vector<int8_t> ticketBytes(ticket.ticket().begin(), ticket.ticket().end());
  auto ss = std::make_shared<SubscribeState>(std::move(server), std::move(ticketBytes),
      std::move(columnDefinitions), std::move(promise), std::move(callback));
  flightExecutor->invoke(std::move(ss));
  return future.get();
}

namespace {
SubscribeState::SubscribeState(std::shared_ptr<Server> server, std::vector<int8_t> ticketBytes,
    std::shared_ptr<ColumnDefinitions> colDefs, std::promise<std::shared_ptr<SubscriptionHandle>> promise,
    std::shared_ptr<TickingCallback> callback) :
    server_(std::move(server)), ticketBytes_(std::move(ticketBytes)), colDefs_(std::move(colDefs)),
    promise_(std::move(promise)), callback_(std::move(callback)) {}

void SubscribeState::invoke() {
  try {
    auto handle = invokeHelper();
    // If you made it this far, then you have been successful!
    promise_.set_value(std::move(handle));
  } catch (const std::exception &e) {
    promise_.set_exception(std::make_exception_ptr(e));
  }
}

namespace {
// Wrapper class that forwards the "cancel" call from SubscriptionHandle to the UpdateProcessor
struct CancelWrapper final : SubscriptionHandle {
  explicit CancelWrapper(std::shared_ptr<UpdateProcessor> updateProcessor) :
      updateProcessor_(std::move(updateProcessor)) {}

  void cancel() final {
    updateProcessor_->cancel();
  }

  std::shared_ptr<UpdateProcessor> updateProcessor_;
};
}  // namespace

std::shared_ptr<SubscriptionHandle> SubscribeState::invokeHelper() {
  arrow::flight::FlightCallOptions fco;
  fco.headers.push_back(server_->makeBlessing());
  auto *client = server_->flightClient();

  arrow::flight::FlightDescriptor dummy;
  char magicData[4];
  uint32_t src = deephavenMagicNumber;
  memcpy(magicData, &src, sizeof(magicData));

  dummy.type = arrow::flight::FlightDescriptor::DescriptorType::CMD;
  dummy.cmd = std::string(magicData, 4);
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
  okOrThrow(DEEPHAVEN_EXPR_MSG(client->DoExchange(fco, dummy, &fsw, &fsr)));

  // Make a BarrageMessageWrapper
  // ...Whose payload is a BarrageSubscriptionRequest
  // ......which has BarrageSubscriptionOptions

  flatbuffers::FlatBufferBuilder payloadBuilder(4096);

  auto subOptions = CreateBarrageSubscriptionOptions(payloadBuilder,
      ColumnConversionMode::ColumnConversionMode_Stringify, true, 0, 4096);

  auto ticket = payloadBuilder.CreateVector(ticketBytes_);
  auto subreq = CreateBarrageSubscriptionRequest(payloadBuilder, ticket, {}, {}, subOptions);
  payloadBuilder.Finish(subreq);
  // TODO(kosak): fix sad cast
  const auto *payloadp = (int8_t*)payloadBuilder.GetBufferPointer();
  const auto payloadSize = payloadBuilder.GetSize();

  // TODO: I'd really like to just point this buffer backwards to the thing I just created, rather
  // then copying it. But, eh, version 2.
  flatbuffers::FlatBufferBuilder wrapperBuilder(4096);
  auto payload = wrapperBuilder.CreateVector(payloadp, payloadSize);
  auto messageWrapper = CreateBarrageMessageWrapper(wrapperBuilder, deephavenMagicNumber,
      BarrageMessageType::BarrageMessageType_BarrageSubscriptionRequest, payload);
  wrapperBuilder.Finish(messageWrapper);
  auto wrapperBuffer = wrapperBuilder.Release();

  auto buffer = std::make_shared<OwningBuffer>(std::move(wrapperBuffer));
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteMetadata(std::move(buffer))));

  // Run forever (until error or cancellation)
  auto processor = UpdateProcessor::startThread(std::move(fsr), std::move(colDefs_),
      std::move(callback_));
  return std::make_shared<CancelWrapper>(std::move(processor));
}

OwningBuffer::OwningBuffer(flatbuffers::DetachedBuffer buffer) :
    arrow::Buffer(buffer.data(), int64_t(buffer.size())), buffer_(std::move(buffer)) {}
OwningBuffer::~OwningBuffer() = default;
}  // namespace
}  // namespace deephaven::client::subscription
