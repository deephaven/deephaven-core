/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/subscribe_thread.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/scalar.h>
#include <arrow/flight/client.h>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include <grpc/support/log.h>
#include "deephaven/client/arrowutil/arrow_column_source.h"
#include "deephaven/client/arrowutil/arrow_array_converter.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/dhcore/ticking/barrage_processor.h"

using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::ticking::BarrageProcessor;
using deephaven::dhcore::ticking::TickingCallback;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::VerboseCast;
using deephaven::client::arrowutil::ArrowArrayConverter;
using deephaven::client::arrowutil::BooleanArrowColumnSource;
using deephaven::client::arrowutil::CharArrowColumnSource;
using deephaven::client::arrowutil::DateTimeArrowColumnSource;
using deephaven::client::arrowutil::DoubleArrowColumnSource;
using deephaven::client::arrowutil::FloatArrowColumnSource;
using deephaven::client::arrowutil::Int8ArrowColumnSource;
using deephaven::client::arrowutil::Int16ArrowColumnSource;
using deephaven::client::arrowutil::Int32ArrowColumnSource;
using deephaven::client::arrowutil::Int64ArrowColumnSource;
using deephaven::client::arrowutil::LocalDateArrowColumnSource;
using deephaven::client::arrowutil::LocalTimeArrowColumnSource;
using deephaven::client::arrowutil::StringArrowColumnSource;
using deephaven::client::utility::Executor;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::server::Server;
using io::deephaven::proto::backplane::grpc::Ticket;
using arrow::flight::FlightStreamReader;
using arrow::flight::FlightStreamWriter;

namespace deephaven::client::subscription {
namespace {
// This class manages just enough state to serve as a Callback to the ... [TODO: finish this comment]
class SubscribeState final {
  using Server = deephaven::client::server::Server;

public:
  SubscribeState(std::shared_ptr<Server> server, std::vector<int8_t> ticket_bytes,
      std::shared_ptr<Schema> schema, std::promise<std::shared_ptr<SubscriptionHandle>> promise,
      std::shared_ptr <TickingCallback> callback);
  void Invoke();

private:
  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> InvokeHelper();

  std::shared_ptr<Server> server_;
  std::vector<int8_t> ticketBytes_;
  std::shared_ptr<Schema> schema_;
  std::promise<std::shared_ptr<SubscriptionHandle>> promise_;
  std::shared_ptr<TickingCallback> callback_;
};

// The UpdateProcessor class also implements the SubscriptionHandle interface so that our callers
// can cancel us if they want to.
class UpdateProcessor final : public SubscriptionHandle {
public:
  [[nodiscard]]
  static std::shared_ptr<UpdateProcessor> StartThread(std::unique_ptr<FlightStreamReader> fsr,
      std::unique_ptr<FlightStreamWriter> fsw, std::shared_ptr<Schema> schema,
      std::shared_ptr<TickingCallback> callback);

  UpdateProcessor(std::unique_ptr<FlightStreamReader> fsr, std::unique_ptr<FlightStreamWriter> fsw,
      std::shared_ptr<Schema> schema, std::shared_ptr<TickingCallback> callback);
  ~UpdateProcessor() final;

  void Cancel() final;

  static void RunUntilCancelled(std::shared_ptr<UpdateProcessor> self);
  void RunForeverHelper();

private:
  std::unique_ptr<FlightStreamReader> fsr_;
  // The FlightStreamWriter is not used inside the thread, but arrow Flight >= 8.0.0 seems to
  // require that it stay alive (along with the FlightStreamReader) for the duration of the
  // DoExchange session.
  std::unique_ptr<FlightStreamWriter> fsw_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<TickingCallback> callback_;

  std::mutex mutex_;
  bool cancelled_ = false;
  std::thread thread_;
};

class OwningBuffer final : public arrow::Buffer {
public:
  explicit OwningBuffer(std::vector<uint8_t> data);
  ~OwningBuffer() final;

private:
  std::vector<uint8_t> data_;
};
struct ColumnSourceAndSize {
  std::shared_ptr<ColumnSource> columnSource_;
  size_t size_ = 0;
};

}  // namespace

std::shared_ptr<SubscriptionHandle> SubscriptionThread::Start(std::shared_ptr<Server> server,
    Executor *flight_executor, std::shared_ptr<Schema> schema, const Ticket &ticket,
    std::shared_ptr<TickingCallback> callback) {
  std::promise<std::shared_ptr<SubscriptionHandle>> promise;
  auto future = promise.get_future();
  std::vector<int8_t> ticket_bytes(ticket.ticket().begin(), ticket.ticket().end());
  auto ss = std::make_shared<SubscribeState>(std::move(server), std::move(ticket_bytes),
      std::move(schema), std::move(promise), std::move(callback));
  flight_executor->Invoke([ss]() { ss->Invoke(); });
  return future.get();
}

namespace {
SubscribeState::SubscribeState(std::shared_ptr<Server> server, std::vector<int8_t> ticket_bytes,
    std::shared_ptr<Schema> schema,
    std::promise<std::shared_ptr<SubscriptionHandle>> promise,
    std::shared_ptr<TickingCallback> callback) :
    server_(std::move(server)), ticketBytes_(std::move(ticket_bytes)), schema_(std::move(schema)),
    promise_(std::move(promise)), callback_(std::move(callback)) {}

void SubscribeState::Invoke() {
  try {
    auto subscription_handle = InvokeHelper();
    // If you made it this far, then you have been successful!
    promise_.set_value(std::move(subscription_handle));
  } catch (const std::exception &e) {
    promise_.set_exception(std::make_exception_ptr(e));
  }
}

std::shared_ptr<SubscriptionHandle> SubscribeState::InvokeHelper() {
  arrow::flight::FlightCallOptions fco;
  server_->ForEachHeaderNameAndValue(
      [&fco](const std::string &name, const std::string &value) {
        fco.headers.emplace_back(name, value);
      }
  );
  auto *client = server_->FlightClient();

  arrow::flight::FlightDescriptor descriptor;
  char magic_data[4];
  auto src = BarrageProcessor::kDeephavenMagicNumber;
  static_assert(sizeof(src) == sizeof(magic_data));
  memcpy(magic_data, &src, sizeof(magic_data));

  descriptor.type = arrow::flight::FlightDescriptor::DescriptorType::CMD;
  descriptor.cmd = std::string(magic_data, 4);
  auto res = client->DoExchange(fco, descriptor);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));

  auto sub_req_raw = BarrageProcessor::CreateSubscriptionRequest(ticketBytes_.data(),
      ticketBytes_.size());
  auto buffer = std::make_shared<OwningBuffer>(std::move(sub_req_raw));
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res->writer->WriteMetadata(std::move(buffer))));

  // Run forever (until error or cancellation)
  auto processor = UpdateProcessor::StartThread(std::move(res->reader), std::move(res->writer),
      std::move(schema_), std::move(callback_));
  return processor;
}

std::shared_ptr<UpdateProcessor> UpdateProcessor::StartThread(
    std::unique_ptr<FlightStreamReader> fsr,
    std::unique_ptr<FlightStreamWriter> fsw,
    std::shared_ptr<Schema> schema,
    std::shared_ptr<TickingCallback> callback) {
  auto result = std::make_shared<UpdateProcessor>(std::move(fsr), std::move(fsw),
      std::move(schema), std::move(callback));
  result->thread_ = std::thread(&RunUntilCancelled, result);
  return result;
}

UpdateProcessor::UpdateProcessor(std::unique_ptr<FlightStreamReader> fsr,
    std::unique_ptr<FlightStreamWriter> fsw,
    std::shared_ptr<Schema> schema, std::shared_ptr<TickingCallback> callback) :
    fsr_(std::move(fsr)), fsw_(std::move(fsw)), schema_(std::move(schema)),
    callback_(std::move(callback)), cancelled_(false) {}

UpdateProcessor::~UpdateProcessor() {
  Cancel();
}

void UpdateProcessor::Cancel() {
  constexpr const char *const kMe = "UpdateProcessor::Cancel";
  gpr_log(GPR_INFO, "%s: Subscription Shutdown requested.", kMe);
  std::unique_lock guard(mutex_);
  if (cancelled_) {
    guard.unlock(); // to be nice
    gpr_log(GPR_ERROR, "%s: Already cancelled.", kMe);
    return;
  }
  cancelled_ = true;
  guard.unlock();

  fsr_->Cancel();
  (void)fsw_->Close();
  thread_.join();
}

void UpdateProcessor::RunUntilCancelled(std::shared_ptr<UpdateProcessor> self) {
  try {
    self->RunForeverHelper();
  } catch (...) {
    // If the thread has been cancelled via explicit user action, then swallow all errors.
    if (!self->cancelled_) {
      self->callback_->OnFailure(std::current_exception());
    }
  }
}

/**
 * The columns coming back from Barrage are all wrapped in a list. This is done in order to
 * satisfy an Arrow invariant that all the columns coming back in a message have the same length.
 * In Barrage, in the modify case, the columns that come in may have different sizes. But we
 * wrap them in an additional List container. So instead of sending e.g.
 * IntArray[12 items], DoubleArray[5 items]
 * Barrage will send
 * List[1 item], List[1 item]
 * where the sole element in the first item is the IntArray[12 items]
 * and the sole element in the second item is the DoubleArray[5 items]
 */
std::shared_ptr<arrow::Array> UnwrapList(const arrow::Array &array) {
  const auto *list_array = VerboseCast<const arrow::ListArray *>(DEEPHAVEN_LOCATION_EXPR(&array));

  if (list_array->length() != 1) {
    auto message = fmt::format("Expected array of length 1, got {}", array.length());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  const auto list_element = list_array->GetScalar(0).ValueOrDie();
  const auto *list_scalar = VerboseCast<const arrow::ListScalar *>(
      DEEPHAVEN_LOCATION_EXPR(list_element.get()));
  return list_scalar->value;
}

void UpdateProcessor::RunForeverHelper() {
  // Reuse the chunk for efficiency.
  BarrageProcessor bp(schema_);
  // Process Arrow Flight messages until error or cancellation.
  while (true) {
    auto chunk = fsr_->Next();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(chunk));
    if (chunk->data == nullptr) {
      // Stream ended. This is abnormal for Deephaven.
      const char *message = "Unexpected end of stream";
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    const auto &cols = chunk->data->columns();
    auto column_sources = MakeReservedVector<std::shared_ptr<ColumnSource>>(cols.size());
    auto sizes = MakeReservedVector<size_t>(cols.size());
    for (const auto &col : cols) {
      auto array = UnwrapList(*col);
      sizes.push_back(array->length());
      auto cs = ArrowArrayConverter::ArrayToColumnSource(std::move(array));
      column_sources.push_back(std::move(cs));
    }

    const void *metadata = nullptr;
    size_t metadata_size = 0;
    if (chunk->app_metadata != nullptr) {
      metadata = chunk->app_metadata->data();
      metadata_size = chunk->app_metadata->size();
    }
    auto result = bp.ProcessNextChunk(column_sources, sizes, metadata, metadata_size);

    if (result.has_value()) {
      callback_->OnTick(std::move(*result));
    }
  }
}

OwningBuffer::OwningBuffer(std::vector<uint8_t> data) :
    arrow::Buffer(data.data(), static_cast<int64_t>(data.size())), data_(std::move(data)) {}
OwningBuffer::~OwningBuffer() = default;

}  // namespace
}  // namespace deephaven::client::subscription
