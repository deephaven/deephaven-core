/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/subscribe_thread.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/scalar.h>
#include <atomic>
#include <grpc/support/log.h>
#include "deephaven/client/arrowutil/arrow_column_source.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/client/utility/executor.h"
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
using deephaven::client::arrowutil::ArrowInt8ColumnSource;
using deephaven::client::arrowutil::ArrowInt16ColumnSource;
using deephaven::client::arrowutil::ArrowInt32ColumnSource;
using deephaven::client::arrowutil::ArrowInt64ColumnSource;
using deephaven::client::arrowutil::ArrowBooleanColumnSource;
using deephaven::client::arrowutil::ArrowDateTimeColumnSource;
using deephaven::client::arrowutil::ArrowStringColumnSource;
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

ColumnSourceAndSize ArrayToColumnSource(const arrow::Array &array);
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
  thread_.join();
}

void UpdateProcessor::RunUntilCancelled(std::shared_ptr<UpdateProcessor> self) {
  try {
    self->RunForeverHelper();
  } catch (...) {
    // If the thread was been cancelled via explicit user action, then swallow all errors.
    if (!self->cancelled_) {
      self->callback_->OnFailure(std::current_exception());
    }
  }
}

void UpdateProcessor::RunForeverHelper() {
  // Reuse the chunk for efficiency.
  BarrageProcessor bp(schema_);
  // Process Arrow Flight messages until error or cancellation.
  while (true) {
    auto chunk = fsr_->Next();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(chunk));
    const auto &cols = chunk->data->columns();
    auto column_sources = MakeReservedVector<std::shared_ptr<ColumnSource>>(cols.size());
    auto sizes = MakeReservedVector<size_t>(cols.size());
    for (const auto &col : cols) {
      auto css = ArrayToColumnSource(*col);
      column_sources.push_back(std::move(css.columnSource_));
      sizes.push_back(css.size_);
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

struct ArrayToColumnSourceVisitor final : public arrow::ArrayVisitor {
  explicit ArrayToColumnSourceVisitor(std::shared_ptr<arrow::Array> storage) : storage_(std::move(storage)) {}

  arrow::Status Visit(const arrow::Int8Array &array) final {
    result_ = ArrowInt8ColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Array &array) final {
    result_ = ArrowInt16ColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Array &array) final {
    result_ = ArrowInt32ColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Array &array) final {
    result_ = ArrowInt64ColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray &array) final {
    result_ = ArrowBooleanColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray &array) final {
    result_ = ArrowStringColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampArray &array) final {
    result_ = ArrowDateTimeColumnSource::Create(std::move(storage_), &array);
    return arrow::Status::OK();
  }

  // We keep a shared_ptr to the arrow array in order to keep the underlying storage alive.
  std::shared_ptr<arrow::Array> storage_;
  std::shared_ptr<ColumnSource> result_;
};

// Creates a non-owning chunk of the right type that points to the corresponding array data.
ColumnSourceAndSize ArrayToColumnSource(const arrow::Array &array) {
  const auto *list_array = VerboseCast<const arrow::ListArray *>(DEEPHAVEN_LOCATION_EXPR(&array));

  if (list_array->length() != 1) {
    auto message = fmt::format("Expected array of length 1, got {}", array.length());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  const auto list_element = list_array->GetScalar(0).ValueOrDie();
  const auto *list_scalar = VerboseCast<const arrow::ListScalar *>(
      DEEPHAVEN_LOCATION_EXPR(list_element.get()));
  const auto &list_scalar_value = list_scalar->value;

  ArrayToColumnSourceVisitor v(list_scalar_value);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(list_scalar_value->Accept(&v)));
  return {std::move(v.result_), static_cast<size_t>(list_scalar_value->length())};
}
}  // namespace
}  // namespace deephaven::client::subscription
