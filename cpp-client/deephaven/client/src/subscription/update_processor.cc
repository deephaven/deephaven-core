/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/update_processor.h"

#include <iostream>
#include <memory>
#include "deephaven/client/chunk/chunk_filler.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/immerutil/abstract_flex_vector.h"
#include "deephaven/client/immerutil/immer_column_source.h"
#include "deephaven/client/subscription/batch_parser.h"
#include "deephaven/client/subscription/classic_table_state.h"
#include "deephaven/client/subscription/immer_table_state.h"
#include "deephaven/client/subscription/index_decoder.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/client/ticking.h"
#include "deephaven/flatbuf/Barrage_generated.h"

using deephaven::client::ClassicTickingUpdate;
using deephaven::client::chunk::ChunkFiller;
using deephaven::client::chunk::ChunkMaker;
using deephaven::client::chunk::UInt64Chunk;
using deephaven::client::column::MutableColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::immerutil::AbstractFlexVectorBase;
using deephaven::client::subscription::BatchParser;
using deephaven::client::table::Table;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::BarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::BarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::BarrageUpdateMetadata;
using io::deephaven::barrage::flatbuf::ColumnConversionMode;
using io::deephaven::barrage::flatbuf::CreateBarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionOptions;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionRequest;

namespace deephaven::client::subscription {
namespace {
struct ExtractedMetadata {
  ExtractedMetadata(size_t numAdds,
      size_t numMods,
      std::shared_ptr<RowSequence> removedRows,
      std::shared_ptr<RowSequence> shiftStartIndex,
      std::shared_ptr<RowSequence> shiftEndIndex,
      std::shared_ptr<RowSequence> shiftDestIndex,
      std::shared_ptr<RowSequence> addedRows,
      std::vector<std::shared_ptr<RowSequence>> modifiedRows);
  ~ExtractedMetadata();

  size_t numAdds_ = 0;
  size_t numMods_ = 0;
  std::shared_ptr<RowSequence> removedRows_;
  std::shared_ptr<RowSequence> shiftStartIndex_;
  std::shared_ptr<RowSequence> shiftEndIndex_;
  std::shared_ptr<RowSequence> shiftDestIndex_;
  std::shared_ptr<RowSequence> addedRows_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRows_;
};

std::optional<ExtractedMetadata> extractMetadata(
    const arrow::flight::FlightStreamChunk &flightStreamChunk);
}  // namespace

std::shared_ptr<UpdateProcessor> UpdateProcessor::startThread(
    std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs,
    std::shared_ptr<TickingCallback> callback,
    bool wantImmer) {
  auto result = std::make_shared<UpdateProcessor>(std::move(fsr),
      std::move(colDefs), std::move(callback));
  std::thread t(&UpdateProcessor::runForever, result, wantImmer);
  t.detach();
  return result;
}

UpdateProcessor::UpdateProcessor(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
    fsr_(std::move(fsr)), colDefs_(std::move(colDefs)), callback_(std::move(callback)) {}

UpdateProcessor::~UpdateProcessor() = default;

void UpdateProcessor::cancel() {
  fsr_->Cancel();
}

void UpdateProcessor::runForever(const std::shared_ptr<UpdateProcessor> &self, bool wantImmer) {
  std::cerr << "UpdateProcessor is starting.\n";
  std::exception_ptr eptr;
  try {
    if (wantImmer) {
      self->immerRunForeverHelper();
    } else {
      self->classicRunForeverHelper();
    }
  } catch (...) {
    eptr = std::current_exception();
    self->callback_->onFailure(eptr);
  }
  std::cerr << "UpdateProcessor is exiting.\n";
}

void UpdateProcessor::classicRunForeverHelper() {
  ClassicTableState state(*colDefs_);

  // In this loop we process Arrow Flight messages until error or cancellation.
  arrow::flight::FlightStreamChunk flightStreamChunk;
  while (true) {
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));

    // Parse all the metadata out of the Barrage message before we advance the cursor past it.
    auto mdo = extractMetadata(flightStreamChunk);
    if (!mdo.has_value()) {
      continue;
    }
    auto &md = *mdo;

    // Correct order to process all this info is:
    // 1. removes
    // 2. shifts
    // 3. adds
    // 4. modifies

    // 1. Removes
    auto removedRowsKeySpace = std::move(md.removedRows_);
    auto removedRowsIndexSpace = state.erase(*removedRowsKeySpace);

    // 2. Shifts
    state.applyShifts(*md.shiftStartIndex_, *md.shiftEndIndex_, *md.shiftDestIndex_);

    // 3. Adds
    auto addedRowsKeySpace = RowSequence::createEmpty();
    auto addedRowsIndexSpace = UInt64Chunk::create(0);
    if (md.numAdds_ != 0) {
      addedRowsKeySpace = std::move(md.addedRows_);
      addedRowsIndexSpace = state.addKeys(*addedRowsKeySpace);

      // Copy everything.
      auto rowsRemaining = addedRowsIndexSpace.take(addedRowsIndexSpace.size());

      auto processAddBatch = [&state, &rowsRemaining](
          const std::vector<std::shared_ptr<arrow::Array>> &data) {
        if (data.empty()) {
          return;
        }
        auto size = data[0]->length();
        auto rowsToAddThisTime = rowsRemaining.take(size);
        rowsRemaining = rowsRemaining.drop(size);
        state.addData(data, rowsToAddThisTime);
      };
      BatchParser::parseBatches(*colDefs_, md.numAdds_, false, fsr_.get(), &flightStreamChunk,
          processAddBatch);

      if (md.numMods_ != 0) {
        // Currently the FlightStreamReader is pointing to the last add record. We need to advance
        // it so it points to the first mod record.
        okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));
      }
    }

    auto ncols = colDefs_->vec().size();

    // 4. Modifies
    auto modifiedRowsKeySpace = std::move(md.modifiedRows_);
    auto modifiedRowsIndexSpace = state.modifyKeys(modifiedRowsKeySpace);
    if (md.numMods_ != 0) {
      // Local copy of modifiedRowsIndexSpace
      auto keysRemaining = makeReservedVector<UInt64Chunk>(ncols);
      for (const auto &keys : modifiedRowsIndexSpace) {
        keysRemaining.push_back(keys.take(keys.size()));
      }

      std::vector<UInt64Chunk> keysToModifyThisTime(ncols);

      auto processModifyBatch = [&state, &keysRemaining, &keysToModifyThisTime, ncols](
          const std::vector<std::shared_ptr<arrow::Array>> &data) {
        if (data.size() != ncols) {
          throw std::runtime_error(stringf("data.size() != ncols (%o != %o)", data.size(), ncols));
        }
        for (size_t i = 0; i < data.size(); ++i) {
          const auto &src = data[i];
          auto &krm = keysRemaining[i];
          keysToModifyThisTime[i] = krm.take(src->length());
          krm = krm.drop(src->length());
        }
        state.modifyData(data, keysToModifyThisTime);
      };

      BatchParser::parseBatches(*colDefs_, md.numMods_, true, fsr_.get(), &flightStreamChunk,
          processModifyBatch);
    }

    auto currentTableKeySpace = state.snapshot();
    auto currentTableIndexSpace = state.snapshotUnwrapped();

    ClassicTickingUpdate update(std::move(removedRowsKeySpace), std::move(removedRowsIndexSpace),
        std::move(addedRowsKeySpace), std::move(addedRowsIndexSpace),
        std::move(modifiedRowsKeySpace), std::move(modifiedRowsIndexSpace),
        std::move(currentTableKeySpace), std::move(currentTableIndexSpace));
    callback_->onTick(update);
  }
}

void UpdateProcessor::immerRunForeverHelper() {
  ImmerTableState state(*colDefs_);

  // In this loop we process Arrow Flight messages until error or cancellation.
  arrow::flight::FlightStreamChunk flightStreamChunk;
  while (true) {
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));

    // Parse all the metadata out of the Barrage message before we advance the cursor past it.
    auto mdo = extractMetadata(flightStreamChunk);
    if (!mdo.has_value()) {
      continue;
    }
    auto &md = *mdo;

    // Correct order to process all this info is:
    // 1. removes
    // 2. shifts
    // 3. adds
    // 4. modifies

    // 1. Removes
    auto beforeRemoves = state.snapshot();
    auto removedRowsKeySpace = std::move(md.removedRows_);
    auto removedRowsIndexSpace = state.erase(*removedRowsKeySpace);

    // 2. Shifts
    state.applyShifts(*md.shiftStartIndex_, *md.shiftEndIndex_, *md.shiftDestIndex_);

    // 3. Adds
    auto addedRowsKeySpace = RowSequence::createEmpty();
    auto addedRowsIndexSpace = RowSequence::createEmpty();
    if (md.numAdds_ != 0) {
      addedRowsKeySpace = std::move(md.addedRows_);
      addedRowsIndexSpace = state.addKeys(*addedRowsKeySpace);

      // Copy everything.
      auto rowsRemaining = addedRowsIndexSpace->take(addedRowsIndexSpace->size());

      auto processAddBatch = [&state, &rowsRemaining](
          const std::vector<std::shared_ptr<arrow::Array>> &data) {
        if (data.empty()) {
          return;
        }
        auto size = data[0]->length();
        auto rowsToAddThisTime = rowsRemaining->take(size);
        rowsRemaining = rowsRemaining->drop(size);
        state.addData(data, *rowsToAddThisTime);
      };
      BatchParser::parseBatches(*colDefs_, md.numAdds_, false, fsr_.get(), &flightStreamChunk,
          processAddBatch);

      if (md.numMods_ != 0) {
        // Currently the FlightStreamReader is pointing to the last add record. We need to advance
        // it so it points to the first mod record.
        okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));
      }
    }

    auto beforeModifies = state.snapshot();

    auto ncols = colDefs_->vec().size();

    // 4. Modifies
    auto modifiedRowsKeySpace = std::move(md.modifiedRows_);
    auto modifiedRowsIndexSpace = state.modifyKeys(modifiedRowsKeySpace);
    if (md.numMods_ != 0) {
      // Local copy of modifiedRowsIndexSpace
      auto keysRemaining = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
      for (const auto &keys : modifiedRowsIndexSpace) {
        keysRemaining.push_back(keys->take(keys->size()));
      }

      std::vector<std::shared_ptr<RowSequence>> keysToModifyThisTime(ncols);

      auto processModifyBatch = [&state, &keysRemaining, &keysToModifyThisTime, ncols](
          const std::vector<std::shared_ptr<arrow::Array>> &data) {
        if (data.size() != ncols) {
          throw std::runtime_error(stringf("data.size() != ncols (%o != %o)", data.size(), ncols));
        }
        for (size_t i = 0; i < data.size(); ++i) {
          const auto &src = data[i];
          auto &krm = keysRemaining[i];
          keysToModifyThisTime[i] = krm->take(src->length());
          krm = krm->drop(src->length());
        }
        state.modifyData(data, keysToModifyThisTime);
      };

      BatchParser::parseBatches(*colDefs_, md.numMods_, true, fsr_.get(), &flightStreamChunk,
          processModifyBatch);
    }

    auto current = state.snapshot();
    ImmerTickingUpdate update(std::move(beforeRemoves), std::move(beforeModifies),
        std::move(current), std::move(removedRowsIndexSpace), std::move(modifiedRowsIndexSpace),
        std::move(addedRowsIndexSpace));
    callback_->onTick(std::move(update));
  }
}

namespace {
std::optional<ExtractedMetadata> extractMetadata(
    const arrow::flight::FlightStreamChunk &flightStreamChunk) {
  if (flightStreamChunk.app_metadata == nullptr) {
    std::cerr << "TODO(kosak) -- unexpected - chunk.app_metdata == nullptr\n";
    return {};
  }

  const auto *barrageWrapperRaw = flightStreamChunk.app_metadata->data();
  const auto *barrageWrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(barrageWrapperRaw);
  if (barrageWrapper->magic() != deephavenMagicNumber) {
    return {};
  }
  if (barrageWrapper->msg_type() !=
      BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata) {
    return {};
  }

  const auto *bmdRaw = barrageWrapper->msg_payload()->data();
  const auto *bmd = flatbuffers::GetRoot<BarrageUpdateMetadata>(bmdRaw);
  auto numAdds = bmd->num_add_batches();
  auto numMods = bmd->num_mod_batches();
  // streamf(std::cerr, "num_add_batches=%o, num_mod_batches=%o\n", numAdds, numMods);
  // streamf(std::cerr, "FYI, my row sequence is currently %o\n", *tickingTable->getRowSequence());

  DataInput diAdded(*bmd->added_rows());
  DataInput diRemoved(*bmd->removed_rows());
  DataInput diThreeShiftIndexes(*bmd->shift_data());

  auto addedRows = IndexDecoder::readExternalCompressedDelta(&diAdded);
  auto removedRows = IndexDecoder::readExternalCompressedDelta(&diRemoved);
  auto shiftStartIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftEndIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftDestIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);

  std::vector<std::shared_ptr<RowSequence>> perColumnModifies;
  if (numMods != 0) {
    const auto &modColumnNodes = *bmd->mod_column_nodes();
    perColumnModifies.reserve(modColumnNodes.size());
    for (size_t i = 0; i < modColumnNodes.size(); ++i) {
      const auto &elt = modColumnNodes.Get(i);
      DataInput diModified(*elt->modified_rows());
      auto modRows = IndexDecoder::readExternalCompressedDelta(&diModified);
      perColumnModifies.push_back(std::move(modRows));
    }
  }
//  streamf(std::cerr, "RemovedRows: {%o}\n", *removedRows);
//  streamf(std::cerr, "AddedRows: {%o}\n", *addedRows);
//  streamf(std::cerr, "shift start: {%o}\n", *shiftStartIndex);
//  streamf(std::cerr, "shift end: {%o}\n", *shiftEndIndex);
//  streamf(std::cerr, "shift dest: {%o}\n", *shiftDestIndex);

  return ExtractedMetadata(numAdds,
      numMods,
      std::move(removedRows),
      std::move(shiftStartIndex),
      std::move(shiftEndIndex),
      std::move(shiftDestIndex),
      std::move(addedRows),
      std::move(perColumnModifies));
}

ExtractedMetadata::ExtractedMetadata(size_t numAdds, size_t numMods,
    std::shared_ptr<RowSequence> removedRows, std::shared_ptr<RowSequence> shiftStartIndex,
    std::shared_ptr<RowSequence> shiftEndIndex, std::shared_ptr<RowSequence> shiftDestIndex,
    std::shared_ptr<RowSequence> addedRows,
    std::vector<std::shared_ptr<RowSequence>> modifiedRows) :
    numAdds_(numAdds), numMods_(numMods),
    removedRows_(std::move(removedRows)), shiftStartIndex_(std::move(shiftStartIndex)),
    shiftEndIndex_(std::move(shiftEndIndex)), shiftDestIndex_(std::move(shiftDestIndex)),
    addedRows_(std::move(addedRows)), modifiedRows_(std::move(modifiedRows)) {}

ExtractedMetadata::~ExtractedMetadata() = default;

//ThreadNubbin::ThreadNubbin(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
//    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
//    fsr_(std::move(fsr)), colDefs_(std::move(colDefs)), callback_(std::move(callback)) {}
}  // namespace
}  // namespace deephaven::client::subscription
