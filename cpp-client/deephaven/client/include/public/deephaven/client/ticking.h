/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include <map>
#include <set>
#include <arrow/type.h>
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/table/table.h"
#include "immer/flex_vector.hpp"

namespace deephaven::client {
class ClassicTickingUpdate;
class ImmerTickingUpdate;
class TickingCallback : public deephaven::client::utility::FailureCallback {
public:
  /**
   * @param update An update message which describes the changes (removes, adds, modifies) that
   * transform the previous version of the table to the new version. This class is *shared* with
   * the caller and so the receiving code will block the caller while it is using it.
   */
  virtual void onTick(const ClassicTickingUpdate &update) = 0;
  /**
   * @param update An update message which describes the changes (removes, adds, modifies) that
   * transform the previous version of the table to the new version. This class is threadsafe and
   * can be kept around for an arbitrary amount of time. On the other hand, it probably should be
   * processed and discard quickly so that the underlying resources can be reused.
   */
  virtual void onTick(ImmerTickingUpdate update) = 0;
};

class ClassicTickingUpdate final {
protected:
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::column::ColumnSource ColumnSource;
  typedef deephaven::client::container::RowSequence RowSequence;
  typedef deephaven::client::table::Table Table;

public:
  ClassicTickingUpdate(std::shared_ptr<RowSequence> removedRowsKeySpace,
      UInt64Chunk removedRowsIndexSpace,
      std::shared_ptr<RowSequence> addedRowsKeySpace,
      UInt64Chunk addedRowsIndexSpace,
      std::vector<std::shared_ptr<RowSequence>> modifiedRowsKeySpace,
      std::vector<UInt64Chunk> modifiedRowsIndexSpace,
      std::shared_ptr<Table> currentTableKeySpace,
      std::shared_ptr<Table> currentTableIndexSpace);
  ClassicTickingUpdate(ClassicTickingUpdate &&other) noexcept;
  ClassicTickingUpdate &operator=(ClassicTickingUpdate &&other) noexcept;
  ~ClassicTickingUpdate();

  const std::shared_ptr<RowSequence> &removedRowsKeySpace() const { return removedRowsKeySpace_; }
  const UInt64Chunk &removedRowsIndexSpace() const { return removedRowsIndexSpace_; }
  const std::shared_ptr<RowSequence> &addedRowsKeySpace() const { return addedRowsKeySpace_; }
  const UInt64Chunk &addedRowsIndexSpace() const { return addedRowsIndexSpace_; }
  const std::vector<std::shared_ptr<RowSequence>> &modifiedRowsKeySpace() const { return modifiedRowsKeySpace_; }
  const std::vector<UInt64Chunk> &modifiedRowsIndexSpace() const { return modifiedRowsIndexSpace_; }
  const std::shared_ptr<Table> &currentTableKeySpace() const { return currentTableKeySpace_; }
  const std::shared_ptr<Table> &currentTableIndexSpace() const { return currentTableIndexSpace_; }

private:
  // In the pre-shift key space
  std::shared_ptr<RowSequence> removedRowsKeySpace_;
  // In the pre-shift index space
  UInt64Chunk removedRowsIndexSpace_;
  // In the post-shift key space
  std::shared_ptr<RowSequence> addedRowsKeySpace_;
  // In the post-shift index space
  UInt64Chunk addedRowsIndexSpace_;
  // In the post-shift key space
  std::vector<std::shared_ptr<RowSequence>> modifiedRowsKeySpace_;
  // In the post-shift index space
  std::vector<UInt64Chunk> modifiedRowsIndexSpace_;

  std::shared_ptr<Table> currentTableKeySpace_;
  std::shared_ptr<Table> currentTableIndexSpace_;
};

class ImmerTickingUpdate final {
protected:
  typedef deephaven::client::container::RowSequence RowSequence;
  typedef deephaven::client::table::Table Table;

public:
  ImmerTickingUpdate(std::shared_ptr<Table> beforeRemoves,
      std::shared_ptr<Table> beforeModifies,
      std::shared_ptr<Table> current,
      std::shared_ptr<RowSequence> removed,
      std::vector<std::shared_ptr<RowSequence>> modified,
      std::shared_ptr<RowSequence> added);
  ImmerTickingUpdate(ImmerTickingUpdate &&other) noexcept;
  ImmerTickingUpdate &operator=(ImmerTickingUpdate &&other) noexcept;
  ~ImmerTickingUpdate();

  // Note: the table is flat.
  const std::shared_ptr<Table> &beforeRemoves() const { return beforeRemoves_; }
  // Note: the table is flat.
  const std::shared_ptr<Table> &beforeModifies() const { return beforeModifies_; }
  // Note: the table is flat.
  const std::shared_ptr<Table> &current() const { return current_; }
  // In the key space of 'prevTable'
  const std::shared_ptr<RowSequence> &removed() const { return removed_; }
  // In the key space of 'current'
  const std::vector<std::shared_ptr<RowSequence>> &modified() const { return modified_; }
  // In the key space of 'current'
  const std::shared_ptr<RowSequence> &added() const { return added_; }

private:
  std::shared_ptr<Table> beforeRemoves_;
  std::shared_ptr<Table> beforeModifies_;
  std::shared_ptr<Table> current_;
  // In the key space of 'beforeRemoves_'
  std::shared_ptr<RowSequence> removed_;
  // In the key space of beforeModifies_ and current_, which have the same key space.
  // Old values are in beforeModifies_; new values are in current_.
  std::vector<std::shared_ptr<RowSequence>> modified_;
  // In the key space of current_.
  std::shared_ptr<RowSequence> added_;
};
}  // namespace deephaven::client
