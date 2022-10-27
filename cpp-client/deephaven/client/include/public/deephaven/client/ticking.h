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
class TickingUpdate;
/**
 * Abstract base class used to define the caller's ticking callback object. This object is passed
 * to the TableHandle::subscribe() method.
 */

class TickingCallback {
public:
  /**
   * Invoked on each update to the subscription.
   */
  virtual void onTick(TickingUpdate update) = 0;

  /**
   * Invoked if there is an error involving the subscription.
   */
  virtual void onFailure(std::exception_ptr eptr) = 0;
};

/**
 * An update message (passed to client code via TickingCallback::onTick()) which describes the
 * changes (removes, adds, modifies) that transform the previous version of the table to the new
 * version. This class is threadsafe and can be kept around for an arbitrary amount of time, though
 * this will consume some memory. The underlying snapshots share a common substructure, so the
 * amount of memory they consumed is roughly proportional to the amount of "new" data in that
 * snapshot.
 */
class TickingUpdate final {
public:
  /**
   * Alias.
   */
  typedef deephaven::client::container::RowSequence RowSequence;
  /**
   * Alias.
   */
  typedef deephaven::client::table::Table Table;

  /**
   * Constructor. Used internally.
   */
  TickingUpdate(std::shared_ptr<Table> prev,
      std::shared_ptr<RowSequence> removedRows, std::shared_ptr<Table> afterRemoves,
      std::shared_ptr<RowSequence> addedRows, std::shared_ptr<Table> afterAdds,
      std::vector<std::shared_ptr<RowSequence>> modifiedRows, std::shared_ptr<Table> afterModifies);
  /**
   * Move constructor.
   */
  TickingUpdate(TickingUpdate &&other) noexcept;
  /**
   * Move assignment operator.
   */
  TickingUpdate &operator=(TickingUpdate &&other) noexcept;
  /**
   * Destructor.
   */
  ~TickingUpdate();

  /**
   * A snapshot of the table before any of the changes in this cycle were applied.
   */
  const std::shared_ptr<Table> &prev() const { return prev_; }

  /**
   * A snapshot of the table before any rows were removed in this cycle.
   */
  const std::shared_ptr<Table> &beforeRemoves() const {
    // Implementation detail: 'beforeRemoves' and 'prev' happen to refer to the same snapshot.
    return prev_;
  }
  /**
   * A RowSequence indicating the indexes of the rows (if any) that were removed in this cycle.
   */
  const std::shared_ptr<RowSequence> &removedRows() const { return removedRows_; }
  /**
   * A snapshot of the table after the rows (if any) were removed in this cycle.
   * If no rows were removed, then this pointer will compare equal to beforeRemoves().
   */
  const std::shared_ptr<Table> &afterRemoves() const { return afterRemoves_; }

  /**
   * A snapshot of the table before any rows were added in this cycle.
   */
  const std::shared_ptr<Table> &beforeAdds() const {
    // Implementation detail: 'afterRemoves' and 'beforeAdds' happen to refer to the same snapshot.
    return afterRemoves_;
  }
  /**
   * A RowSequence indicating the indexes of the rows (if any) that were added in this cycle.
   */
  const std::shared_ptr<RowSequence> &addedRows() const { return addedRows_; }
  /**
   * A snapshot of the table after rows (if any) were added in this cycle.
   * If no rows were added, then this pointer will compare equal to beforeAdds().
   */
  const std::shared_ptr<Table> &afterAdds() const { return afterAdds_; }

  /**
   * A snapshot of the table before cells were modified in this cycle.
   */
  const std::shared_ptr<Table> &beforeModifies() const {
    // Implementation detail: 'afterAdds' and 'beforeModifies' happen to refer to the same snapshot.
    return afterAdds_;
  }
  /**
   * A vector of RowSequences which represents, for each column in the table, the indexes of the
   * rows (if any) of the given column that were modified in this cycle.
   */
  const std::vector<std::shared_ptr<RowSequence>> &modifiedRows() const { return modifiedRows_; }
  /**
   * A snapshot of the table after cells (if any) were modified in this cycle.
   */
  const std::shared_ptr<Table> &afterModifies() const { return afterModifies_; }

  /**
   * A snapshot of the table after all of the changes in this cycle were applied.
   */
  const std::shared_ptr<Table> &current() const {
    // Implementation detail: 'afterModifies' and 'current' happen to refer to the same snapshot.
    return afterModifies_;
  }

private:
  std::shared_ptr<Table> prev_;
  std::shared_ptr<RowSequence> removedRows_;
  std::shared_ptr<Table> afterRemoves_;
  std::shared_ptr<RowSequence> addedRows_;
  std::shared_ptr<Table> afterAdds_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRows_;
  std::shared_ptr<Table> afterModifies_;
};
}  // namespace deephaven::client
