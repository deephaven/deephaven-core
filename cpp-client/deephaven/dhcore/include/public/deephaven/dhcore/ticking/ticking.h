/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include "deephaven/dhcore/utility/callbacks.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/clienttable/client_table.h"

namespace deephaven::dhcore::ticking {
class TickingUpdate;

namespace internal {
class OnDemandState {
  /**
   * Alias.
   */
  using RowSequence = deephaven::dhcore::container::RowSequence;
public:
  OnDemandState();
  ~OnDemandState();

  const std::shared_ptr<RowSequence> &AllModifiedRows(
      const std::vector<std::shared_ptr<RowSequence>> &modified_rows);

private:
  std::mutex mutex_;
  std::shared_ptr<RowSequence> allModifiedRows_;
};
}  // namespace internal

/**
 * Abstract base class used to define the caller's ticking callback object. This object is passed
 * to the TableHandle::Subscribe() method.
 */

class TickingCallback {
public:
  virtual ~TickingCallback();

  /**
   * Invoked on each update to the subscription.
   */
  virtual void OnTick(TickingUpdate update) = 0;

  /**
   * Invoked if there is an error involving the subscription.
   */
  virtual void OnFailure(std::exception_ptr eptr) = 0;
};

/**
 * An update message (passed to client code via TickingCallback::OnTick()) which describes the
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
  using RowSequence = deephaven::dhcore::container::RowSequence;
  /**
   * Alias.
   */
  using Table = deephaven::dhcore::clienttable::ClientTable;

  /**
   * Default constructor.
   */
   TickingUpdate();

  /**
   * Constructor. Used internally.
   */
  TickingUpdate(std::shared_ptr<Table> prev,
      std::shared_ptr<RowSequence> removed_rows, std::shared_ptr<Table> after_removes,
      std::shared_ptr<RowSequence> added_rows, std::shared_ptr<Table> after_adds,
      std::vector<std::shared_ptr<RowSequence>> modified_rows, std::shared_ptr<Table> after_modifies);
  /**
   * Copy constructor.
   */
  TickingUpdate(const TickingUpdate &other);
  /**
   * Assignment operator.
   */
  TickingUpdate &operator=(const TickingUpdate &other);
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
  [[nodiscard]]
  const std::shared_ptr<Table> &Prev() const { return prev_; }

  /**
   * A snapshot of the table before any rows were removed in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &BeforeRemoves() const {
    // Implementation detail: 'beforeRemoves' and 'prev' happen to refer to the same snapshot.
    return prev_;
  }
  /**
   * A RowSequence indicating the indexes of the rows (if any) that were removed in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<RowSequence> &RemovedRows() const { return removedRows_; }
  /**
   * A snapshot of the table after the rows (if any) were removed in this cycle.
   * If no rows were removed, then this pointer will compare equal to BeforeRemoves().
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &AfterRemoves() const { return afterRemoves_; }

  /**
   * A snapshot of the table before any rows were added in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &BeforeAdds() const {
    // Implementation detail: 'afterRemoves' and 'beforeAdds' happen to refer to the same snapshot.
    return afterRemoves_;
  }
  /**
   * A RowSequence indicating the indexes of the rows (if any) that were added in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<RowSequence> &AddedRows() const { return addedRows_; }
  /**
   * A snapshot of the table after rows (if any) were added in this cycle.
   * If no rows were added, then this pointer will compare equal to BeforeAdds().
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &AfterAdds() const { return afterAdds_; }

  /**
   * A snapshot of the table before cells were modified in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &BeforeModifies() const {
    // Implementation detail: 'afterAdds' and 'beforeModifies' happen to refer to the same snapshot.
    return afterAdds_;
  }
  /**
   * A vector of RowSequences which represents, for each column in the table, the indexes of the
   * rows (if any) of the given column that were modified in this cycle.
   */
  [[nodiscard]]
  const std::vector<std::shared_ptr<RowSequence>> &ModifiedRows() const { return modifiedRows_; }
  /**
   * A RowSequence which represents the union of the RowSequences returned By ModifiedRows().
   */
  [[nodiscard]]
  const std::shared_ptr<RowSequence> &AllModifiedRows() const {
    return onDemandState_->AllModifiedRows(modifiedRows_);
  }
  /**
   * A snapshot of the table after cells (if any) were modified in this cycle.
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &AfterModifies() const { return afterModifies_; }

  /**
   * A snapshot of the table after all of the changes in this cycle were applied.
   */
  [[nodiscard]]
  const std::shared_ptr<Table> &Current() const {
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
  std::shared_ptr<internal::OnDemandState> onDemandState_;
};
}  // namespace deephaven::dhcore::ticking

