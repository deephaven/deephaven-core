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
class TickingCallback : public deephaven::client::utility::FailureCallback {
public:
  /**
   * @param update An update message which describes the changes (removes, adds, modifies) that
   * transform the previous version of the table to the new version. This class is threadsafe and
   * can be kept around for an arbitrary amount of time. On the other hand, it probably should be
   * processed and discard quickly so that the underlying resources can be reused.
   */
  virtual void onTick(TickingUpdate update) = 0;
};

class TickingUpdate final {
protected:
  typedef deephaven::client::container::RowSequence RowSequence;
  typedef deephaven::client::table::Table Table;

public:
  TickingUpdate(std::shared_ptr<Table> prev,
      std::shared_ptr<RowSequence> removedRows, std::shared_ptr<Table> afterRemoves,
      std::shared_ptr<RowSequence> addedRows, std::shared_ptr<Table> afterAdds,
      std::vector<std::shared_ptr<RowSequence>> modifiedRows, std::shared_ptr<Table> afterModifies);
  TickingUpdate(TickingUpdate &&other) noexcept;
  TickingUpdate &operator=(TickingUpdate &&other) noexcept;
  ~TickingUpdate();

  const std::shared_ptr<Table> &prev() const { return prev_; }

  const std::shared_ptr<Table> &beforeRemoves() const { return prev_; }
  const std::shared_ptr<RowSequence> &removedRows() const { return removedRows_; }
  const std::shared_ptr<Table> &afterRemoves() const { return afterRemoves_; }

  const std::shared_ptr<Table> &beforeAdds() const { return afterRemoves_; }
  const std::shared_ptr<RowSequence> &addedRows() const { return addedRows_; }
  const std::shared_ptr<Table> &afterAdds() const { return afterAdds_; }

  const std::shared_ptr<Table> &beforeModifies() const { return afterAdds_; }
  const std::vector<std::shared_ptr<RowSequence>> &modifiedRows() const { return modifiedRows_; }
  const std::shared_ptr<Table> &afterModifies() const { return afterModifies_; }

  const std::shared_ptr<Table> &current() const { return afterModifies_; }

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
