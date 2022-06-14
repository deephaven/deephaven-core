/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdlib>
#include <map>
#include <set>
#include <arrow/type.h>
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"
#include "deephaven/client/highlevel/sad/sad_table.h"

namespace deephaven::client::highlevel {
class TickingCallback : public deephaven::client::utility::FailureCallback {
protected:
  typedef deephaven::client::highlevel::sad::SadRowSequence SadRowSequence;
  typedef deephaven::client::highlevel::sad::SadTable SadTable;

public:
  /**
   * @param tableView A "view" on the updated table, after the changes in this delta have been
   * applied. Although this "view" shared underlying data with other TableViews, it is threadsafe
   * and can be kept around for an arbitrarily long time. (an aspirational statement that will
   * eventually be true, even though it's false at the moment).
   */
  virtual void onTick(const std::shared_ptr<SadTable> &tableView) = 0;
};
}  // namespace deephaven::client::highlevel
