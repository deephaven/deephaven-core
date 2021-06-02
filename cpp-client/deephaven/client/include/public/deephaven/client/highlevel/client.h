#pragma once

#include <future>
#include <memory>
#include <absl/strings/string_view.h>
#include <arrow/flight/client.h>
#include "deephaven/client/highlevel/columns.h"
#include "deephaven/client/highlevel/expressions.h"
#include "deephaven/client/utility/callbacks.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class AggregateComboImpl;
class AggregateImpl;
class ClientImpl;
class TableHandleImpl;
class TableHandleManagerImpl;
}  // namespace impl
class Client;
class TableHandle;
class TableHandleManager;

namespace internal {
class TableHandleStreamAdaptor;
}  // namespace

class FlightWrapper {
public:
  explicit FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl);
  ~FlightWrapper();

  std::shared_ptr<arrow::flight::FlightStreamReader> getFlightStreamReader(
      const TableHandle &table) const;

  void addAuthHeaders(arrow::flight::FlightCallOptions *options);

  arrow::flight::FlightClient *flightClient() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

class TableHandleManager {
  template<typename T>
  using SFCallback = deephaven::client::utility::SFCallback<T>;

public:
  explicit TableHandleManager(std::shared_ptr<impl::TableHandleManagerImpl> impl);
  TableHandleManager(TableHandleManager &&other) noexcept;
  TableHandleManager &operator=(TableHandleManager &&other) noexcept;
  ~TableHandleManager();

  TableHandle emptyTable(int64_t size) const;
  TableHandle fetchTable(std::string tableName) const;
  TableHandle timeTable(int64_t startTimeNanos, int64_t periodNanos) const;
  TableHandle timeTable(std::chrono::system_clock::time_point startTime,
      std::chrono::system_clock::duration period) const;

  FlightWrapper createFlightWrapper() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

class Client {
  template<typename... Args>
  using SFCallback = deephaven::client::utility::SFCallback<Args...>;

public:
  static Client connect(const std::string &target);

  Client(Client &&other) noexcept;
  Client &operator=(Client &&other) noexcept;
  ~Client();

  TableHandleManager getManager() const;

private:
  Client(std::shared_ptr<impl::ClientImpl> impl);
  std::shared_ptr<impl::ClientImpl> impl_;
};

class Aggregate {
public:
  template<typename ...Args>
  static Aggregate absSum(Args &&... args);
  static Aggregate absSum(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate array(Args &&... args);
  static Aggregate array(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate avg(Args &&... args);
  static Aggregate avg(std::vector<std::string> columnSpecs);

  template<typename ColArg>
  static Aggregate count(ColArg &&arg);
  static Aggregate count(std::string columnSpec);

  template<typename ...Args>
  static Aggregate first(Args &&... args);
  static Aggregate first(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate last(Args &&... args);
  static Aggregate last(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate max(Args &&... args);
  static Aggregate max(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate med(Args &&... args);
  static Aggregate med(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate min(Args &&... args);
  static Aggregate min(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate pct(double percentile, bool avgMedian, Args &&... args);
  static Aggregate pct(double percentile, bool avgMedian, std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate std(Args &&... args);
  static Aggregate std(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate sum(Args &&... args);
  static Aggregate sum(std::vector<std::string> columnSpecs);

  template<typename ...Args>
  static Aggregate var(Args &&... args);
  static Aggregate var(std::vector<std::string> columnSpecs);

  template<typename ColArg, typename ...Args>
  static Aggregate wavg(ColArg &&weightColumn, Args &&... args);
  static Aggregate wavg(std::string weightColumn, std::vector<std::string> columnSpecs);

  explicit Aggregate(std::shared_ptr<impl::AggregateImpl> impl);

  const std::shared_ptr<impl::AggregateImpl> &impl() const { return impl_; }

private:
  std::shared_ptr<impl::AggregateImpl> impl_;
};

class AggregateCombo {
public:
  static AggregateCombo create(std::initializer_list<Aggregate> list);
  static AggregateCombo create(std::vector<Aggregate> vec);

  ~AggregateCombo();

  const std::shared_ptr<impl::AggregateComboImpl> &impl() const { return impl_; }

private:
  explicit AggregateCombo(std::shared_ptr<impl::AggregateComboImpl> impl);

  std::shared_ptr<impl::AggregateComboImpl> impl_;
};

template<typename ...Args>
Aggregate aggAbsSum(Args &&... args) {
  return Aggregate::absSum(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggArray(Args &&... args) {
  return Aggregate::array(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggAvg(Args &&... args) {
  return Aggregate::avg(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggCount(Args &&... args) {
  return Aggregate::count(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggFirst(Args &&... args) {
  return Aggregate::first(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggLast(Args &&... args) {
  return Aggregate::last(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggMax(Args &&... args) {
  return Aggregate::max(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggMed(Args &&... args) {
  return Aggregate::med(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggMin(Args &&... args) {
  return Aggregate::min(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggPct(double percentile, Args &&... args) {
  return Aggregate::pct(percentile, std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggStd(Args &&... args) {
  return Aggregate::std(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggSum(Args &&... args) {
  return Aggregate::sum(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggVar(Args &&... args) {
  return Aggregate::var(std::forward<Args>(args)...);
}

template<typename ...Args>
Aggregate aggWavg(Args &&... args) {
  return Aggregate::wavg(std::forward<Args>(args)...);
}

inline AggregateCombo aggCombo(std::initializer_list<Aggregate> args) {
  return AggregateCombo::create(args);
}

namespace internal {
// Holds a string, regardless of T. Used for GetCols
template<typename T>
struct StringHolder {
  StringHolder(std::string s) : s_(std::move(s)) {}
  StringHolder(const char *s) : s_(s) {}
  StringHolder(std::string_view s) : s_(s) {}

  std::string s_;
};
}  // namespace internal

class TableHandle {
  typedef deephaven::client::highlevel::BooleanExpression BooleanExpression;
  typedef deephaven::client::highlevel::Column Column;
  typedef deephaven::client::highlevel::DateTimeCol DateTimeCol;
  typedef deephaven::client::highlevel::MatchWithColumn MatchWithColumn;
  typedef deephaven::client::highlevel::NumCol NumCol;
  typedef deephaven::client::highlevel::SelectColumn SelectColumn;
  typedef deephaven::client::highlevel::SortPair SortPair;
  typedef deephaven::client::highlevel::StrCol StrCol;

  template<typename... Args>
  using Callback = deephaven::client::utility::Callback<Args...>;

  template<typename... Args>
  using SFCallback = deephaven::client::utility::SFCallback<Args...>;

public:
  explicit TableHandle(std::shared_ptr<impl::TableHandleImpl> impl);
  TableHandle(const TableHandle &other);
  TableHandle &operator=(const TableHandle &other);
  TableHandle(TableHandle &&other) noexcept;
  TableHandle &operator=(TableHandle &&other) noexcept;
  ~TableHandle();

  TableHandleManager getManager() const;

  template<typename ...Args>
  TableHandle select(Args &&... args) const;
  TableHandle select(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle view(Args &&... args) const;
  TableHandle view(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle dropColumns(Args &&... args) const;
  TableHandle dropColumns(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle update(Args &&... args) const;
  TableHandle update(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle updateView(Args &&... args) const;
  TableHandle updateView(std::vector<std::string> columnSpecs) const;

  TableHandle where(const BooleanExpression &condition) const;
  TableHandle where(std::string condition) const;

  TableHandle sort(std::vector<SortPair> sortPairs) const;

  template<typename ...Args>
  TableHandle by(Args &&... args) const;
  TableHandle by(std::vector<std::string> columnSpecs) const;

  TableHandle by(AggregateCombo combo, std::vector<std::string> groupByColumns) const;
  template<typename ...Args>
  TableHandle by(AggregateCombo combo, Args &&... args) const;

  template<typename ...Args>
  TableHandle minBy(Args &&... args) const;
  TableHandle minBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle maxBy(Args &&... args) const;
  TableHandle maxBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle sumBy(Args &&... args) const;
  TableHandle sumBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle absSumBy(Args &&... args) const;
  TableHandle absSumBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle varBy(Args &&... args) const;
  TableHandle varBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle stdBy(Args &&... args) const;
  TableHandle stdBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle avgBy(Args &&... args) const;
  TableHandle avgBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle firstBy(Args &&... args) const;
  TableHandle firstBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle lastBy(Args &&... args) const;
  TableHandle lastBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle medianBy(Args &&... args) const;
  TableHandle medianBy(std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle percentileBy(double percentile, bool avgMedian, Args &&... args) const;
  TableHandle percentileBy(double percentile, bool avgMedian, std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle percentileBy(double percentile, Args &&... args) const;
  TableHandle percentileBy(double percentile, std::vector<std::string> columnSpecs) const;

  template<typename CCol, typename ...Args>
  TableHandle countBy(CCol &&countByColumn, Args &&... args) const;
  TableHandle countBy(std::string countByColumn, std::vector<std::string> columnSpecs) const;

  template<typename WCol, typename ...Args>
  TableHandle wAvgBy(WCol &&weightColumn, Args &&... args) const;
  TableHandle wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle tailBy(int64_t n, Args &&... args) const;
  TableHandle tailBy(int64_t n, std::vector<std::string> columnSpecs) const;

  template<typename ...Args>
  TableHandle headBy(int64_t n, Args &&... args) const;
  TableHandle headBy(int64_t n, std::vector<std::string> columnSpecs) const;

  TableHandle head(int64_t n) const;
  TableHandle tail(int64_t n) const;

  template<typename ...Args>
  TableHandle ungroup(bool nullFill, Args &&... args) const;
  TableHandle ungroup(bool nullFill, std::vector<std::string> groupByColumns) const;

  template<typename ...Args>
  TableHandle ungroup(Args &&... args) const {
    return ungroup(false, std::forward<Args>(args)...);
  }
  TableHandle ungroup(std::vector<std::string> groupByColumns) const {
    return ungroup(false, std::move(groupByColumns));
  }

  TableHandle merge(std::string keyColumn, std::vector<TableHandle> sources) const;
  TableHandle merge(std::vector<TableHandle> sources) const {
    // TODO(kosak): may need to support null
    return merge("", std::move(sources));
  }

  TableHandle crossJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  TableHandle crossJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  TableHandle naturalJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  TableHandle naturalJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  TableHandle exactJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  TableHandle exactJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  TableHandle leftJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  TableHandle leftJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  void bindToVariable(std::string variable) const;
  void bindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback) const;

  std::vector<Column> getAllCols() const;

  StrCol getStrCol(std::string columnName) const;
  NumCol getNumCol(std::string columnName) const;
  DateTimeCol getDateTimeCol(std::string columnName) const;

  // internal::StringHolder is a trick to make sure we are passed as many strings as there are
  // template arguments.
  template<typename... Cols>
  std::tuple<Cols...> getCols(internal::StringHolder<Cols>... names);

  internal::TableHandleStreamAdaptor stream(bool wantHeaders) const;

  // for debugging
  void observe() const;

  const std::shared_ptr<impl::TableHandleImpl> &impl() const { return impl_; }

  std::shared_ptr<arrow::flight::FlightStreamReader> getFlightStreamReader() const;

private:
  std::shared_ptr<impl::TableHandleImpl> impl_;
};

namespace internal {
template<typename Col>
struct ColGetter {
  static_assert(!std::is_same_v<Col, Col>, "Unknown column type");
};

template<>
struct ColGetter<NumCol> {
  static NumCol getCol(const TableHandle &table, std::string name) {
    return table.getNumCol(std::move(name));
  }
};

template<>
struct ColGetter<StrCol> {
  static StrCol getCol(const TableHandle &table, std::string name) {
    return table.getStrCol(std::move(name));
  }
};

template<>
struct ColGetter<DateTimeCol> {
  static DateTimeCol getCol(const TableHandle &table, std::string name) {
    return table.getDateTimeCol(std::move(name));
  }
};
}  // namespace internal

template<typename... Cols>
std::tuple<Cols...> TableHandle::getCols(internal::StringHolder<Cols>... names) {
  return std::make_tuple(
      internal::ColGetter<Cols>::getCol(*this, std::move(names.s_))...
      );
}

namespace internal {
class TableHandleStreamAdaptor {
public:
  TableHandleStreamAdaptor(TableHandle table, bool wantHeaders);
  TableHandleStreamAdaptor(const TableHandleStreamAdaptor &) = delete;
  TableHandleStreamAdaptor &operator=(const TableHandleStreamAdaptor &) = delete;
  ~TableHandleStreamAdaptor();

private:
  TableHandle table_;
  bool wantHeaders_ = false;

  friend std::ostream &operator<<(std::ostream &s, const TableHandleStreamAdaptor &o);
};

struct ConvertToString {
  static std::string toString(const char *s) {
    return s;
  }

  static std::string toString(absl::string_view sv) {
    return {sv.data(), sv.size()};
  }

  static std::string toString(std::string s) {
    return s;
  }

  static std::string toString(const deephaven::client::highlevel::SelectColumn &selectColumn);
};
}  // namespace internal

template<typename ...Args>
Aggregate Aggregate::absSum(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return absSum(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::array(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return array(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::avg(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return avg(std::move(columnSpecs));
}

template<typename ColArg>
Aggregate Aggregate::count(ColArg &&arg) {
  auto columnSpec = internal::ConvertToString::toString(std::forward<ColArg>(arg));
  return count(std::move(columnSpec));
}

template<typename ...Args>
Aggregate Aggregate::first(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return first(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::last(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return last(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::max(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return max(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::med(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return max(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::min(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return min(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::pct(double percentile, bool avgMedian, Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return pct(percentile, avgMedian, std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::std(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return std(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::sum(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return sum(std::move(columnSpecs));
}

template<typename ...Args>
Aggregate Aggregate::var(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return var(std::move(columnSpecs));
}

template<typename ColArg, typename ...Args>
Aggregate Aggregate::wavg(ColArg &&weightColumn, Args &&... args) {
  auto weightCol = internal::ConvertToString::toString(std::forward<ColArg>(weightColumn));
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return wavg(std::move(weightCol), std::move(columnSpecs));
}

template<typename ...Args>
TableHandle TableHandle::select(Args &&... args) const {
  std::vector<std::string> selectColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return select(std::move(selectColumns));
}

template<typename ...Args>
TableHandle TableHandle::view(Args &&... args) const {
  std::vector<std::string> viewColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return view(std::move(viewColumns));
}

template<typename ...Args>
TableHandle TableHandle::dropColumns(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return dropColumns(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::update(Args &&... args) const {
  std::vector<std::string> updateColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return update(std::move(updateColumns));
}

template<typename ...Args>
TableHandle TableHandle::updateView(Args &&... args) const {
  std::vector<std::string> updateColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return updateView(std::move(updateColumns));
}

template<typename ...Args>
TableHandle TableHandle::by(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return by(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::by(AggregateCombo combo, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return by(std::move(combo), std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::minBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return minBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::maxBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return maxBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::sumBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return sumBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::absSumBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return absSumBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::varBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return varBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::stdBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return stdBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::avgBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return avgBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::firstBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return firstBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::lastBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return lastBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::medianBy(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return medianBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::percentileBy(double percentile, bool avgMedian, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return percentileBy(percentile, avgMedian, std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::percentileBy(double percentile, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return percentileBy(percentile, std::move(columns));
}

template<typename CCol, typename ...Args>
TableHandle TableHandle::countBy(CCol &&countByColumn, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return countBy(internal::ConvertToString::toString(countByColumn), std::move(columns));
}

template<typename WCol, typename ...Args>
TableHandle TableHandle::wAvgBy(WCol &&weightColumn, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return wAvgBy(internal::ConvertToString::toString(weightColumn), std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::tailBy(int64_t n, Args &&... args) const {
  std::vector<std::string> lastByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return tailBy(n, std::move(lastByColumns));
}

template<typename ...Args>
TableHandle TableHandle::headBy(int64_t n, Args &&... args) const {
  std::vector<std::string> lastByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return headBy(n, std::move(lastByColumns));
}

template<typename ...Args>
TableHandle TableHandle::ungroup(bool nullFill, Args &&... args) const {
  std::vector<std::string> groupByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return ungroup(nullFill, std::move(groupByColumns));
}
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
