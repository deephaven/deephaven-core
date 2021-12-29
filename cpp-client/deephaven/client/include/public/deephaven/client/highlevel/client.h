#pragma once

#include <future>
#include <memory>
#include <string_view>
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

/**
 * This class provides an interface to Arrow Flight, which is the main way to push data into and
 * get data out of the system.
 */
class FlightWrapper {
public:
  /**
   * Constructor. Used internally.
   */
  explicit FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl);
  /**
   * Destructor
   */
  ~FlightWrapper();

  /**
   * Construct an Arrow FlightStreamReader that is set up to read the given TableHandle.
   * @param table The table to read from.
   * @return An Arrow FlightStreamReader
   */
  std::shared_ptr<arrow::flight::FlightStreamReader> getFlightStreamReader(
      const TableHandle &table) const;

  /**
   * Add Deephaven authentication headers to Arrow FlightCallOptions.
   * This is a bit of a hack, and is used in the scenario where the caller is rolling
   * their own Arrow Flight `DoPut` operation. Example code might look like this:
   * @code
   *   // Get a FlightWrapper
   *   auto wrapper = manager.createFlightWrapper();
   *   // Get a
   *   auto [result, fd] = manager.newTableHandleAndFlightDescriptor();
   *   // Empty FlightCallOptions
   *   arrow::flight::FlightCallOptions options;
   *   // add Deephaven auth headers to the FlightCallOptions
   *   wrapper.addAuthHeaders(&options);
   *   std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
   *   std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
   *   auto status = wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr);
   * @endcode
   * @param options Destination object where the authentication headers should be written.
   */
  void addAuthHeaders(arrow::flight::FlightCallOptions *options);

  /**
   * Gets the underlying FlightClient
   * @return A pointer to the FlightClient.
   */
  arrow::flight::FlightClient *flightClient() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

/**
 * This class is the main way to get access to new TableHandle objects, via methods like emptyTable()
 * and fetchTable(). A TableHandleManager is created by Client::getManager(). You can have more than
 * one TableHandleManager for a given client. The reason you'd want more than one is that (in the
 * future) you will be able to set parameters here that will apply to all TableHandles created
 * by this class, such as flags that control asynchronous behavior.
 * This class is move-only.
 */
class TableHandleManager {
public:
  /**
   * Constructor. Used internally.
   */
  explicit TableHandleManager(std::shared_ptr<impl::TableHandleManagerImpl> impl);
  /**
   * Move constructor
   */
  TableHandleManager(TableHandleManager &&other) noexcept;
  /**
   * Move assigment operator.
   */
  TableHandleManager &operator=(TableHandleManager &&other) noexcept;
  /**
   * Destructor
   */
  ~TableHandleManager();

  /**
   * Creates a "zero-width" table on the server. Such a table knows its number of rows but has no columns.
   * @param size Number of rows in the empty table.
   */
  TableHandle emptyTable(int64_t size) const;
  /**
   * Looks up an existing table by name.
   * @param tableName The name of the table.
   */
  TableHandle fetchTable(std::string tableName) const;
  /**
   * Creates a ticking table.
   * @param startTimeNanos When the table should start ticking (in units of nanoseconds since the epoch).
   * @param periodNanos Table ticking frequency (in nanoseconds).
   * @return The TableHandle of the new table.
   */
  TableHandle timeTable(int64_t startTimeNanos, int64_t periodNanos) const;
  /**
   * Creates a ticking table. This is an overload of timeTable(int64_t, int64_t) const that takes
   * different parameter types.
   * @param startTime When the table should start ticking.
   * @param periodNanos Table ticking frequency.
   * @return The TableHandle of the new table.
   */
  TableHandle timeTable(std::chrono::system_clock::time_point startTime,
      std::chrono::system_clock::duration period) const;
  /**
   * Allocate a fresh TableHandle and return both it and its corresponding Arrow FlightDescriptor.
   * This is used when the caller wants to do an Arrow DoPut operation.
   * @return A TableHandle and Arrow FlightDescriptor referring to the new table.
   */
  std::tuple<TableHandle, arrow::flight::FlightDescriptor> newTableHandleAndFlightDescriptor() const;
  /**
   * Creates a FlightWrapper that is used for Arrow Flight integration. Arrow Flight is the primary
   * way to push data into or pull data out of the system.
   * @return A FlightWrapper object.
   */
  FlightWrapper createFlightWrapper() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

/**
 * The main class for interacting with Deephaven. Start here to connect with
 * the server and to get a TableHandleManager.
 */
class Client {
  template<typename... Args>
  using SFCallback = deephaven::client::utility::SFCallback<Args...>;

public:
  /**
   * Factory method to connect to a Deephaven server
   * @param target A connection string in the format host:port. For example "localhost:10000".
   * @return A Client object conneted to the Deephaven server.
   */
  static Client connect(const std::string &target);
  /**
   * Move constructor
   */
  Client(Client &&other) noexcept;
  /**
   * Move assigment operator.
   */
  Client &operator=(Client &&other) noexcept;
  /**
   * Destructor
   */
  ~Client();

  /**
   * Gets a TableHandleManager which you can use to create empty tables, fetch tables, and so on.
   * You can create more than one TableHandleManager.
   * @return The TableHandleManager.
   */
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
  static Aggregate group(Args &&... args);
  static Aggregate group(std::vector<std::string> columnSpecs);

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
Aggregate aggGroup(Args &&... args) {
  return Aggregate::group(std::forward<Args>(args)...);
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

/**
 * Holds an reference to a server resource representing a table. TableHandle objects have shared
 * ownership semantics so they can be copied freely. When the last TableHandle pointing to a
 * server resource is destructed, the resource will be released.
 */
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
  TableHandle();
  /**
   * Constructor. Used internally.
   */
  explicit TableHandle(std::shared_ptr<impl::TableHandleImpl> impl);
  /**
   * Copy constructor.
   */
  TableHandle(const TableHandle &other);
  /**
   * Copy assignment.
   */
  TableHandle &operator=(const TableHandle &other);
  /**
   * Move constructor.
   */
  TableHandle(TableHandle &&other) noexcept;
  /**
   * Move assignment.
   */
  TableHandle &operator=(TableHandle &&other) noexcept;
  /**
   * Destructor
   */
  ~TableHandle();

  /**
   * Get the TableHandlerManager that is managing this TableHandle.
   * @return the TableHandleManager.
   */
  TableHandleManager getManager() const;

  /**
   * A variadic form of select(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The arguments to select
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle select(Args && ...args) const;
  /**
   * Select columnSpecs from a table. The columnSpecs can be column names or formulas like
   * "NewCol = A + 12". See the Deephaven documentation for the difference between "select" and
   * "view".
   * @param columnSpecs The columnSpecs
   * @return A TableHandle referencing the new table
   */
  TableHandle select(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of view(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The arguments to view
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle view(Args &&... args) const;
  /**
   * View columnSpecs from a table. The columnSpecs can be column names or formulas like
   * "NewCol = A + 12". See the Deephaven documentation for the difference between select() and
   * view().
   * @param columnSpecs The columnSpecs to view
   * @return A TableHandle referencing the new table
   */
  TableHandle view(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of dropColumns(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to drop
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle dropColumns(Args &&... args) const;
  /**
   * Creates a new table from this table where the specified columns have been excluded.
   * @param columnSpecs The columns to exclude.
   * @return
   */
  TableHandle dropColumns(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of update(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to update
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle update(Args &&... args) const;
  /**
   * Creates a new table from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between update() and updateView().
   * @return A TableHandle referencing the new table
   */
  TableHandle update(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of updateView(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to updateView
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle updateView(Args &&... args) const;
  /**
   * Creates a new view from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between update() and updateView().
   * @return A TableHandle referencing the new table
   */
  TableHandle updateView(std::vector<std::string> columnSpecs) const;

  /**
   * A structured form of where(std::string) const, but which takes the Fluent syntax.
   * @param condition A Deephaven fluent BooleanExpression such as `Price > 100` or `Col3 == Col1 * Col2`
   * @return A TableHandle referencing the new table
   */
  TableHandle where(const BooleanExpression &condition) const;
  /**
   * Creates a new table from this table, filtered by condition. Consult the Deephaven
   * documentation for more information about valid conditions.
   * @param condition A Deephaven boolean expression such as "Price > 100" or "Col3 == Col1 * Col2".
   * @return A TableHandle referencing the new table
   */
  TableHandle where(std::string condition) const;

  /**
   * Creates a new table from this table, sorted by sortPairs.
   * @param sortPairs A vector of SortPair objects describing the sort. Each SortPair refers to
   *   a column, a sort direction, and whether the sort should consider to the value's regular or
   *   absolute value when doing comparisons.
   * @return A TableHandle referencing the new table
   */
  TableHandle sort(std::vector<SortPair> sortPairs) const;

  /**
   * A variadic form of by(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle by(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs with the column content grouped
   * into arrays.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle by(std::vector<std::string> columnSpecs) const;
  // TODO(kosak): document
  TableHandle by(AggregateCombo combo, std::vector<std::string> groupByColumns) const;

  // TODO(kosak): document
  template<typename ...Args>
  TableHandle by(AggregateCombo combo, Args &&... args) const;

  /**
   * A variadic form of minBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle minBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "min" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle minBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of maxBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle maxBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "max" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle maxBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of sumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle sumBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "sum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle sumBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of absSumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle absSumBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "absSum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle absSumBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of varBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle varBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "var" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle varBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of stdBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle stdBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "std" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle stdBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of avgBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle avgBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "avg" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle avgBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of firstBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle firstBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "first" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle firstBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of lastBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle lastBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "last" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle lastBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of medianBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle medianBy(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "median" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle medianBy(std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of percentileBy(double, bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle percentileBy(double percentile, bool avgMedian, Args &&... args) const;
  // TODO(kosak): document avgMedian
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle percentileBy(double percentile, bool avgMedian, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of percentileBy(double, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle percentileBy(double percentile, Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle percentileBy(double percentile, std::vector<std::string> columnSpecs) const;

  // TODO(kosak): I wonder if these are all MatchWithColumns, not SelectColumns
  /**
   * A variadic form of countBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam CCol Any of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param countByColumn The output column
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename CCol, typename ...Args>
  TableHandle countBy(CCol &&countByColumn, Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, having a new column named by
   * `countByColumn` containing the size of each group.
   * @param countByColumn Name of the output column.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle countBy(std::string countByColumn, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of wAvgBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam CCol Any of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param weightColumn The output column
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename WCol, typename ...Args>
  TableHandle wAvgBy(WCol &&weightColumn, Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, having a new column named by
   * `weightColumn` containing the weighted average of each group.
   * @param countByColumn Name of the output column.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of tailBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle tailBy(int64_t n, Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, containing the last `n` rows of
   * each group.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle tailBy(int64_t n, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of headBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle headBy(int64_t n, Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, containing the first `n` rows of
   * each group.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle headBy(int64_t n, std::vector<std::string> columnSpecs) const;

  /**
   * Creates a new table from this table containing the first `n` rows of this table.
   * @param n Number of rows
   * @return A TableHandle referencing the new table
   */
  TableHandle head(int64_t n) const;
  /**
   * Creates a new table from this table containing the last `n` rows of this table.
   * @param n Number of rows
   * @return A TableHandle referencing the new table
   */
  TableHandle tail(int64_t n) const;

  //TODO(kosak): document nullFill
  /**
   * A variadic form of ungroup(bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle ungroup(bool nullFill, Args &&... args) const;
  //TODO(kosak): document nullFill
  /**
   * Creates a new table from this table with the column array data ungrouped. This is the inverse
   * of the by(std::vector<std::string>) const operation.
   * @param groupByColumns Columns to ungroup.
   * @return A TableHandle referencing the new table
   */
  TableHandle ungroup(bool nullFill, std::vector<std::string> groupByColumns) const;

  /**
   * A variadic form of ungroup(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle ungroup(Args &&... args) const {
    return ungroup(false, std::forward<Args>(args)...);
  }
  /**
   * Creates a new table from this table with the column array data ungrouped. This is the inverse
   * of the by(std::vector<std::string>) const operation.
   * @param groupByColumns Columns to ungroup.
   * @return A TableHandle referencing the new table
   */
  TableHandle ungroup(std::vector<std::string> groupByColumns) const {
    return ungroup(false, std::move(groupByColumns));
  }

  //TODO(kosak): document keyColumn
  /**
   * Creates a new table by merging `sources` together. The tables are essentially stacked on top
   * of each other.
   * @param sources The tables to merge.
   * @return A TableHandle referencing the new table
   */
  TableHandle merge(std::string keyColumn, std::vector<TableHandle> sources) const;
  /**
   * Creates a new table by merging `sources` together. The tables are essentially stacked on top
   * of each other.
   * @param sources The tables to merge.
   * @return A TableHandle referencing the new table
   */
  TableHandle merge(std::vector<TableHandle> sources) const {
    // TODO(kosak): may need to support null
    return merge("", std::move(sources));
  }

  /**
   * Creates a new table by cross joining this table with `rightSide`. The tables are joined by
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed by `columnsToAdd`. Example:
   * @code
   * t1.crossJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  TableHandle crossJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  /**
   * The fluent version of crossJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.crossJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  TableHandle crossJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  /**
   * Creates a new table by natural joining this table with `rightSide`. The tables are joined by
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed by `columnsToAdd`. Example:
   * @code
   * t1.naturalJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  TableHandle naturalJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  /**
   * The fluent version of naturalJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.naturalJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  TableHandle naturalJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  /**
   * Creates a new table by exact joining this table with `rightSide`. The tables are joined by
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed by `columnsToAdd`. Example:
   * @code
   * t1.exactJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  TableHandle exactJoin(const TableHandle &rightSide, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columnsToAdd) const;
  /**
   * The fluent version of exactJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.exactJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  TableHandle exactJoin(const TableHandle &rightSide, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columnsToAdd) const;

  /**
   * Binds this table to a variable name in the QueryScope.
   * @param variable
   */
  void bindToVariable(std::string variable) const;
  /**
   * The async version of bindToVariable(std::string variable) const.
   * @param The QueryScope variable to bind to.
   * @param callback The asynchronous callback.
   */
  void bindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback) const;

  /**
   * Get all the table's columns.
   * @return A vector of the table's columns.
   */
  std::vector<Column> getAllCols() const;

  /**
   * Get a column that is of string type. Used in the fluent interface. Example:
   * @code
   * auto symbol = table.getStrCol("Symbol")
   * auto t2 = table.where(symbol == "IBM");
   * @endcode
   * @param columnName The name of the column
   * @return The specified StrCol.
   */
  StrCol getStrCol(std::string columnName) const;
  /**
   * Get a column that is of numeric type. Used in the fluent interface. Example:
   * @code
   * auto volume = table.getNumCol("Volume")
   * auto t2 = table.where(volume < 1000);
   * @endcode
   * @param columnName The name of the column
   * @return The specified NumCol.
   */
  NumCol getNumCol(std::string columnName) const;
  /**
   * Get a column that is of DateTime type. Used in the fluent interface.
   * @param columnName The name of the column
   * @return The specified DateTimeCol.
   */
  DateTimeCol getDateTimeCol(std::string columnName) const;

  // internal::StringHolder is a trick to make sure we are passed as many strings as there are
  // template arguments.
  /**
    * Convenience function to get several columns at once. Example:
    * @code
    * auto [symbol, volume] = table.getCols<StrCol, NumCol>("Symbol", "Volume");
    * @endcode
    * @tparam Cols Column types
    * @param names Column names
    * @return A tuple of columns.
    */
  template<typename... Cols>
  std::tuple<Cols...> getCols(internal::StringHolder<Cols>... names);

  /**
   * Create an ostream adpator that prints a human-readable representation of the table to a
   * C++ stream. Example:
   * @code
   * std::cout << table.stream(true) << std::endl;
   * @endcode
   * @param wantHeaders Include column headers.
   * @return
   */
  internal::TableHandleStreamAdaptor stream(bool wantHeaders) const;

  // for debugging
  void observe() const;

  const std::shared_ptr<impl::TableHandleImpl> &impl() const { return impl_; }

  /**
   * Construct an arrow FlightStreamReader bound to this table
   * @return the Arrow FlightStreamReader.
   */
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

  static std::string toString(std::string_view sv) {
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
Aggregate Aggregate::group(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::toString(std::forward<Args>(args))...
  };
  return group(std::move(columnSpecs));
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
