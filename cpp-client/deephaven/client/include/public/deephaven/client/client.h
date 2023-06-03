/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <string_view>
#include "deephaven/client/columns.h"
#include "deephaven/client/expressions.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/callbacks.h"

/**
 * Arrow-related classes, used by TableHandleManager::newTableHandleAndFlightDescriptor() and
 * TableHandleManager::createFlightWrapper. Callers that use those methods need to include
 * deephaven/client/flight.h
 */
namespace deephaven::client {
class FlightWrapper;
class TableHandleAndFlightDescriptor;
}  // namespace deephaven::client

/**
 * Internal impl classes. Their definitions are opaque here.
 */
namespace deephaven::client::impl {
class AggregateComboImpl;
class AggregateImpl;
class ClientImpl;
class TableHandleImpl;
class TableHandleManagerImpl;
}  // namespace deephaven::client::impl

/**
 * Forward reference to arrow's FlightStreamReader
 */
namespace arrow::flight {
class FlightStreamReader;
}  // namespace arrow::flight

/**
 * Opaque class used as a handle to a subscription
 */
namespace deephaven::client::subscription {
class SubscriptionHandle;
} // namespace deephaven::client::subscription

/**
 * Forward references
 */
namespace deephaven::client {
class Client;
class TableHandle;
class TableHandleManager;
namespace internal {
class TableHandleStreamAdaptor;
}  // namespace internal
}  // namespace deephaven::client

namespace deephaven::client {
/**
 * This class is the main way to get access to new TableHandle objects, via methods like emptyTable()
 * and fetchTable(). A TableHandleManager is created by Client::getManager(). You can have more than
 * one TableHandleManager for a given client. The reason you'd want more than one is that (in the
 * future) you will be able to set parameters here that will apply to all TableHandles created
 * by this class, such as flags that control asynchronous behavior.
 * This class is move-only.
 */
class TableHandleManager {
  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;
public:
  /*
   * Default constructor. Creates a (useless) empty client object.
   */
  TableHandleManager();
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
   * The object returned is only forward-referenced in this file. If you want to use it, you will
   * also need to include deephaven/client/flight.h.
   * @param numRows The number of table rows (reflected back when you call TableHandle::numRows())
   * @param isStatic Whether the table is static (reflected back when youcall TableHandle::isStatic())
   * @return A TableHandle and Arrow FlightDescriptor referring to the new table.
   */
  TableHandleAndFlightDescriptor newTableHandleAndFlightDescriptor(int64_t numRows, bool isStatic) const;
  /**
   * Execute a script on the server. This assumes that the Client was created with a sessionType corresponding to
   * the language of the script (typically either "python" or "groovy") and that the code matches that language.
   */
  void runScript(std::string code) const;
  /**
   * The async version of runScript(std::string variable) code.
   * @param code The script to run on the server.
   * @param callback The asynchronous callback.
   */
  void runScriptAsync(std::string code, std::shared_ptr<SFCallback<>> callback) const;

  /**
   * Creates a FlightWrapper that is used for Arrow Flight integration. Arrow Flight is the primary
   * way to push data into or pull data out of the system. The object returned is only
   * forward-referenced in this file. If you want to use it, you will also need to include
   * deephaven/client/flight.h.
   * @return A FlightWrapper object.
   */
  FlightWrapper createFlightWrapper() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

/**
 * The ClientOptions object is intended to be passed to Client::connect(). For convenience, the mutating methods can be
 * chained. For example:
 * auto client = Client::connect("localhost:10000", ClientOptions().setBasicAuthentication("foo", "bar").setSessionType("groovy")
 */
class ClientOptions {
public:
  /*
   * Default constructor. Creates a default ClientOptions object with default authentication and Python scripting.
   */
  ClientOptions();
  /**
   * Move constructor
   */
  ClientOptions(ClientOptions &&other) noexcept;
  /**
   * Move assigment operator.
   */
  ClientOptions &operator=(ClientOptions &&other) noexcept;
  /**
   * Destructor
   */
  ~ClientOptions();

  /**
   * Modifies the ClientOptions object to set the default authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setDefaultAuthentication();
  /**
   * Modifies the ClientOptions object to set the basic authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setBasicAuthentication(const std::string &username, const std::string &password);
  /**
   * Modifies the ClientOptions object to set a custom authentication scheme.
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setCustomAuthentication(const std::string &authenticationKey, const std::string &authenticationValue);
  /**
   * Modifies the ClientOptions object to set the scripting language for the session.
   * @param sessionType The scripting language for the session, such as "groovy" or "python".
   * @return *this, so that methods can be chained.
   */
  ClientOptions &setSessionType(const std::string &sessionType);

private:
  std::string authorizationValue_;
  std::string sessionType_;

  friend class Client;
};

/**
 * The main class for interacting with Deephaven. Start here to connect with
 * the server and to get a TableHandleManager.
 */
class Client {
  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

  typedef deephaven::dhcore::ticking::TickingUpdate TickingUpdate;

public:
  typedef void (*CCallback)(TickingUpdate, void *);

  /*
   * Default constructor. Creates a (useless) empty client object.
   */
  Client();

  /**
   * Factory method to connect to a Deephaven server using the specified options.
   * @param target A connection string in the format host:port. For example "localhost:10000".
   * @param options An options object for setting options like authentication and script language.
   * @return A Client object conneted to the Deephaven server.
   */
  static Client connect(const std::string &target, const ClientOptions &options = {});
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
  explicit Client(std::shared_ptr<impl::ClientImpl> impl);
  std::shared_ptr<impl::ClientImpl> impl_;
};

/**
 * Defines an aggregator class that represents one of a variet of aggregation operations.
 */
class Aggregate {
public:
  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate absSum(Args &&... args);
  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  static Aggregate absSum(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes an array of all values within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate group(Args &&... args);

  /**
   * Returns an aggregator that computes an array of all values within an aggregation group,
   * for each input column.
   */
  static Aggregate group(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate avg(Args &&... args);

  /**
   * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
   * for each input column.
   */
  static Aggregate avg(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the number of elements within an aggregation group.
   */
  template<typename ColArg>
  static Aggregate count(ColArg &&arg);

  /**
   * Returns an aggregator that computes the number of elements within an aggregation group.
   */
  static Aggregate count(std::string columnSpec);

  /**
   * Returns an aggregator that computes the first value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate first(Args &&... args);

  /**
   * Returns an aggregator that computes the first value, within an aggregation group,
   * for each input column.
   */
  static Aggregate first(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the last value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate last(Args &&... args);

  /**
   * Returns an aggregator that computes the last value, within an aggregation group,
   * for each input column.
   */
  static Aggregate last(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the maximum value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate max(Args &&... args);

  /**
   * Returns an aggregator that computes the maximum value, within an aggregation group,
   * for each input column.
   */
  static Aggregate max(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the median value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate med(Args &&... args);

  /**
   * Returns an aggregator that computes the median value, within an aggregation group,
   * for each input column.
   */
  static Aggregate med(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the minimum value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate min(Args &&... args);

  /**
   * Returns an aggregator that computes the minimum value, within an aggregation group,
   * for each input column.
   */
  static Aggregate min(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the designated percentile of values, within an aggregation
   * group, for each input column.
   */
  template<typename ...Args>
  static Aggregate pct(double percentile, bool avgMedian, Args &&... args);

  /**
   * Returns an aggregator that computes the designated percentile of values, within an aggregation
   * group, for each input column.
   */
  static Aggregate pct(double percentile, bool avgMedian, std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the standard deviation of values, within an aggregation
   * group, for each input column.
   */
  template<typename ...Args>
  static Aggregate std(Args &&... args);

  /**
   * Returns an aggregator that computes the standard deviation of values, within an aggregation
   * group, for each input column.
   */
  static Aggregate std(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate sum(Args &&... args);

  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  static Aggregate sum(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the variance of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  static Aggregate var(Args &&... args);

  /**
   * Returns an aggregator that computes the variance of values, within an aggregation group,
   * for each input column.
   */
  static Aggregate var(std::vector<std::string> columnSpecs);

  /**
   * Returns an aggregator that computes the weighted average of values, within an aggregation
   * group, for each input column.
   */
  template<typename ColArg, typename ...Args>
  static Aggregate wavg(ColArg &&weightColumn, Args &&... args);
  /**
   * Returns an aggregator that computes the weighted average of values, within an aggregation
   * group, for each input column.
   */
  static Aggregate wavg(std::string weightColumn, std::vector<std::string> columnSpecs);

  /**
   * Constructor.
   */
  explicit Aggregate(std::shared_ptr<impl::AggregateImpl> impl);

  /**
   * Returns the underlying "impl" object. Used internally.
   */
  const std::shared_ptr<impl::AggregateImpl> &impl() const { return impl_; }

private:
  std::shared_ptr<impl::AggregateImpl> impl_;
};

/**
 * Represents a collection of Aggregate objects.
 */
class AggregateCombo {
public:
  /**
   * Create an AggregateCombo from an initializer list.
   */
  static AggregateCombo create(std::initializer_list<Aggregate> list);
  /**
   * Create an AggregateCombo from a vector.
   */
  static AggregateCombo create(std::vector<Aggregate> vec);

  /**
   * Move constructor.
   */
  AggregateCombo(AggregateCombo &&other) noexcept;
  /**
   * Move assignment operator.
   */
  AggregateCombo &operator=(AggregateCombo &&other) noexcept;

  ~AggregateCombo();

  /**
   * Returns the underlying "impl" object. Used internally.
   */
  const std::shared_ptr<impl::AggregateComboImpl> &impl() const { return impl_; }

private:
  explicit AggregateCombo(std::shared_ptr<impl::AggregateComboImpl> impl);

  std::shared_ptr<impl::AggregateComboImpl> impl_;
};

/**
 * Returns an aggregator that computes the total sum of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggAbsSum(Args &&... args) {
  return Aggregate::absSum(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes an array of all values within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggGroup(Args &&... args) {
  return Aggregate::group(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggAvg(Args &&... args) {
  return Aggregate::avg(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the number of elements within an aggregation group.
 */
template<typename ...Args>
Aggregate aggCount(Args &&... args) {
  return Aggregate::count(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the first value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggFirst(Args &&... args) {
  return Aggregate::first(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the last value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggLast(Args &&... args) {
  return Aggregate::last(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the maximum value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggMax(Args &&... args) {
  return Aggregate::max(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the median value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggMed(Args &&... args) {
  return Aggregate::med(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the minimum value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggMin(Args &&... args) {
  return Aggregate::min(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the designated percentile of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
Aggregate aggPct(double percentile, Args &&... args) {
  return Aggregate::pct(percentile, std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the standard deviation of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
Aggregate aggStd(Args &&... args) {
  return Aggregate::std(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the total sum of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggSum(Args &&... args) {
  return Aggregate::sum(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the variance of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
Aggregate aggVar(Args &&... args) {
  return Aggregate::var(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the weighted average of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
Aggregate aggWavg(Args &&... args) {
  return Aggregate::wavg(std::forward<Args>(args)...);
}

/**
 * Returns an AggregateCombo object, which represents a collection of aggregators.
 */
inline AggregateCombo aggCombo(std::initializer_list<Aggregate> args) {
  return AggregateCombo::create(args);
}

namespace internal {
/**
 * Used internally. Holds a std::string, regardless of T. Used for GetCols.
 * T is ignored but we use it to make sure we are passed the right number of arguments.
 */
template<typename T>
struct StringHolder {
  /**
   * Constructor.
   */
  StringHolder(std::string s) : s_(std::move(s)) {}

  /**
   * Constructor.
   */
  StringHolder(const char *s) : s_(s) {}

  /**
   * Constructor.
   */
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
  typedef deephaven::dhcore::ticking::TickingCallback TickingCallback;
  typedef deephaven::dhcore::ticking::TickingUpdate TickingUpdate;
  typedef deephaven::client::BooleanExpression BooleanExpression;
  typedef deephaven::client::Column Column;
  typedef deephaven::client::DateTimeCol DateTimeCol;
  typedef deephaven::client::MatchWithColumn MatchWithColumn;
  typedef deephaven::client::NumCol NumCol;
  typedef deephaven::client::SelectColumn SelectColumn;
  typedef deephaven::client::SortPair SortPair;
  typedef deephaven::client::StrCol StrCol;
  typedef deephaven::client::subscription::SubscriptionHandle SubscriptionHandle;

  template<typename... Args>
  using Callback = deephaven::dhcore::utility::Callback<Args...>;

  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

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
  TableHandle select(Args &&...args) const;
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
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle minBy(Args &&... columnSpecs) const;
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
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle maxBy(Args &&... columnSpecs) const;
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
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle sumBy(Args &&... columnSpecs) const;
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
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle absSumBy(Args &&... columnSpecs) const;
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
  TableHandle varBy(Args &&... columnSpecs) const;
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
  TableHandle stdBy(Args &&... columnSpecs) const;
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
  TableHandle avgBy(Args &&... columnSpecs) const;
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
  TableHandle firstBy(Args &&... columnSpecs) const;
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
  TableHandle lastBy(Args &&... columnSpecs) const;
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
  TableHandle medianBy(Args &&... columnSpecs) const;
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
  TableHandle percentileBy(double percentile, bool avgMedian, Args &&... columnSpecs) const;
  // TODO(kosak): document avgMedian
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  TableHandle
  percentileBy(double percentile, bool avgMedian, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of percentileBy(double, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle percentileBy(double percentile, Args &&... columnSpecs) const;
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
  TableHandle countBy(CCol &&countByColumn, Args &&... columnSpecs) const;
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
  TableHandle wAvgBy(WCol &&weightColumn, Args &&... columnSpecs) const;
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
  TableHandle tailBy(int64_t n, Args &&... columnSpecs) const;
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
  TableHandle headBy(int64_t n, Args &&... columnSpecs) const;
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
  TableHandle ungroup(bool nullFill, Args &&... columnSpecs) const;
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
  TableHandle ungroup(Args &&... columnSpecs) const {
    return ungroup(false, std::forward<Args>(columnSpecs)...);
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
   * @param variable The QueryScope variable to bind to.
   */
  void bindToVariable(std::string variable) const;
  /**
   * The async version of bindToVariable(std::string variable) const.
   * @param variable The QueryScope variable to bind to.
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

  /**
   * Used internally, for demo purposes.
   */
  std::string toString(bool wantHeaders) const;

  /**
   * Used internally, for debugging.
   */
  void observe() const;

  /**
   * A specialized operation to release the state of this TableHandle. This operation is normally done by the
   * destructor, so most programs will never need to call this method.. If there are no other copies of this
   * TableHandle, and if there are no "child" TableHandles dependent on this TableHandle, then the corresponding server
   * resources will be released.
   */
  void release() {
    impl_.reset();
  }

  /**
   * Number of rows in the table at the time this TableHandle was created.
   */
  int64_t numRows();

  /**
   * Whether the table was static at the time this TableHandle was created.
   */
  bool isStatic();

  /**
   * Used internally. Returns the underlying impl object.
   */
  const std::shared_ptr<impl::TableHandleImpl> &impl() const { return impl_; }

  /**
   * Construct an arrow FlightStreamReader bound to this table
   * @return the Arrow FlightStreamReader.
   */
  std::shared_ptr<arrow::flight::FlightStreamReader> getFlightStreamReader() const;

  /**
   * Subscribe to a ticking table.
   */
  std::shared_ptr<SubscriptionHandle> subscribe(std::shared_ptr<TickingCallback> callback);

  typedef void (*onTickCallback_t)(TickingUpdate update, void *userData);
  typedef void (*onErrorCallback_t)(std::string errorMessage, void *userData);
  /**
   * Subscribe to a ticking table (C-style).
   */
  std::shared_ptr<SubscriptionHandle> subscribe(onTickCallback_t onTick, void *onTickUserData,
      onErrorCallback_t onError, void *onErrorUserData);
  /**
   * Unsubscribe from the table.
   */
  void unsubscribe(std::shared_ptr<SubscriptionHandle> callback);

  /**
   * Get access to the bytes of the Deephaven "Ticket" type (without having to reference the
   * protobuf data structure, which we would prefer not to have a dependency on here)
   */
  const std::string &getTicketAsString() const;

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

  static std::string toString(const deephaven::client::SelectColumn &selectColumn);
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
TableHandle TableHandle::by(AggregateCombo combo, Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return by(std::move(combo), std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::minBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return minBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::maxBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return maxBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::sumBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return sumBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::absSumBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return absSumBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::varBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return varBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::stdBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return stdBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::avgBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return avgBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::firstBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return firstBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::lastBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return lastBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::medianBy(Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return medianBy(std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::percentileBy(double percentile, bool avgMedian, Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return percentileBy(percentile, avgMedian, std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::percentileBy(double percentile, Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return percentileBy(percentile, std::move(columns));
}

template<typename CCol, typename ...Args>
TableHandle TableHandle::countBy(CCol &&countByColumn, Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return countBy(internal::ConvertToString::toString(countByColumn), std::move(columns));
}

template<typename WCol, typename ...Args>
TableHandle TableHandle::wAvgBy(WCol &&weightColumn, Args &&... columnSpecs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return wAvgBy(internal::ConvertToString::toString(weightColumn), std::move(columns));
}

template<typename ...Args>
TableHandle TableHandle::tailBy(int64_t n, Args &&... columnSpecs) const {
  std::vector<std::string> lastByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return tailBy(n, std::move(lastByColumns));
}

template<typename ...Args>
TableHandle TableHandle::headBy(int64_t n, Args &&... columnSpecs) const {
  std::vector<std::string> lastByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return headBy(n, std::move(lastByColumns));
}

template<typename ...Args>
TableHandle TableHandle::ungroup(bool nullFill, Args &&... columnSpecs) const {
  std::vector<std::string> groupByColumns = {
      internal::ConvertToString::toString(std::forward<Args>(columnSpecs))...
  };
  return ungroup(nullFill, std::move(groupByColumns));
}
}  // namespace deephaven::client
