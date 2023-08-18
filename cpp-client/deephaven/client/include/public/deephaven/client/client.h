/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <string_view>
#include "deephaven/client/columns.h"
#include "deephaven/client/client_options.h"
#include "deephaven/client/expressions.h"
#include "deephaven/client/utility/misc_types.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/callbacks.h"

/**
 * Arrow-related classes, used by TableHandleManager::newTableHandleAndFlightDescriptor() and
 * TableHandleManager::CreateFlightWrapper. Callers that use those methods need to include
 * deephaven/client/flight.h
 */
namespace deephaven::client {
class FlightWrapper;
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
class UpdateByOperation;
namespace internal {
class TableHandleStreamAdaptor;
}  // namespace internal
}  // namespace deephaven::client

namespace deephaven::client {
/**
 * This class is the main way to get access to new TableHandle objects, via methods like EmptyTable()
 * and FetchTable(). A TableHandleManager is created by Client::GetManager(). You can have more than
 * one TableHandleManager for a given client. The reason you'd want more than one is that (in the
 * future) you will be able to set parameters here that will apply to all TableHandles created
 * by this class, such as flags that control asynchronous behavior.
 * This class is move-only.
 */
class TableHandleManager {
  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

  using DurationSpecifier = deephaven::client::utility::DurationSpecifier;
  using TimePointSpecifier = deephaven::client::utility::TimePointSpecifier;

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
  [[nodiscard]]
  TableHandle EmptyTable(int64_t size) const;
  /**
   * Looks up an existing table by name.
   * @param tableName The name of the table.
   */
  [[nodiscard]]
  TableHandle FetchTable(std::string table_name) const;
  /**
   * Creates a ticking table.
   * @param period Table ticking frequency, specified as a std::chrono::duration,
   *   int64_t nanoseconds, or a string containing an ISO 8601 duration representation.
   * @param start_time When the table should start ticking, specified as a std::chrono::time_point,
   *   int64_t nanoseconds since the epoch, or a string containing an ISO 8601 time point specifier.
   * @return The TableHandle of the new table.
   */
  [[nodiscard]]
  TableHandle TimeTable(DurationSpecifier period, TimePointSpecifier start_time = 0,
      bool blink_table = false) const;
  /**
   * Allocate a fresh client ticket. This is a low level operation, typically used when the caller wants to do an Arrow
   * doPut operation.
   * @example
   * auto ticket = manager.NewTicket();
   * auto flightDescriptor = ConvertTicketToFlightDescriptor(ticket);
   * // [do arrow operations here to put your table to the server]
   * // Once that is done, you can bind the ticket to a TableHandle
   * auto tableHandle = manager.MakeTableHandleFromTicket(ticket);
   */
  [[nodiscard]]
  std::string NewTicket() const;
  /**
   * Creates a TableHandle that owns the underlying ticket and its resources. The ticket argument is typically
   * created with NewTicket() and then populated e.g. with Arrow operations.
   */
  [[nodiscard]]
  TableHandle MakeTableHandleFromTicket(std::string ticket) const;
  /**
   * Execute a script on the server. This assumes that the Client was created with a sessionType corresponding to
   * the language of the script (typically either "python" or "groovy") and that the code matches that language.
   */
  void RunScript(std::string code) const;
  /**
   * The async version of RunScript(std::string variable) code.
   * @param code The script to run on the server.
   * @param callback The asynchronous callback.
   */
  void RunScriptAsync(std::string code, std::shared_ptr<SFCallback<>> callback) const;

  /**
   * Creates a FlightWrapper that is used for Arrow Flight integration. Arrow Flight is the primary
   * way to push data into or pull data out of the system. The object returned is only
   * forward-referenced in this file. If you want to use it, you will also need to include
   * deephaven/client/flight.h.
   * @return A FlightWrapper object.
   */
  [[nodiscard]]
  FlightWrapper CreateFlightWrapper() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};


/**
 * The main class for interacting with Deephaven. Start here to Connect with
 * the server and to get a TableHandleManager.
 */
class Client {
  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

public:
  /*
   * Default constructor. Creates a (useless) empty client object.
   */
  Client();

  /**
   * Factory method to Connect to a Deephaven server using the specified options.
   * @param target A connection string in the format host:port. For example "localhost:10000".
   * @param options An options object for setting options like authentication and script language.
   * @return A Client object conneted to the Deephaven server.
   */
  [[nodiscard]]
  static Client Connect(const std::string &target, const ClientOptions &options = {});
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
   * Shuts down the Client and all associated state (GRPC connections, subscriptions, etc).
   * This method is used if a caller wants to shut down Client state early. If it is not called,
   * the shutdown actions will happen when this Client is destructed. The caller must not use any
   * associated data structures (TableHandleManager, TableHandle, etc) after Close() is called or
   * after Client's destructor is invoked. If the caller tries to do so, the behavior is
   * unspecified.
   */
  void Close();

  /**
   * Gets a TableHandleManager which you can use to create empty tables, fetch tables, and so on.
   * You can create more than one TableHandleManager.
   * @return The TableHandleManager.
   */
  [[nodiscard]]
  TableHandleManager GetManager() const;

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
  [[nodiscard]]
  static Aggregate AbsSum(Args &&... args);
  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate AbsSum(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes an array of all values within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Group(Args &&... args);

  /**
   * Returns an aggregator that computes an array of all values within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Group(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Avg(Args &&... args);

  /**
   * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Avg(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the number of elements within an aggregation group.
   */
  template<typename ColArg>
  [[nodiscard]]
  static Aggregate Count(ColArg &&arg);

  /**
   * Returns an aggregator that computes the number of elements within an aggregation group.
   */
  [[nodiscard]]
  static Aggregate Count(std::string column_spec);

  /**
   * Returns an aggregator that computes the first value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate First(Args &&... args);

  /**
   * Returns an aggregator that computes the first value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate First(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the last value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Last(Args &&... args);

  /**
   * Returns an aggregator that computes the last value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Last(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the maximum value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Max(Args &&... args);

  /**
   * Returns an aggregator that computes the maximum value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Max(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the median value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Med(Args &&... args);

  /**
   * Returns an aggregator that computes the median value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Med(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the minimum value, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Min(Args &&... args);

  /**
   * Returns an aggregator that computes the minimum value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Min(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the designated percentile of values, within an aggregation
   * group, for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Pct(double percentile, bool avg_median, Args &&... args);

  /**
   * Returns an aggregator that computes the designated percentile of values, within an aggregation
   * group, for each input column.
   */
  [[nodiscard]]
  static Aggregate Pct(double percentile, bool avg_median, std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the standard deviation of values, within an aggregation
   * group, for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Std(Args &&... args);

  /**
   * Returns an aggregator that computes the standard deviation of values, within an aggregation
   * group, for each input column.
   */
  [[nodiscard]]
  static Aggregate Std(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Sum(Args &&... args);

  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Sum(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the variance of values, within an aggregation group,
   * for each input column.
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Var(Args &&... args);

  /**
   * Returns an aggregator that computes the variance of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Var(std::vector<std::string> column_specs);

  /**
   * Returns an aggregator that computes the weighted average of values, within an aggregation
   * group, for each input column.
   */
  template<typename ColArg, typename ...Args>
  [[nodiscard]]
  static Aggregate WAvg(ColArg &&weight_column, Args &&... args);
  /**
   * Returns an aggregator that computes the weighted average of values, within an aggregation
   * group, for each input column.
   */
  [[nodiscard]]
  static Aggregate WAvg(std::string weight_column, std::vector<std::string> column_specs);

  /**
   * Constructor.
   */
  explicit Aggregate(std::shared_ptr<impl::AggregateImpl> impl);

  /**
   * Returns the underlying "impl" object. Used internally.
   */
  [[nodiscard]]
  const std::shared_ptr<impl::AggregateImpl> &Impl() const { return impl_; }

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
  [[nodiscard]]
  static AggregateCombo Create(std::initializer_list<Aggregate> list);
  /**
   * Create an AggregateCombo from a vector.
   */
  [[nodiscard]]
  static AggregateCombo Create(std::vector<Aggregate> vec);

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
  [[nodiscard]]
  const std::shared_ptr<impl::AggregateComboImpl> &Impl() const { return impl_; }

private:
  explicit AggregateCombo(std::shared_ptr<impl::AggregateComboImpl> impl);

  std::shared_ptr<impl::AggregateComboImpl> impl_;
};

/**
 * Returns an aggregator that computes the total sum of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggAbsSum(Args &&... args) {
  return Aggregate::AbsSum(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes an array of all values within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggGroup(Args &&... args) {
  return Aggregate::Group(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggAvg(Args &&... args) {
  return Aggregate::Avg(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the number of elements within an aggregation group.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate aggCount(Args &&... args) {
  return Aggregate::Count(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the first value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggFirst(Args &&... args) {
  return Aggregate::First(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the last value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggLast(Args &&... args) {
  return Aggregate::Last(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the maximum value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate aggMax(Args &&... args) {
  return Aggregate::Max(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the median value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggMed(Args &&... args) {
  return Aggregate::Med(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the minimum value, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate aggMin(Args &&... args) {
  return Aggregate::Min(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the designated percentile of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggPct(double percentile, Args &&... args) {
  return Aggregate::Pct(percentile, std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the standard deviation of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggStd(Args &&... args) {
  return Aggregate::Std(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the total sum of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate aggSum(Args &&... args) {
  return Aggregate::Sum(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the variance of values, within an aggregation group,
 * for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggVar(Args &&... args) {
  return Aggregate::Var(std::forward<Args>(args)...);
}

/**
 * Returns an aggregator that computes the weighted average of values, within an aggregation
 * group, for each input column.
 */
template<typename ...Args>
[[nodiscard]]
Aggregate AggWavg(Args &&... args) {
  return Aggregate::WAvg(std::forward<Args>(args)...);
}

/**
 * Returns an AggregateCombo object, which represents a collection of aggregators.
 */
[[nodiscard]]
inline AggregateCombo aggCombo(std::initializer_list<Aggregate> args) {
  return AggregateCombo::Create(args);
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
  using SchemaType = deephaven::dhcore::clienttable::Schema;
  using TickingCallback = deephaven::dhcore::ticking::TickingCallback;
  using TickingUpdate = deephaven::dhcore::ticking::TickingUpdate;
  using BooleanExpression = deephaven::client::BooleanExpression;
  using Column = deephaven::client::Column;
  using DateTimeCol = deephaven::client::DateTimeCol;
  using MatchWithColumn = deephaven::client::MatchWithColumn;
  using NumCol = deephaven::client::NumCol;
  using SelectColumn = deephaven::client::SelectColumn;
  using SortPair = deephaven::client::SortPair;
  using StrCol = deephaven::client::StrCol;
  using SubscriptionHandle = deephaven::client::subscription::SubscriptionHandle;

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
  [[nodiscard]]
  TableHandleManager GetManager() const;

  /**
   * A variadic form of Select(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The arguments to Select
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Select(Args &&...args) const;
  /**
   * Select columnSpecs from a table. The columnSpecs can be column names or formulas like
   * "NewCol = A + 12". See the Deephaven documentation for the difference between "Select" and
   * "View".
   * @param columnSpecs The columnSpecs
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Select(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of View(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The arguments to View
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle View(Args &&... args) const;
  /**
   * View columnSpecs from a table. The columnSpecs can be column names or formulas like
   * "NewCol = A + 12". See the Deephaven documentation for the difference between Select() and
   * View().
   * @param columnSpecs The columnSpecs to View
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle View(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of DropColumns(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to drop
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle DropColumns(Args &&... args) const;
  /**
   * Creates a new table from this table Where the specified columns have been excluded.
   * @param columnSpecs The columns to exclude.
   * @return
   */
  [[nodiscard]]
  TableHandle DropColumns(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of Update(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to Update
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Update(Args &&... args) const;
  /**
   * Creates a new table from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between Update() and UpdateView().
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Update(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of UpdateView(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to UpdateView
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle UpdateView(Args &&... args) const;
  /**
   * Creates a new View from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between Update() and UpdateView().
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle UpdateView(std::vector<std::string> column_specs) const;

  /**
   * A structured form of Where(std::string) const, but which takes the Fluent syntax.
   * @param condition A Deephaven fluent BooleanExpression such as `Price > 100` or `Col3 == Col1 * Col2`
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Where(const BooleanExpression &condition) const;
  /**
   * Creates a new table from this table, filtered by condition. Consult the Deephaven
   * documentation for more information about valid conditions.
   * @param condition A Deephaven boolean expression such as "Price > 100" or "Col3 == Col1 * Col2".
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Where(std::string condition) const;

  /**
   * Creates a new table from this table, sorted By sortPairs.
   * @param sortPairs A vector of SortPair objects describing the sort. Each SortPair refers to
   *   a column, a sort direction, and whether the sort should consider to the value's regular or
   *   absolute value when doing comparisons.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Sort(std::vector<SortPair> sort_pairs) const;

  /**
   * A variadic form of By(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle By(Args &&... args) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs with the column content grouped
   * into arrays.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle By(std::vector<std::string> column_specs) const;
  // TODO(kosak): document
  [[nodiscard]]
  TableHandle By(AggregateCombo combo, std::vector<std::string> group_by_columns) const;

  // TODO(kosak): document
  template<typename ...Args>
  [[nodiscard]]
  TableHandle By(AggregateCombo combo, Args &&... args) const;

  /**
   * A variadic form of MinBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MinBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Min" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MinBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of MaxBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MaxBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Max" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MaxBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of SumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle SumBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Sum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle SumBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of AbsSumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  TableHandle AbsSumBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "AbsSum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle AbsSumBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of VarBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle VarBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Var" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle VarBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of StdBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle StdBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "std" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle StdBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of AvgBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle AvgBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Avg" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle AvgBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of FirstBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle FirstBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "First" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle FirstBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of LastBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle LastBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Last" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle LastBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of MedianBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MedianBy(Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "median" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MedianBy(std::vector<std::string> column_specs) const;

  /**
   * A variadic form of PercentileBy(double, bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, bool avg_median, Args &&... column_specs) const;
  // TODO(kosak): document avgMedian
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle
  PercentileBy(double percentile, bool avg_median, std::vector<std::string> column_specs) const;

  /**
   * A variadic form of PercentileBy(double, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, std::vector<std::string> column_specs) const;

  // TODO(kosak): I wonder if these are all MatchWithColumns, not SelectColumns
  /**
   * A variadic form of CountBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam CCol Any of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param countByColumn The output column
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename CCol, typename ...Args>
  [[nodiscard]]
  TableHandle CountBy(CCol &&count_by_column, Args &&... columnSpecs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, having a new column named By
   * `countByColumn` containing the size of each group.
   * @param countByColumn Name of the output column.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle CountBy(std::string count_by_column, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of WavgBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam CCol Any of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param weightColumn The output column
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename WCol, typename ...Args>
  [[nodiscard]]
  TableHandle WAvgBy(WCol &&weight_column, Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, having a new column named By
   * `weightColumn` containing the weighted average of each group.
   * @param countByColumn Name of the output column.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle WAvgBy(std::string weight_column, std::vector<std::string> columnSpecs) const;

  /**
   * A variadic form of TailBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle TailBy(int64_t n, Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, containing the Last `n` rows of
   * each group.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle TailBy(int64_t n, std::vector<std::string> column_specs) const;

  /**
   * A variadic form of HeadBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle HeadBy(int64_t n, Args &&... column_specs) const;
  /**
   * Creates a new table from this table, grouped by columnSpecs, containing the First `n` rows of
   * each group.
   * @param n Number of rows
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle HeadBy(int64_t n, std::vector<std::string> column_specs) const;

  /**
   * Creates a new table from this table containing the First `n` rows of this table.
   * @param n Number of rows
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Head(int64_t n) const;
  /**
   * Creates a new table from this table containing the Last `n` rows of this table.
   * @param n Number of rows
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Tail(int64_t n) const;

  //TODO(kosak): document nullFill
  /**
   * A variadic form of Ungroup(bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Ungroup(bool null_fill, Args &&... column_specs) const;
  //TODO(kosak): document nullFill
  /**
   * Creates a new table from this table with the column array data ungrouped. This is the inverse
   * of the By(std::vector<std::string>) const operation.
   * @param groupByColumns Columns to Ungroup.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Ungroup(bool null_fill, std::vector<std::string> groupByColumns) const;

  /**
   * A variadic form of Ungroup(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, `const char *`, or `SelectColumn`.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Ungroup(Args &&... column_specs) const {
    return Ungroup(false, std::forward<Args>(column_specs)...);
  }

  /**
   * Creates a new table from this table with the column array data ungrouped. This is the inverse
   * of the By(std::vector<std::string>) const operation.
   * @param groupByColumns Columns to Ungroup.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Ungroup(std::vector<std::string> group_by_columns) const {
    return Ungroup(false, std::move(group_by_columns));
  }

  //TODO(kosak): document keyColumn
  /**
   * Creates a new table By merging `sources` together. The tables are essentially stacked on top
   * of each other.
   * @param sources The tables to Merge.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Merge(std::string key_column, std::vector<TableHandle> sources) const;

  /**
   * Creates a new table By merging `sources` together. The tables are essentially stacked on top
   * of each other.
   * @param sources The tables to Merge.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Merge(std::vector<TableHandle> sources) const {
    // TODO(kosak): may need to support null
    return Merge("", std::move(sources));
  }

  /**
   * Creates a new table By cross joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.CrossJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle CrossJoin(const TableHandle &right_side, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columns_to_add) const;
  /**
   * The fluent version of CrossJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.CrossJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param right_side The table to join with this table
   * @param columns_to_match The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  [[nodiscard]]
  TableHandle CrossJoin(const TableHandle &right_side, std::vector<MatchWithColumn> columns_to_match,
      std::vector<SelectColumn> columns_to_add) const;

  /**
   * Creates a new table By natural joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.NaturalJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle NaturalJoin(const TableHandle &right_side, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columns_to_add) const;
  /**
   * The fluent version of NaturalJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.NaturalJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  [[nodiscard]]
  TableHandle NaturalJoin(const TableHandle &right_side, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columns_to_add) const;

  /**
   * Creates a new table By exact joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.ExactJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle ExactJoin(const TableHandle &right_side, std::vector<std::string> columnsToMatch,
      std::vector<std::string> columns_to_add) const;
  /**
   * The fluent version of ExactJoin(const TableHandle &, std::vector<std::string>, std::vector<std::string>) const.
   * @code
   * t1.ExactJoin(col1, col2}, {col3, col4.as("NewCol"})
   * @endcode

   * @param rightSide The table to join with this table
   * @param columnsToMatch The columns to join on
   * @param columnsToAdd The columns from the right side to add, and possibly rename.
   * @return
   */
  [[nodiscard]]
  TableHandle ExactJoin(const TableHandle &right_side, std::vector<MatchWithColumn> columnsToMatch,
      std::vector<SelectColumn> columns_to_add) const;

  [[nodiscard]]
  TableHandle UpdateBy(std::vector<UpdateByOperation> ops, std::vector<std::string> by) const;

  /**
   * Binds this table to a variable name in the QueryScope.
   * @param variable The QueryScope variable to bind to.
   */
  void BindToVariable(std::string variable) const;
  /**
   * The async version of BindToVariable(std::string variable) const.
   * @param variable The QueryScope variable to bind to.
   * @param callback The asynchronous callback.
   */
  void BindToVariableAsync(std::string variable, std::shared_ptr<SFCallback<>> callback) const;

  /**
   * Get all the table's columns.
   * @return A vector of the table's columns.
   */
  [[nodiscard]]
  std::vector<Column> GetAllCols() const;

  /**
   * Get a column that is of string type. Used in the fluent interface. Example:
   * @code
   * auto symbol = table.GetStrCol("Symbol")
   * auto t2 = table.Where(symbol == "IBM");
   * @endcode
   * @param columnName The name of the column
   * @return The specified StrCol.
   */
  [[nodiscard]]
  StrCol GetStrCol(std::string column_name) const;
  /**
   * Get a column that is of numeric type. Used in the fluent interface. Example:
   * @code
   * auto volume = table.GetNumCol("Volume")
   * auto t2 = table.Where(volume < 1000);
   * @endcode
   * @param columnName The name of the column
   * @return The specified NumCol.
   */
  [[nodiscard]]
  NumCol GetNumCol(std::string column_name) const;
  /**
   * Get a column that is of DateTime type. Used in the fluent interface.
   * @param columnName The name of the column
   * @return The specified DateTimeCol.
   */
  [[nodiscard]]
  DateTimeCol GetDateTimeCol(std::string column_name) const;

  // internal::StringHolder is a trick to make sure we are passed as many strings as there are
  // template arguments.
  /**
    * Convenience function to get several columns at once. Example:
    * @code
    * auto [symbol, volume] = table.GetCols<StrCol, NumCol>("Symbol", "Volume");
    * @endcode
    * @tparam Cols Column types
    * @param names Column names
    * @return A tuple of columns.
    */
  template<typename... Cols>
  [[nodiscard]]
  std::tuple<Cols...> GetCols(internal::StringHolder<Cols>... names);

  /**
   * Create an ostream adpator that prints a human-readable representation of the table to a
   * C++ stream. Example:
   * @code
   * std::cout << table.Stream(true) << std::endl;
   * @endcode
   * @param wantHeaders Include column headers.
   * @return
   */
  [[nodiscard]]
  internal::TableHandleStreamAdaptor Stream(bool want_headers) const;

  /**
   * Used internally, for demo purposes.
   */
  [[nodiscard]]
  std::string ToString(bool want_headers) const;

  /**
   * Used internally, for debugging.
   */
  void Observe() const;

  /**
   * A specialized operation to Release the state of this TableHandle. This operation is normally done By the
   * destructor, so most programs will never need to call this method.. If there are no other copies of this
   * TableHandle, and if there are no "child" TableHandles dependent on this TableHandle, then the corresponding server
   * resources will be released.
   */
  void Release() {
    impl_.reset();
  }

  /**
   * Number of rows in the table at the time this TableHandle was created.
   */
  [[nodiscard]]
  int64_t NumRows() const;

  /**
   * Whether the table was static at the time this TableHandle was created.
   */
  [[nodiscard]]
  bool IsStatic() const;

  /**
   * Returns the table's Schema.
   */
  [[nodiscard]]
  std::shared_ptr<SchemaType> Schema() const;

  /**
   * Used internally. Returns the underlying Impl object.
   */
  [[nodiscard]]
  const std::shared_ptr<impl::TableHandleImpl> &Impl() const { return impl_; }

  /**
   * Construct an arrow FlightStreamReader bound to this table
   * @return the Arrow FlightStreamReader.
   */
  [[nodiscard]]
  std::shared_ptr<arrow::flight::FlightStreamReader> GetFlightStreamReader() const;

  /**
   * Subscribe to a ticking table.
   */
  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(std::shared_ptr<TickingCallback> callback);

  using onTickCallback_t = void (*)(TickingUpdate, void *);
  using onErrorCallback_t = void (*)(std::string, void *);
  /**
   * Subscribe to a ticking table (C-style).
   */
  [[nodiscard]]
  std::shared_ptr<SubscriptionHandle> Subscribe(onTickCallback_t onTick, void *onTickUserData,
      onErrorCallback_t on_error, void *onErrorUserData);
  /**
   * Unsubscribe from the table.
   */
  void Unsubscribe(std::shared_ptr<SubscriptionHandle> callback);

  /**
   * Get access to the bytes of the Deephaven "Ticket" type (without having to reference the
   * protobuf data structure, which we would prefer not to have a dependency on here)
   */
  [[nodiscard]]
  const std::string &GetTicketAsString() const;

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
  [[nodiscard]]
  static NumCol GetCol(const TableHandle &table, std::string name) {
    return table.GetNumCol(std::move(name));
  }
};

template<>
struct ColGetter<StrCol> {
  [[nodiscard]]
  static StrCol GetCol(const TableHandle &table, std::string name) {
    return table.GetStrCol(std::move(name));
  }
};

template<>
struct ColGetter<DateTimeCol> {
  [[nodiscard]]
  static DateTimeCol GetCol(const TableHandle &table, std::string name) {
    return table.GetDateTimeCol(std::move(name));
  }
};
}  // namespace internal

template<typename... Cols>
[[nodiscard]]
std::tuple<Cols...> TableHandle::GetCols(internal::StringHolder<Cols>... names) {
  return std::make_tuple(
      internal::ColGetter<Cols>::GetCol(*this, std::move(names.s_))...
  );
}

namespace internal {
class TableHandleStreamAdaptor {
public:
  TableHandleStreamAdaptor(TableHandle table, bool want_headers);
  TableHandleStreamAdaptor(const TableHandleStreamAdaptor &) = delete;
  TableHandleStreamAdaptor &operator=(const TableHandleStreamAdaptor &) = delete;
  ~TableHandleStreamAdaptor();

private:
  TableHandle table_;
  bool wantHeaders_ = false;

  friend std::ostream &operator<<(std::ostream &s, const TableHandleStreamAdaptor &o);
};

struct ConvertToString {
  [[nodiscard]]
  static std::string ToString(const char *s) {
    return s;
  }

  [[nodiscard]]
  static std::string ToString(std::string_view sv) {
    return {sv.data(), sv.size()};
  }

  [[nodiscard]]
  static std::string ToString(std::string s) {
    return s;
  }

  [[nodiscard]]
  static std::string ToString(const deephaven::client::SelectColumn &selectColumn);
};
}  // namespace internal

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::AbsSum(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return AbsSum(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Group(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Group(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Avg(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Avg(std::move(column_specs));
}

template<typename ColArg>
[[nodiscard]]
Aggregate Aggregate::Count(ColArg &&arg) {
  auto columnSpec = internal::ConvertToString::ToString(std::forward<ColArg>(arg));
  return Count(std::move(columnSpec));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::First(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return First(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Last(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Last(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Max(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Max(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Med(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Max(std::move(columnSpecs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Min(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Min(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Pct(double percentile, bool avg_median, Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Pct(percentile, avg_median, std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Std(Args &&... args) {
  std::vector<std::string> column_specs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Std(std::move(column_specs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Sum(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Sum(std::move(columnSpecs));
}

template<typename ...Args>
[[nodiscard]]
Aggregate Aggregate::Var(Args &&... args) {
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Var(std::move(columnSpecs));
}

template<typename ColArg, typename ...Args>
[[nodiscard]]
Aggregate Aggregate::WAvg(ColArg &&weight_column, Args &&... args) {
  auto weightCol = internal::ConvertToString::ToString(std::forward<ColArg>(weight_column));
  std::vector<std::string> columnSpecs = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return WAvg(std::move(weightCol), std::move(columnSpecs));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::Select(Args &&... args) const {
  std::vector<std::string> selectColumns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Select(std::move(selectColumns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::View(Args &&... args) const {
  std::vector<std::string> viewColumns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return View(std::move(viewColumns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::DropColumns(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return DropColumns(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::Update(Args &&... args) const {
  std::vector<std::string> updateColumns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return Update(std::move(updateColumns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::UpdateView(Args &&... args) const {
  std::vector<std::string> updateColumns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return UpdateView(std::move(updateColumns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::By(Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return By(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::By(AggregateCombo combo, Args &&... args) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(args))...
  };
  return By(std::move(combo), std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::MinBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return MinBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::MaxBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return MaxBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::SumBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return SumBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::AbsSumBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return AbsSumBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::VarBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return VarBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::StdBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return StdBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::AvgBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return AvgBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::FirstBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return FirstBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::LastBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return LastBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::MedianBy(Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return MedianBy(std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::PercentileBy(double percentile, bool avg_median, Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return PercentileBy(percentile, avg_median, std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::PercentileBy(double percentile, Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return PercentileBy(percentile, std::move(columns));
}

template<typename CCol, typename ...Args>
[[nodiscard]]
TableHandle TableHandle::CountBy(CCol &&count_by_column, Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return CountBy(internal::ConvertToString::ToString(count_by_column), std::move(columns));
}

template<typename WCol, typename ...Args>
[[nodiscard]]
TableHandle TableHandle::WAvgBy(WCol &&weight_column, Args &&... column_specs) const {
  std::vector<std::string> columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return WAvgBy(internal::ConvertToString::ToString(weight_column), std::move(columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::TailBy(int64_t n, Args &&... column_specs) const {
  std::vector<std::string> last_by_columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return TailBy(n, std::move(last_by_columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::HeadBy(int64_t n, Args &&... column_specs) const {
  std::vector<std::string> last_by_columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return HeadBy(n, std::move(last_by_columns));
}

template<typename ...Args>
[[nodiscard]]
TableHandle TableHandle::Ungroup(bool null_fill, Args &&... column_specs) const {
  std::vector<std::string> group_by_columns = {
      internal::ConvertToString::ToString(std::forward<Args>(column_specs))...
  };
  return Ungroup(null_fill, std::move(group_by_columns));
}
}  // namespace deephaven::client
