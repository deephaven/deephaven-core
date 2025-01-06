/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <string_view>
#include "deephaven/client/client_options.h"
#include "deephaven/client/utility/misc_types.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"

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
 * Forward reference to arrow's Table
 */
namespace arrow {
class Table;
}  // namespace arrow

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
/**
 * This class is used to assist the various variadic entry points like Select(Args...).
 * The methods in this class canonicalize string-like arguments (const char *, string_view),
 * turning them into strings.
 */
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
};
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
  using DurationSpecifier = deephaven::client::utility::DurationSpecifier;
  using TimePointSpecifier = deephaven::client::utility::TimePointSpecifier;

  using SchemaType = deephaven::dhcore::clienttable::Schema;

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
   * @param blink_table Whether the table is a blink table
   * @return The TableHandle of the new table.
   */
  [[nodiscard]]
  TableHandle TimeTable(DurationSpecifier period, TimePointSpecifier start_time = 0,
      bool blink_table = false) const;

  /**
   * Creates an input table from an initial table. When key columns are provided, the InputTable
   * will be keyed, otherwise it will be append-only.
   * @param initial_table The initial table
   * @param columns The set of key columns
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle InputTable(const TableHandle &initial_table,
      std::vector<std::string> key_columns = {}) const;

  // TODO(kosak): not implemented yet.
  /**
   * Creates an input table from a Schema. When key columns are provided, the InputTable
   * will be keyed, otherwise it will be append-only.
   * @param schema The table schema.
   * @return A TableHandle referencing the new table
   */
//  [[nodiscard]]
//  TableHandle InputTable(std::shared_ptr<SchemaType> schema,
//      std::vector<std::string> key_columns = {}) const;

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
   * @param code The script to be run on the server
   */
  void RunScript(std::string code) const;

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
 * Describes a sort direction
 */
enum class SortDirection {
  kAscending, kDescending
};

/**
 * A tuple (not a "pair", despite the name) representing a column to sort, the SortDirection,
 * and whether the Sort should consider the value's regular or absolute value when doing comparisons.
 */
class SortPair {
public:
  /**
   * Create a SortPair with direction set to ascending.
   * @param column The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  [[nodiscard]]
  static SortPair Ascending(std::string column, bool abs = false) {
    return {std::move(column), SortDirection::kAscending, abs};
  }
  /**
   * Create a SortPair with direction set to descending.
   * @param column The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair Descending(std::string column, bool abs = false) {
    return {std::move(column), SortDirection::kDescending, abs};
  }

  /**
   * Constructor.
   */
  SortPair(std::string column, SortDirection direction, bool abs = false) :
      column_(std::move(column)), direction_(direction), abs_(abs) {}

  /**
   * Constructor.
   */
  SortPair(std::string column, bool abs = false) :
      column_(std::move(column)), direction_(SortDirection::kAscending), abs_(abs) {}

  /**
   * Get the column name
   * @return The column name
   */
  [[nodiscard]]
  std::string &Column() { return column_; }
  /**
   * Get the column name
   * @return The column name
   */
  [[nodiscard]]
  const std::string &Column() const { return column_; }

  /**
   * Get the SortDirection
   * @return The SortDirection
   */
  [[nodiscard]]
  SortDirection Direction() const { return direction_; }

  /**
   * Get the "Sort by absolute value" flag
   * @return
   */
  [[nodiscard]]
  bool Abs() const { return abs_; }

private:
  std::string column_;
  SortDirection direction_ = SortDirection::kAscending;
  bool abs_ = false;
};

/**
 * The main class for interacting with Deephaven. Start here to Connect with
 * the server and to get a TableHandleManager.
 */
class Client {
public:
  /*
   * Default constructor. Creates a (useless) empty client object.
   */
  Client();

  /**
   * Factory method to Connect to a Deephaven server using the specified options.
   * @param target A connection string in the format host:port. For example "localhost:10000".
   * @param options An options object for setting options like authentication and script language.
   * @return A Client object connected to the Deephaven server.
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

  using OnCloseCbId = utility::OnCloseCbId;
  using OnCloseCb = utility::OnCloseCb;

  /**
   * Adds a callback to be invoked when this client is closed.
   * On close callbacks are invoked before the client is actually shut down,
   * so they can perform regular client and table manager operations before
   * closing.
   *
   * @param cb the callback
   * @return an id for the added callback that can be used to remove it.
   */
  OnCloseCbId AddOnCloseCallback(OnCloseCb cb);

  /**
   * Removes an on close callback.
   * @param cb_id the id of the callback to remove
   * @return true if a callback with that id was found and removed, false otherwise.
   */
  bool RemoveOnCloseCallback(OnCloseCbId cb_id);

private:
  explicit Client(std::shared_ptr<impl::ClientImpl> impl);
  std::shared_ptr<impl::ClientImpl> impl_;
};

/**
 * Defines an aggregator class that represents one of a variet of aggregation operations.
 */
class Aggregate {
public:
  /*
 * Default constructor. Creates a (useless) empty object.
 */
  Aggregate();
  /**
   * Copy constructor
   */
  Aggregate(const Aggregate &other);
  /**
   * Move constructor
   */
  Aggregate(Aggregate &&other) noexcept;
  /**
   * Copy assigment operator.
   */
  Aggregate &operator=(const Aggregate &other);
  /**
   * Move assigment operator.
   */
  Aggregate &operator=(Aggregate &&other) noexcept;
  /**
   * Destructor
   */
  ~Aggregate();
  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate AbsSum(std::vector<std::string> column_specs);
  /**
   * A variadic form of AbsSum(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to AbsSum
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate AbsSum(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return AbsSum(std::move(vec));
  }

  /**
   * Returns an aggregator that computes an array of all values within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Group(std::vector<std::string> column_specs);
  /**
   * A variadic form of Group(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Group
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Group(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Group(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the average (mean) of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Avg(std::vector<std::string> column_specs);
  /**
   * A variadic form of Avg(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Avg
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Avg(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Avg(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the number of elements within an aggregation group.
   */
  [[nodiscard]]
  static Aggregate Count(std::string column_spec);

  /**
   * Returns an aggregator that computes the first value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate First(std::vector<std::string> column_specs);
  /**
   * A variadic form of First(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to First
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate First(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return First(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the last value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Last(std::vector<std::string> column_specs);
  /**
   * A variadic form of First(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Last
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Last(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Last(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the maximum value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Max(std::vector<std::string> column_specs);
  /**
   * A variadic form of Max(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Max
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Max(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Max(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the median value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Med(std::vector<std::string> column_specs);
  /**
   * A variadic form of Med(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Med
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Med(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Med(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the minimum value, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Min(std::vector<std::string> column_specs);
  /**
   * A variadic form of Min(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Min
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Min(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Min(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the designated percentile of values, within an aggregation
   * group, for each input column.
   */
  [[nodiscard]]
  static Aggregate Pct(double percentile, bool avg_median, std::vector<std::string> column_specs);
  /**
   * A variadic form of Pct(double, bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Pct
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Pct(double percentile, bool avg_median, Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Pct(percentile, avg_median, std::move(vec));
  }

  /**
   * Returns an aggregator that computes the sample standard deviation of values, within an
   * aggregation group, for each input column.
   *
   * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
   * which ensures that the sample variance will be an unbiased estimator of population variance.
   */
  [[nodiscard]]
  static Aggregate Std(std::vector<std::string> column_specs);
  /**
   * A variadic form of Std(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Std
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Std(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Std(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the total sum of values, within an aggregation group,
   * for each input column.
   */
  [[nodiscard]]
  static Aggregate Sum(std::vector<std::string> column_specs);
  /**
   * A variadic form of Sum(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Sum
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Sum(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Sum(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the sample variance of values, within an aggregation group,
   * for each input column.
   *
   * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
   * which ensures that the sample variance will be an unbiased estimator of population variance.
   */
  [[nodiscard]]
  static Aggregate Var(std::vector<std::string> column_specs);
  /**
   * A variadic form of Var(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Var
   * @return An Aggregate object representing the aggregation
   */
  template<typename ...Args>
  [[nodiscard]]
  static Aggregate Var(Args &&...args) {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Var(std::move(vec));
  }

  /**
   * Returns an aggregator that computes the weighted average of values, within an aggregation
   * group, for each input column.
   */
  [[nodiscard]]
  static Aggregate WAvg(std::string weight_column, std::vector<std::string> column_specs);
  /**
   * A variadic form of WAvg(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to WAvg
   * @return An Aggregate object representing the aggregation
   */
  template<typename WeightArg, typename ...Args>
  [[nodiscard]]
  static Aggregate WAvg(WeightArg &&weight_column, Args &&...args) {
    auto weight = internal::ConvertToString::ToString(std::forward<WeightArg>(weight_column));
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return WAvg(std::move(weight), std::move(vec));
  }

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
   * Copy constructor
   */
  AggregateCombo(const AggregateCombo &other);
  /**
   * Move constructor
   */
  AggregateCombo(AggregateCombo &&other) noexcept;
  /**
   * Copy assigment operator.
   */
  AggregateCombo &operator=(const AggregateCombo &other);
  /**
   * Move assigment operator.
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
 * Returns an aggregator that computes the sample standard deviation of values, within an aggregation group,
 * for each input column.
 *
 * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
 * which ensures that the sample variance will be an unbiased estimator of population variance.
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
 * Returns an aggregator that computes the sample variance of values, within an aggregation group,
 * for each input column.
 *
 * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
 * which ensures that the sample variance will be an unbiased estimator of population variance.
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

/**
 * Holds an reference to a server resource representing a table. TableHandle objects have shared
 * ownership semantics so they can be copied freely. When the last TableHandle pointing to a
 * server resource is destructed, the resource will be released.
 */
class TableHandle {
  using ClientTable = deephaven::dhcore::clienttable::ClientTable;
  using SchemaType = deephaven::dhcore::clienttable::Schema;
  using TickingCallback = deephaven::dhcore::ticking::TickingCallback;
  using TickingUpdate = deephaven::dhcore::ticking::TickingUpdate;
  using SubscriptionHandle = deephaven::client::subscription::SubscriptionHandle;

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
   * Select columnSpecs from a table. The columnSpecs can be column names or formulas like
   * "NewCol = A + 12". See the Deephaven documentation for the difference between "Select" and
   * "View".
   * @param columnSpecs The columnSpecs
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Select(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of Select(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to Select
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Select(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Select(std::move(vec));
  }

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
   * A variadic form of View(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The arguments to View
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle View(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return View(std::move(vec));
  }

  /**
   * Creates a new table from this table Where the specified columns have been excluded.
   * @param columnSpecs The columns to exclude.
   * @return
   */
  [[nodiscard]]
  TableHandle DropColumns(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of DropColumns(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns to drop
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle DropColumns(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return DropColumns(std::move(vec));
  }

  /**
   * Creates a new table from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For example, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between Update() and UpdateView().
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Update(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of Update(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns to Update
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Update(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Update(std::move(vec));
  }

  /**
   * Creates a new table containing a new cached formula column for each argument.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between Update() and LazyUpdate().
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle LazyUpdate(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of LazyUpdate(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns to LazyUpdate
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle LazyUpdate(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return LazyUpdate(std::move(vec));
  }

  /**
   * Creates a new View from this table, but including the additional specified columns.
   * @param columnSpecs The columnSpecs to add. For exampe, {"X = A + 5", "Y = X * 2"}.
   * See the Deephaven documentation for the difference between Update() and UpdateView().
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle UpdateView(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of UpdateView(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns to UpdateView
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle UpdateView(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return UpdateView(std::move(vec));
  }

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
   * A variadic form of Sort(std::vector<SortPair>) const
   * @tparam Args One or more SortPairs
   * @param args The sort_pairs
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Sort(Args &&... args) const {
    std::vector<SortPair> vec{std::forward<Args>(args)...};
    return Sort(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs with the column content grouped
   * into arrays.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle By(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of By(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args Columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle By(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return By(std::move(vec));
  }

  // TODO(kosak): document
  [[nodiscard]]
  TableHandle By(AggregateCombo combo, std::vector<std::string> group_by_columns) const;
  /**
   * A variadic form of By(AggregateCombo, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle By(AggregateCombo combo, Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return By(std::move(combo), std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Min" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MinBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of MinBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MinBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return MinBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Max" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MaxBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of MaxBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args Columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MaxBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return MaxBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Sum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle SumBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of SumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle SumBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return SumBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "AbsSum" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle AbsSumBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of AbsSumBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args Columns to group by.
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle AbsSumBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return AbsSumBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Var" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle VarBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of VarBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns to group by
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle VarBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return VarBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "std" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle StdBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of StdBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle StdBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return StdBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Avg" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle AvgBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of AvgBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle AvgBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return AvgBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "First" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle FirstBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of FirstBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle FirstBy(Args &&... args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return FirstBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "Last" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle LastBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of LastBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle LastBy(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return LastBy(std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "median" aggregate operation
   * applied to the remaining columns.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle MedianBy(std::vector<std::string> column_specs) const;
  /**
   * A variadic form of LastBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle MedianBy(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return MedianBy(std::move(vec));
  }

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
   * A variadic form of PercentileBy(double, bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, bool avg_median, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return PercentileBy(percentile, avg_median, std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, with the "percentile" aggregate operation
   * applied to the remaining columns.
   * @param percentile The designated percentile
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, std::vector<std::string> column_specs) const {
    return PercentileBy(percentile, false, std::move(column_specs));
  }
  /**
   * A variadic form of PercentileBy(double, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle PercentileBy(double percentile, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return PercentileBy(percentile, false, std::forward<Args...>(args...));
  }

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
   * A variadic form of CountBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle CountBy(std::string count_by_column, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return CountBy(std::move(count_by_column), std::move(vec));
  }

  /**
   * Creates a new table from this table, grouped by columnSpecs, having a new column named By
   * `weightColumn` containing the weighted average of each group.
   * @param countByColumn Name of the output column.
   * @param columnSpecs Columns to group by.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle WAvgBy(std::string weight_column, std::vector<std::string> column_specs) const;
  /**
   * A variadic form of WAvgBy(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle WAvgBy(std::string weight_column, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return WAvgBy(std::move(weight_column), std::move(vec));
  }

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
   * A variadic form of TailBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle TailBy(int64_t n, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return TailBy(n, std::move(vec));
  }

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
   * A variadic form of HeadBy(int64_t, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle HeadBy(int64_t n, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return HeadBy(n, std::move(vec));
  }

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
   * Creates a new table from this table with the column array data ungrouped. This is the inverse
   * of the By(std::vector<std::string>) const operation.
   * @param groupByColumns Columns to Ungroup.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Ungroup(bool null_fill, std::vector<std::string> group_by_columns) const;
  /**
   * A variadic form of Ungroup(bool, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Ungroup(bool null_fill, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Ungroup(null_fill, std::move(vec));
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
  /**
   * A variadic form of Ungroup(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle Ungroup(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return Ungroup(std::move(vec));
  }

  //TODO(kosak): document keyColumn
  /**
   * Creates a new table By merging `sources` together. The tables are essentially stacked on top
   * of each other.
   * @param sources The tables to Merge.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle Merge(std::string key_columns, std::vector<TableHandle> sources) const;
  /**
   * A variadic form of Merge(std::string, std::vector<std::string>) const that takes a combination of
   * argument types.
   * @param first The first TableHandle in the list of sources.
   * @param rest The rest of the TableHandles in the list of sources. The arguments are split this
   * way to aid in overload resolution.
   * @tparam Rest Zero or more TableHandles
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Rest>
  [[nodiscard]]
  TableHandle Merge(std::string key_column, TableHandle first, Rest &&...rest) const {
    std::vector<TableHandle> vec{std::move(first)};
    vec.insert(vec.end(), {std::forward<Rest>(rest)...});
    return Merge(std::move(key_column), std::move(vec));
  }

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
   * A variadic form of Merge(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @param first The first TableHandle in the list of sources.
   * @param rest The rest of the TableHandles in the list of sources. The arguments are split this
   * @tparam Rest Zero or more TableHandles
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Rest>
  [[nodiscard]]
  TableHandle Merge(TableHandle first, Rest &&...rest) const {
    std::vector<TableHandle> vec{std::move(first)};
    vec.insert(vec.end(), {std::forward<Rest>(rest)...});
    return Merge(std::move(vec));
  }

  /**
   * Creates a new table By cross joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.CrossJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columns_to_match The columns to join on
   * @param columns_to_add The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle CrossJoin(const TableHandle &right_side, std::vector<std::string> columns_to_match,
      std::vector<std::string> columns_to_add) const;

  /**
   * Creates a new table By natural joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.NaturalJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columns_to_match The columns to join on
   * @param columns_to_add The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle NaturalJoin(const TableHandle &right_side, std::vector<std::string> columns_to_match,
      std::vector<std::string> columns_to_add) const;

  /**
   * Creates a new table By exact joining this table with `rightSide`. The tables are joined By
   * the columns in `columnsToMatch`, and columns from `rightSide` are brought in and optionally
   * renamed By `columnsToAdd`. Example:
   * @code
   * t1.ExactJoin({"Col1", "Col2"}, {"Col3", "NewCol=Col4"})
   * @endcode
   * @param rightSide The table to join with this table
   * @param columns_to_match The columns to join on
   * @param columns_to_add The columns from the right side to add, and possibly rename.
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle ExactJoin(const TableHandle &right_side, std::vector<std::string> columns_to_match,
      std::vector<std::string> columns_to_add) const;

  /**
   * Creates a new table containing all the rows and columns of the left table, plus additional
   * columns containing data from the right table. For columns appended to the left table (joins),
   * row values equal the row values from the right table where the keys from the left table most
   * closely match the keys from the right table without going over. If there is no matching key in
   * the right table, appended row values are NULL.
   *
   * @param right_side The table to join with this table
   * @param on The column(s) to match, can be a common name or a match condition of two
   *    columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches. The final match is
   *    an inexact match.  The inexact match can use either '>' or '>='.  If a common name is used
   *    for the inexact match, '>=' is used for the comparison.
   * @param joins The column(s) to be added from the right table to the result table, can be
   *   renaming expressions, i.e. "new_col = col"; default is empty, which means all the columns
   *   from the right table, excluding those specified in 'on'
   */
  [[nodiscard]]
  TableHandle Aj(const TableHandle &right_side, std::vector<std::string> on,
      std::vector<std::string> joins = {}) const;

  /**
   * Creates a new table containing all the rows and columns of the left table, plus additional
   * columns containing data from the right table. For columns appended to the left table (joins),
   * row values equal the row values from the right table where the keys from the left table most closely
   * match the keys from the right table without going under. If there is no matching key in the
   * right table, appended row values are NULL.
   *
   * @param right_side The table to join with this table
   * @param on The column(s) to match, can be a common name or a match condition of two
   *    columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches. The final match is
   *    an inexact match.  The inexact match can use either '<' or '<='.  If a common name is used
   *    for the inexact match, '<=' is used for the comparison.
   * @param joins The column(s) to be added from the right table to the result table, can be
   *   renaming expressions, i.e. "new_col = col"; default is empty, which means all the columns
   *   from the right table, excluding those specified in 'on'
   */
  [[nodiscard]]
  TableHandle Raj(const TableHandle &right_side, std::vector<std::string> on,
      std::vector<std::string> joins = {}) const;

//  [[nodiscard]]
//  TableHandle RangeJoin(const TableHandle &right_side, std::vector<std::string> on,
//      std::vector<Aggregate> aggregations) const;


  [[nodiscard]]
  TableHandle LeftOuterJoin(const TableHandle &right_side, std::vector<std::string> on,
      std::vector<std::string> joins) const;

  /**
   * Performs one or more UpdateByOperation ops grouped by zero or more key columns to calculate
   * cumulative or window-based aggregations of columns in a source table. Operations include
   * cumulative sums, moving averages, EMAs, etc. The aggregations are defined by the provided
   * operations, which support incremental aggregations over the corresponding rows in the source
   * table. Cumulative aggregations use all rows in the source table, whereas rolling aggregations
   * will apply position or time-based windowing relative to the current row. Calculations are
   * performed over all rows or each row group as identified by the provided key columns.
   * @param ops The requested UpdateByOperation ops
   * @param by The columns to group by
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle UpdateBy(std::vector<UpdateByOperation> ops, std::vector<std::string> by) const;
  /**
   * A variadic form of UpdateBy(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle UpdateBy(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return UpdateBy(std::move(vec));
  }

  /**
   * Creates a new table containing all of the unique values for a set of key columns.
   * When used on multiple columns, it looks for distinct sets of values in the selected columns.
   * @param columns The set of key columns
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle SelectDistinct(std::vector<std::string> columns) const;
  /**
   * A variadic form of SelectDistinct(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle SelectDistinct(Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return SelectDistinct(std::move(vec));
  }

  /**
   * Creates a new table containing rows from the source table, where the rows match values in the
   * filter table. The filter is updated whenever either table changes. See the Deephaven
   * documentation for the difference between "Where" and "WhereIn".
   * @param filter_table The table containing the set of values to filter on
   * @param columns The columns to match on
   * @return A TableHandle referencing the new table
   */
  [[nodiscard]]
  TableHandle WhereIn(const TableHandle &filter_table, std::vector<std::string> columns) const;
  /**
   * A variadic form of WhereIn(std::vector<std::string>) const that takes a combination of
   * argument types.
   * @tparam Args Any combination of `std::string`, `std::string_view`, or `const char *`
   * @param args The columns
   * @return A TableHandle referencing the new table
   */
  template<typename ...Args>
  [[nodiscard]]
  TableHandle WhereIn(const TableHandle &filter_table, Args &&...args) const {
    std::vector<std::string> vec{internal::ConvertToString::ToString(std::forward<Args>(args))...};
    return WhereIn(std::move(filter_table), std::move(vec));
  }

  /**
   * Adds a table to an input table. Requires that this object be an InputTable (such as that
   * created by TableHandleManager::InputTable).
   * @param table_to_add The table to add to the InputTable
   */
  void AddTable(const TableHandle &table_to_add);

  /**
   * Removes a table from an input table. Requires that this object be an InputTable (such as that
   * created by TableHandleManager::InputTable).
   * @param table_to_add The table to remove from the InputTable
   * @return The new table
   */
  void RemoveTable(const TableHandle &table_to_remove);

  /**
   * Binds this table to a variable name in the QueryScope.
   * @param variable The QueryScope variable to bind to.
   */
  void BindToVariable(std::string variable) const;

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
   * Read in the entire table as an Arrow table.
   * @return the Arrow table
   */
  [[nodiscard]]
  std::shared_ptr<arrow::Table> ToArrowTable() const;

  /**
   * Read in the entire table as a ClientTable.
   * @return the ClientTable
  */
  [[nodiscard]]
  std::shared_ptr<ClientTable> ToClientTable() const;

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
  std::shared_ptr<SubscriptionHandle> Subscribe(onTickCallback_t on_tick, void *on_tick_user_data,
      onErrorCallback_t on_error, void *on_error_user_data);
  /**
   * Unsubscribe from the table.
   */
  void Unsubscribe(const std::shared_ptr<SubscriptionHandle> &handle);

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
}  // namespace internal
}  // namespace deephaven::client
