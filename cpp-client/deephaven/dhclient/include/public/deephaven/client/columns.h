/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <string>
#include "deephaven/client/expressions.h"

namespace deephaven::client {
namespace impl {
class ColumnImpl;

class StrColImpl;

class NumColImpl;

class DateTimeColImpl;

class IrisRepresentableImpl;

class AssignedColumnImpl;
}  // namespace impl

/**
 * Base class for columns. Used internally.
 */
class IrisRepresentable {
public:
  /**
   * Constructor.
   */
  explicit IrisRepresentable(std::shared_ptr<impl::IrisRepresentableImpl> impl) : impl_(
      std::move(impl)) {}

  /**
   * Destructor.
   */
  virtual ~IrisRepresentable();

  /**
   * Gets underlying "Impl" member. Used internally.
   */
  [[nodiscard]]
  impl::IrisRepresentableImpl *GetIrisRepresentableImpl() const {
    return impl_.get();
  }

protected:
  /**
   * The underlying implementation.
   */
  std::shared_ptr<impl::IrisRepresentableImpl> impl_;
};

/**
 * A base class for representing columns in the fluent interface. Objects inheriting from
 * SelectColumn are suitable for use in methods like TableHandle::Select() const
 * Some common leaf classes are StrCol, NumCol, and DateTimeCol.
 */
class SelectColumn : public virtual IrisRepresentable {
public:
  /**
   * Constructor.
   */
  explicit SelectColumn(std::shared_ptr<impl::IrisRepresentableImpl> impl) : IrisRepresentable(
      std::move(impl)) {}

  /**
   * Destructor.
   */
  ~SelectColumn() override;
};

/**
 * A base class used for representing columns that can be used for matching, in methods like
 * TableHandle::ExactJoin(const TableHandle &, std::vector<MatchWithColumn> columnsToMatch, std::vector<SelectColumn> columnsToAdd) const
 */
class MatchWithColumn : public virtual IrisRepresentable {
public:
  /**
   * Constructor.
   */
  explicit MatchWithColumn(std::shared_ptr<impl::IrisRepresentableImpl> impl) : IrisRepresentable(
      std::move(impl)) {}

  /**
   * Destructor.
   */
  ~MatchWithColumn() override;
};

class SortPair;

/**
 * A base class for representing columns in the fluent interface.
 */
class Column : public SelectColumn, public MatchWithColumn {
public:
  /**
   * Constructor. Used internally.
   */
  explicit Column(std::shared_ptr<impl::ColumnImpl> impl);
  /**
   * Destructor.
   */
  ~Column() override;

  /**
   * Create a SortPair from this column, with direction_ set to ascending and abs_ set to the
   * incoming parameter.
   */
  [[nodiscard]]
  SortPair Ascending(bool abs = false) const;

  /**
   * Create a SortPair from this column, with direction_ set to descending and abs_ set to the
   * incoming parameter.
   */
  [[nodiscard]]
  SortPair Descending(bool abs = false) const;

  /**
   * Gets the name of this column.
   */
  [[nodiscard]]
  const std::string &Name() const;
};

/**
 * Describes a sort direction
 */
enum class SortDirection {
  kAscending, kDescending
};

/**
 * A tuple (not really a "pair", despite the name) representing a column to sort, the SortDirection,
 * and whether the Sort should consider the value's regular or absolute value when doing comparisons.
 */
class SortPair {
  using ColumnType = deephaven::client::Column;
public:
  /**
   * Create a SortPair with direction set to ascending.
   * @param column_name The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  [[nodiscard]]
  static SortPair Ascending(std::string column_name, bool abs = false);
  /**
   * Create a SortPair with direction set to ascending.
   * @param column The column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  [[nodiscard]]
  static SortPair Ascending(const ColumnType &column, bool abs = false);
  /**
   * Create a SortPair with direction set to descending.
   * @param column_name The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair Descending(std::string column_name, bool abs = false);
  /**
   * Create a SortPair with direction set to descending.
   * @param column The column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair Descending(const ColumnType &column, bool abs = false);

  /**
   * Constructor.
   */
  SortPair(std::string column, SortDirection direction, bool abs);
  /**
   * Destructor.
   */
  ~SortPair();

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
 * Represents a numeric column in the fluent interface. Typically created by TableHandle::GetNumCol()
 * or TableHandle::GetCols().
 * @code
 * auto col2 = table.GetNumCol("name2");
 * or
 * auto [col1, col2, col3] = table.GetCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class NumCol final : public NumericExpression, public Column {
public:
  /**
   * Create the NumCol. Used internally. Typically client code will call TableHandle::GetNumCol()
   * or TableHandle::GetCols<>().
   */
  [[nodiscard]]
  static NumCol Create(std::string name);
  /**
   * Used internally.
   */
  explicit NumCol(std::shared_ptr<impl::NumColImpl> impl);
  /**
   * Move constructor.
   */
  NumCol(NumCol &&) = default;
  /**
   * Move assignment operator.
   */
  NumCol &operator=(NumCol &&) = default;
  /**
   * Destructor.
   */
  ~NumCol() final;
};

/**
 * Represents a string column in the fluent interface. Typically created By
 * @code
 * auto col1 = table.GetStrCol("name1");
 * or
 * auto [col1, col2, col3] = table.GetCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class StrCol final : public StringExpression, public Column {
public:
  /**
   * Create the StrCol. Used internally. Typically client code will call TableHandle::GetStrCol()
   * or TableHandle::GetCols<>().
   */
  [[nodiscard]]
  static StrCol create(std::string name);
  /**
   * Used internally.
   */
  explicit StrCol(std::shared_ptr<impl::StrColImpl> impl);
  /**
   * Move constructor.
   */
  StrCol(StrCol &&) = default;
  /**
   * Move assignment operator.
   */
  StrCol &operator=(StrCol &&) = default;
  /**
   * Destructor.
   */
  ~StrCol() final;
};

/**
 * Represents a DateTime column in the fluent interface. Typically created By
 * @code
 * auto col3 = table.GetDateTimeCol("name3");
 * or
 * auto [col1, col2, col3] = table.GetCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class DateTimeCol final : public DateTimeExpression, public Column {
public:
  /**
   * Create the DateTimeCol. Used internally. Typically client code will call TableHandle::GetDateTimeCol()
   * or TableHandle::GetCols<>().
   */
  [[nodiscard]]
  static DateTimeCol Create(std::string name);
  /**
   * Used internally.
   */
  explicit DateTimeCol(std::shared_ptr<impl::DateTimeColImpl> impl);
  /**
   * Move constructor.
   */
  DateTimeCol(DateTimeCol &&) = default;
  /**
   * Move assignment operator.
   */
  DateTimeCol &operator=(DateTimeCol &&) = default;
  /**
   * Destructor.
   */
  ~DateTimeCol() final;
};

/**
 * Represents an expression that has been assigned to a new column name. Used for example in the
 * TableHandle::Select() method. Example:
 * @code
 * auto newTable = tableHandle.Select(A, B, (C + 5).as("NewCol"));
 * @endcode
 */
class AssignedColumn final : public SelectColumn {
public:
  /**
   * Create a new AssignedColumn. Used internally.
   */
  [[nodiscard]]
  static AssignedColumn Create(std::string name, std::shared_ptr<impl::ExpressionImpl> expression);
  /**
   * Constructor. Used internally.
   */
  explicit AssignedColumn(std::shared_ptr<impl::AssignedColumnImpl> impl);
  /**
   * Move constructor.
   */
  AssignedColumn(AssignedColumn &&) = default;
  /**
   * Move assignment operator.
   */
  AssignedColumn &operator=(AssignedColumn &&) = default;
  /**
   * Destructor.
   */
  ~AssignedColumn() final;

private:
  std::shared_ptr<impl::AssignedColumnImpl> impl_;
};
}  // namespace deephaven::client
