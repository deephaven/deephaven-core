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
   * Gets underlying "impl" member. Used internally.
   */
  impl::IrisRepresentableImpl *getIrisRepresentableImpl() const {
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
 * SelectColumn are suitable for use in methods like TableHandle::select() const
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
 * TableHandle::exactJoin(const TableHandle &, std::vector<MatchWithColumn> columnsToMatch, std::vector<SelectColumn> columnsToAdd) const
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
  SortPair ascending(bool abs = false) const;

  /**
   * Create a SortPair from this column, with direction_ set to descending and abs_ set to the
   * incoming parameter.
   */
  SortPair descending(bool abs = false) const;

  /**
   * Gets the name of this column.
   */
  const std::string &name() const;
};

/**
 * Describes a sort direction
 */
enum class SortDirection {
  Ascending, Descending
};

/**
 * A tuple (not really a "pair", despite the name) representing a column to sort, the SortDirection,
 * and whether the sort should consider the value's regular or absolute value when doing comparisons.
 */
class SortPair {
public:
  /**
   * Create a SortPair with direction set to ascending.
   * @param columnName The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair ascending(std::string columnName, bool abs = false);
  /**
   * Create a SortPair with direction set to ascending.
   * @param column The column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair ascending(const Column &column, bool abs = false);
  /**
   * Create a SortPair with direction set to descending.
   * @param columnName The name of the column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair descending(std::string columnName, bool abs = false);
  /**
   * Create a SortPair with direction set to descending.
   * @param column The column to sort.
   * @param abs If true, the data should be sorted by absolute value.
   * @return The SortPair tuple.
   */
  static SortPair descending(const Column &column, bool abs = false);

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
  std::string &column() { return column_; }

  /**
   * Get the column name
   * @return The column name
   */
  const std::string &column() const { return column_; }

  /**
   * Get the SortDirection
   * @return The SortDirection
   */
  SortDirection direction() const { return direction_; }

  /**
   * Get the "sort by absolute value" flag
   * @return
   */
  bool abs() const { return abs_; }

private:
  std::string column_;
  SortDirection direction_ = SortDirection::Ascending;
  bool abs_ = false;
};

/**
 * Represents a numeric column in the fluent interface. Typically created by TableHandle::getNumCol()
 * or TableHandle::getCols().
 * @code
 * auto col2 = table.getNumCol("name2");
 * or
 * auto [col1, col2, col3] = table.getCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class NumCol final : public NumericExpression, public Column {
public:
  /**
   * Create the NumCol. Used internally. Typically client code will call TableHandle::getNumCol()
   * or TableHandle::getCols<>().
   */
  static NumCol create(std::string name);
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
 * Represents a string column in the fluent interface. Typically created by
 * @code
 * auto col1 = table.getStrCol("name1");
 * or
 * auto [col1, col2, col3] = table.getCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class StrCol final : public StringExpression, public Column {
public:
  /**
   * Create the StrCol. Used internally. Typically client code will call TableHandle::getStrCol()
   * or TableHandle::getCols<>().
   */
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
 * Represents a DateTime column in the fluent interface. Typically created by
 * @code
 * auto col3 = table.getDateTimeCol("name3");
 * or
 * auto [col1, col2, col3] = table.getCols<StrCol, NumCol, DateTimeCol>("name1", "name2", "name3")
 * @endcode
 */
class DateTimeCol final : public DateTimeExpression, public Column {
public:
  /**
   * Create the DateTimeCol. Used internally. Typically client code will call TableHandle::getDateTimeCol()
   * or TableHandle::getCols<>().
   */
  static DateTimeCol create(std::string name);
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
 * TableHandle::select() method. Example:
 * @code
 * auto newTable = tableHandle.select(A, B, (C + 5).as("NewCol"));
 * @endcode
 */
class AssignedColumn final : public SelectColumn {
public:
  /**
   * Create a new AssignedColumn. Used internally.
   */
  static AssignedColumn create(std::string name, std::shared_ptr<impl::ExpressionImpl> expression);
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
