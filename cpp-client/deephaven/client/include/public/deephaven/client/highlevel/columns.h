#pragma once

#include <memory>
#include <string>
#include "deephaven/client/highlevel/expressions.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class ColumnImpl;
class StrColImpl;
class NumColImpl;
class DateTimeColImpl;
class IrisRepresentableImpl;
class AssignedColumnImpl;
}  // namespace impl

class IrisRepresentable {
public:
  explicit IrisRepresentable(std::shared_ptr<impl::IrisRepresentableImpl> impl) : impl_(std::move(impl)) {}
  virtual ~IrisRepresentable();
  impl::IrisRepresentableImpl *getIrisRepresentableImpl() const {
    return impl_.get();
  }

protected:
  std::shared_ptr<impl::IrisRepresentableImpl> impl_;
};

class SelectColumn : public virtual IrisRepresentable {
public:
  explicit SelectColumn(std::shared_ptr<impl::IrisRepresentableImpl> impl) : IrisRepresentable(std::move(impl)) {}
  ~SelectColumn() override;
};

class MatchWithColumn : public virtual IrisRepresentable {
public:
  explicit MatchWithColumn(std::shared_ptr<impl::IrisRepresentableImpl> impl) : IrisRepresentable(std::move(impl)) {}
  ~MatchWithColumn() override;
};

class SortPair;

class Column : public SelectColumn, public MatchWithColumn {
public:
  explicit Column(std::shared_ptr<impl::ColumnImpl> impl);
  ~Column() override;

  SortPair ascending(bool abs = false) const;
  SortPair descending(bool abs = false) const;

  const std::string &name() const;
};

enum class SortDirection { Ascending, Descending };

class SortPair {
public:
  static SortPair ascending(std::string columnName, bool abs = false);
  static SortPair ascending(const Column &column, bool abs = false);
  static SortPair descending(std::string columnName, bool abs = false);
  static SortPair descending(const Column &column, bool abs = false);

  SortPair(std::string column, SortDirection direction, bool abs);
  ~SortPair();

  std::string &column() { return column_; }
  const std::string &column() const { return column_; }

  SortDirection direction() const { return direction_; }
  bool abs() const { return abs_; }

private:
  std::string column_;
  SortDirection direction_ = SortDirection::Ascending;
  bool abs_ = false;
};

class NumCol final : public NumericExpression, public Column {
public:
  static NumCol create(std::string name);
  explicit NumCol(std::shared_ptr<impl::NumColImpl> impl);
  NumCol(NumCol &&) = default;
  NumCol &operator=(NumCol &&) = default;
  ~NumCol() final;
};

class StrCol final : public StringExpression, public Column {
public:
  static StrCol create(std::string name);
  explicit StrCol(std::shared_ptr<impl::StrColImpl> impl);
  StrCol(StrCol &&) = default;
  StrCol &operator=(StrCol &&) = default;

  ~StrCol() final;
};

class DateTimeCol final : public DateTimeExpression, public Column {
public:
  static DateTimeCol create(std::string name);
  explicit DateTimeCol(std::shared_ptr<impl::DateTimeColImpl> impl);
  DateTimeCol(DateTimeCol &&) = default;
  DateTimeCol &operator=(DateTimeCol &&) = default;

  ~DateTimeCol() final;
};

class AssignedColumn final : public SelectColumn {
public:
  static AssignedColumn create(std::string name, std::shared_ptr<impl::ExpressionImpl> expression);

  explicit AssignedColumn(std::shared_ptr<impl::AssignedColumnImpl> impl);
  AssignedColumn(AssignedColumn &&) = default;
  AssignedColumn &operator=(AssignedColumn &&) = default;
  ~AssignedColumn() final;

private:
  std::shared_ptr<impl::AssignedColumnImpl> impl_;
};
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
