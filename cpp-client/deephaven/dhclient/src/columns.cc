/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/columns.h"

#include "deephaven/client/impl/columns_impl.h"

namespace deephaven::client {
IrisRepresentable::~IrisRepresentable() = default;
SelectColumn::~SelectColumn() = default;

MatchWithColumn::~MatchWithColumn() = default;

Column::Column(std::shared_ptr<impl::ColumnImpl> impl) :
    IrisRepresentable(impl), SelectColumn(impl), MatchWithColumn(impl) {}

Column::~Column() = default;

const std::string &Column::Name() const {
  return dynamic_cast<const impl::ColumnImpl *>(impl_.get())->Name();
}

SortPair Column::Ascending(bool abs) const {
  return SortPair(Name(), SortDirection::kAscending, abs);
}

SortPair Column::Descending(bool abs) const {
  return SortPair(Name(), SortDirection::kDescending, abs);
}

SortPair SortPair::Ascending(std::string column_name, bool abs) {
  return SortPair(std::move(column_name), SortDirection::kAscending, abs);
}

SortPair SortPair::Ascending(const ColumnType &column, bool abs) {
  return SortPair(column.Name(), SortDirection::kAscending, abs);
}

SortPair SortPair::Descending(std::string columnName, bool abs) {
  return SortPair(std::move(columnName), SortDirection::kDescending, abs);
}

SortPair SortPair::Descending(const ColumnType &column, bool abs) {
  return SortPair(column.Name(), SortDirection::kDescending, abs);
}

SortPair::SortPair(std::string column, SortDirection direction, bool abs) :
    column_(std::move(column)), direction_(direction), abs_(abs) {}

SortPair::~SortPair() = default;

NumCol NumCol::Create(std::string name) {
  auto impl = impl::NumColImpl::Create(std::move(name));
  return NumCol(std::move(impl));
}

NumCol::NumCol(std::shared_ptr<impl::NumColImpl> impl) : IrisRepresentable(impl),
    NumericExpression(impl), Column(std::move(impl)) {}

NumCol::~NumCol() = default;

StrCol StrCol::create(std::string name) {
  auto impl = impl::StrColImpl::Create(std::move(name));
  return StrCol(std::move(impl));
}

StrCol::StrCol(std::shared_ptr<impl::StrColImpl> impl) : IrisRepresentable(impl),
    StringExpression(impl), Column(std::move(impl)) {}

StrCol::~StrCol() = default;

DateTimeCol DateTimeCol::Create(std::string name) {
  auto impl = impl::DateTimeColImpl::Create(std::move(name));
  return DateTimeCol(std::move(impl));
}

DateTimeCol::DateTimeCol(std::shared_ptr<impl::DateTimeColImpl> impl) : IrisRepresentable(impl),
    DateTimeExpression(impl), Column(std::move(impl)) {}

DateTimeCol::~DateTimeCol() = default;

AssignedColumn AssignedColumn::Create(std::string name,
    std::shared_ptr<impl::ExpressionImpl> expression) {
  auto impl = impl::AssignedColumnImpl::Create(std::move(name), std::move(expression));
  return AssignedColumn(std::move(impl));
}

AssignedColumn::AssignedColumn(std::shared_ptr<impl::AssignedColumnImpl> impl) :
    IrisRepresentable(impl), SelectColumn(impl), impl_(std::move(impl)) {}

AssignedColumn::~AssignedColumn() = default;
}  // namespace deephaven::client
