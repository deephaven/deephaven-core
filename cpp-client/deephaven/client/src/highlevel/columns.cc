#include "deephaven/client/highlevel/columns.h"

#include "deephaven/client/highlevel/impl/columns_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
IrisRepresentable::~IrisRepresentable() = default;
SelectColumn::~SelectColumn() = default;

MatchWithColumn::~MatchWithColumn() = default;

Column::Column(std::shared_ptr<impl::ColumnImpl> impl) :
    IrisRepresentable(impl), SelectColumn(impl), MatchWithColumn(impl) {}
Column::~Column() = default;

const std::string & Column::name() const {
  return dynamic_cast<const impl::ColumnImpl*>(impl_.get())->name();
}

SortPair Column::ascending(bool abs) const {
  return SortPair(name(), SortDirection::Ascending, abs);
}

SortPair Column::descending(bool abs) const {
  return SortPair(name(), SortDirection::Descending, abs);
}

SortPair SortPair::ascending(std::string columnName, bool abs) {
  return SortPair(std::move(columnName), SortDirection::Ascending, abs);
}

SortPair SortPair::ascending(const Column &column, bool abs) {
  return SortPair(column.name(), SortDirection::Ascending, abs);
}

SortPair SortPair::descending(std::string columnName, bool abs) {
  return SortPair(std::move(columnName), SortDirection::Descending, abs);
}

SortPair SortPair::descending(const Column &column, bool abs) {
  return SortPair(column.name(), SortDirection::Descending, abs);
}

SortPair::SortPair(std::string column, SortDirection direction, bool abs) :
    column_(std::move(column)), direction_(direction), abs_(abs) {}
SortPair::~SortPair() = default;

NumCol NumCol::create(std::string name) {
  auto impl = impl::NumColImpl::create(std::move(name));
  return NumCol(std::move(impl));
}

NumCol::NumCol(std::shared_ptr<impl::NumColImpl> impl) : IrisRepresentable(impl),
    NumericExpression(impl), Column(std::move(impl)) {}
NumCol::~NumCol() = default;

StrCol StrCol::create(std::string name) {
  auto impl = impl::StrColImpl::create(std::move(name));
  return StrCol(std::move(impl));
}

StrCol::StrCol(std::shared_ptr<impl::StrColImpl> impl) : IrisRepresentable(impl),
    StringExpression(impl), Column(std::move(impl)) {}
StrCol::~StrCol() = default;

DateTimeCol DateTimeCol::create(std::string name) {
  auto impl = impl::DateTimeColImpl::create(std::move(name));
  return DateTimeCol(std::move(impl));
}
DateTimeCol::DateTimeCol(std::shared_ptr<impl::DateTimeColImpl> impl) : IrisRepresentable(impl),
    DateTimeExpression(impl), Column(std::move(impl)) {}
DateTimeCol::~DateTimeCol() = default;

AssignedColumn AssignedColumn::create(std::string name,
    std::shared_ptr<impl::ExpressionImpl> expression) {
  auto impl = impl::AssignedColumnImpl::create(std::move(name), std::move(expression));
  return AssignedColumn(std::move(impl));
}

AssignedColumn::AssignedColumn(std::shared_ptr<impl::AssignedColumnImpl> impl) :
    IrisRepresentable(impl), SelectColumn(impl), impl_(std::move(impl)) {}
AssignedColumn::~AssignedColumn() = default;

}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
