#pragma once

#include <memory>
#include "deephaven/client/highlevel/impl/datetime_expression_impl.h"
#include "deephaven/client/highlevel/impl/expression_impl.h"
#include "deephaven/client/highlevel/impl/numeric_expression_impl.h"
#include "deephaven/client/highlevel/impl/string_expression_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class ColumnImpl : public ExpressionImpl {
protected:
  struct Private {};

public:
  static std::shared_ptr<ColumnImpl> create(std::string name);

  ColumnImpl(Private, std::string name);
  ~ColumnImpl() override;

  const std::string &name() const { return name_; }

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::string name_;
};

class NumColImpl final : public NumericExpressionImpl, public ColumnImpl {
public:
  static std::shared_ptr<NumColImpl> create(std::string name);

  NumColImpl(Private, std::string name);
  ~NumColImpl() final;
};

class StrColImpl final : public StringExpressionImpl, public ColumnImpl {
public:
  static std::shared_ptr<StrColImpl> create(std::string name);

  StrColImpl(Private, std::string name);
  ~StrColImpl() final;
};

class DateTimeColImpl final : public DateTimeExpressionImpl, public ColumnImpl {
public:
  static std::shared_ptr<DateTimeColImpl> create(std::string name);

  DateTimeColImpl(Private, std::string name);
  ~DateTimeColImpl() final;
};

class AssignedColumnImpl final : public IrisRepresentableImpl {
  struct Private {};

public:
  static std::shared_ptr<AssignedColumnImpl> create(std::string name,
      std::shared_ptr<ExpressionImpl> expr);

  AssignedColumnImpl(Private, std::string name, std::shared_ptr<ExpressionImpl> expr);
  ~AssignedColumnImpl() final;
  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::string name_;
  std::shared_ptr<ExpressionImpl> expr_;
};

}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
