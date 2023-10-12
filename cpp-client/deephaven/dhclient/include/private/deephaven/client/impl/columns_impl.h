/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/impl/datetime_expression_impl.h"
#include "deephaven/client/impl/expression_impl.h"
#include "deephaven/client/impl/numeric_expression_impl.h"
#include "deephaven/client/impl/string_expression_impl.h"

namespace deephaven::client::impl {
class ColumnImpl : public ExpressionImpl {
protected:
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<ColumnImpl> Create(std::string name);

  ColumnImpl(Private, std::string name);
  ~ColumnImpl() override;

  [[nodiscard]]
  const std::string &Name() const { return name_; }

  void StreamIrisRepresentation(std::ostream &s) const final;

private:
  std::string name_;
};

class NumColImpl final : public NumericExpressionImpl, public ColumnImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<NumColImpl> Create(std::string name);

  NumColImpl(Private, std::string name);
  ~NumColImpl() final;
};

class StrColImpl final : public StringExpressionImpl, public ColumnImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<StrColImpl> Create(std::string name);

  StrColImpl(Private, std::string name);
  ~StrColImpl() final;
};

class DateTimeColImpl final : public DateTimeExpressionImpl, public ColumnImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<DateTimeColImpl> Create(std::string name);

  DateTimeColImpl(Private, std::string name);
  ~DateTimeColImpl() final;
};

class AssignedColumnImpl final : public IrisRepresentableImpl {
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<AssignedColumnImpl> Create(std::string name,
      std::shared_ptr<ExpressionImpl> expr);

  AssignedColumnImpl(Private, std::string name, std::shared_ptr<ExpressionImpl> expr);
  ~AssignedColumnImpl() final;
  void StreamIrisRepresentation(std::ostream &s) const final;

private:
  std::string name_;
  std::shared_ptr<ExpressionImpl> expr_;
};
}  // namespace deephaven::client::impl
