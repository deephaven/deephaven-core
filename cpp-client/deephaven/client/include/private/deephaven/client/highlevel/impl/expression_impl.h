#pragma once

#include <iostream>
#include <memory>

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class BooleanExpressionImpl;

class IrisRepresentableImpl {
public:
  virtual ~IrisRepresentableImpl();
  virtual void streamIrisRepresentation(std::ostream &s) const = 0;
};

void streamIris(std::ostream &s, const std::shared_ptr<IrisRepresentableImpl> &o);

class ExpressionImpl : public virtual IrisRepresentableImpl {
public:
  static std::shared_ptr<BooleanExpressionImpl> createIsNull(std::shared_ptr<ExpressionImpl> impl);
  ~ExpressionImpl() override;
};
}  // namespace impl {
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
