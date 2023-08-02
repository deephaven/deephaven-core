/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client.h"

#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/update_by.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::impl::moveVectorData;
using deephaven::client::impl::UpdateByOperationImpl;
using deephaven::dhcore::utility::stringf;
// typedef io::deephaven::proto::backplane::grpc::UpdateByDelta UpdateByDelta;
using io::deephaven::proto::backplane::grpc::UpdateByEmOptions;

typedef io::deephaven::proto::backplane::grpc::BadDataBehavior BadDataBehaviorProtoEnum;
typedef io::deephaven::proto::backplane::grpc::MathContext MathContextProto;
typedef io::deephaven::proto::backplane::grpc::MathContext::RoundingMode RoundingModeProtoEnum;
typedef io::deephaven::proto::backplane::grpc::UpdateByNullBehavior UpdateByNullBehavior;
//typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn UpdateByColumn;
typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn::UpdateBySpec UpdateBySpec;
typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation UpdateByOperationProto;
typedef io::deephaven::proto::backplane::grpc::UpdateByWindowScale::UpdateByWindowTime UpdateByWindowTime;

namespace deephaven::client {
UpdateByOperation::UpdateByOperation() = default;
UpdateByOperation::UpdateByOperation(std::shared_ptr<impl::UpdateByOperationImpl> impl) :
    impl_(std::move(impl)) {}
UpdateByOperation::UpdateByOperation(const UpdateByOperation &other) = default;
UpdateByOperation &UpdateByOperation::operator=(const UpdateByOperation &other) = default;
UpdateByOperation::UpdateByOperation(UpdateByOperation &&other) noexcept = default;
UpdateByOperation &UpdateByOperation::operator=(UpdateByOperation &&other) noexcept = default;
UpdateByOperation::~UpdateByOperation() = default;
}  // namespace deephaven::client

namespace deephaven::client::update_by {
namespace {
UpdateByNullBehavior convertDeltaControl(DeltaControl dc) {
  switch (dc) {
    case DeltaControl::NULL_DOMINATES: return UpdateByNullBehavior::NULL_DOMINATES;
    case DeltaControl::VALUE_DOMINATES: return UpdateByNullBehavior::VALUE_DOMINATES;
    case DeltaControl::ZERO_DOMINATES: return UpdateByNullBehavior::ZERO_DOMINATES;
    default: {
      auto message = stringf("Unexpected DeltaControl %o", (int)dc);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
}

BadDataBehaviorProtoEnum convertBadDataBehavior(BadDataBehavior bdb) {
  switch (bdb) {
    case BadDataBehavior::RESET: return BadDataBehaviorProtoEnum::RESET;
    case BadDataBehavior::SKIP: return BadDataBehaviorProtoEnum::SKIP;
    case BadDataBehavior::THROW: return BadDataBehaviorProtoEnum::THROW;
    case BadDataBehavior::POISON: return BadDataBehaviorProtoEnum::POISON;
    default: {
      auto message = stringf("Unexpected BadDataBehavior %o", (int)bdb);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
}

MathContextProto convertMathContext(MathContext mctx) {
  int32_t precision;
  RoundingModeProtoEnum roundingMode;
  switch (mctx) {
    // For the values used here, please see the documentation for java.math.MathContext:
    // https://docs.oracle.com/javase/8/docs/api/java/math/MathContext.html
    case MathContext::UNLIMITED: {
      // "A MathContext object whose settings have the values required for unlimited precision arithmetic."
      precision = 0;
      roundingMode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_UP;
      break;
    }
    case MathContext::DECIMAL32: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal32 format, 7 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 7;
      roundingMode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::DECIMAL64: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal64 format, 16 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 16;
      roundingMode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::DECIMAL128: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal128 format, 34 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 34;
      roundingMode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    default: {
      auto message = stringf("Unexpected MathContext %o", (int)mctx);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
  MathContextProto result;
  result.set_precision(precision);
  result.set_rounding_mode(roundingMode);
  return result;
}

UpdateByEmOptions convertOperationControl(const OperationControl &oc) {
  auto onNull = convertBadDataBehavior(oc.onNull);
  auto onNan = convertBadDataBehavior(oc.onNaN);
  auto bigValueContext = convertMathContext(oc.bigValueContext);

  UpdateByEmOptions result;
  result.set_on_null_value(onNull);
  result.set_on_nan_value(onNan);
  *result.mutable_big_value_context() = std::move(bigValueContext);
  return result;
};

/**
 * decayTime will be specified as either std::chrono::nanoseconds, or as a string.
 * If it is nanoseconds, we set the nanos field of the UpdateByWindowTime proto. Otherwise (if it is
 * a string), then we set the duration_string field.
 */
UpdateByWindowTime convertDecayTime(std::string timestampCol, durationSpecifier_t decayTime) {
  struct visitor_t {
    void operator()(std::chrono::nanoseconds nanos) {
      result.set_nanos(nanos.count());
    }
    void operator()(std::string duration) {
      *result.mutable_duration_string() = std::move(duration);
    }
    UpdateByWindowTime result;
  };
  visitor_t v;
  // Unconditionally set the column field with the value from timestampCol
  *v.result.mutable_column() = std::move(timestampCol);

  // Conditionally set either the nanos field or the duration_string with the nanoseconds or string
  // part of the variant.
  std::visit(v, std::move(decayTime));
  return std::move(v.result);
}

class UpdateByBuilder {
public:
  explicit UpdateByBuilder(std::vector<std::string> cols) {
    moveVectorData(std::move(cols), gup_.mutable_column()->mutable_match_pairs());
  }

  template<typename MEMBER>
  void touchEmpty(MEMBER mutableMember) {
    (void)(gup_.mutable_column()->mutable_spec()->*mutableMember)();
  }

  template<typename MEMBER>
  void setNullBehavior(MEMBER mutableMember, const DeltaControl deltaControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    auto nb = convertDeltaControl(deltaControl);
    which->mutable_options()->set_null_behavior(nb);
  }

  template<typename MEMBER>
  void setTicks(MEMBER mutableMember, const double decayTicks, const OperationControl &opControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_options() = convertOperationControl(opControl);
    which->mutable_window_scale()->mutable_ticks()->set_ticks(decayTicks);
  }

  template<typename MEMBER>
  void setTime(MEMBER mutableMember, std::string timestampCol, durationSpecifier_t decayTime,
      const OperationControl &opControl) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_options() = convertOperationControl(opControl);
    *which->mutable_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestampCol), std::move(decayTime));
  }

  template<typename MEMBER>
  void setRevAndFwdTicks(MEMBER mutableMember, const int revTicks, const int fwdTicks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    which->mutable_reverse_window_scale()->mutable_ticks()->set_ticks(revTicks);
    which->mutable_forward_window_scale()->mutable_ticks()->set_ticks(fwdTicks);
  }

  template<typename MEMBER>
  void setRevAndFwdTime(MEMBER mutableMember, std::string timestampCol,
      durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_reverse_window_scale()->mutable_time() =
        convertDecayTime(timestampCol, std::move(revTime));
    *which->mutable_forward_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestampCol), std::move(fwdTime));
  }

  template<typename MEMBER>
  void setWeightedRevAndFwdTicks(MEMBER mutableMember, std::string weightCol, const int revTicks,
      const int fwdTicks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_weight_column() = std::move(weightCol);
    setRevAndFwdTicks(mutableMember, revTicks, fwdTicks);
  }

  template<typename MEMBER>
  void setWeightedRevAndFwdTime(MEMBER mutableMember, std::string timestampCol,
      std::string weightCol, durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutableMember)();
    *which->mutable_weight_column() = std::move(weightCol);
    setRevAndFwdTime(mutableMember, std::move(timestampCol), std::move(revTime), std::move(fwdTime));
  }

  UpdateByOperation build() {
    auto impl = std::make_shared<UpdateByOperationImpl>(std::move(gup_));
    return UpdateByOperation(std::move(impl));
  }

  UpdateByOperationProto gup_;
};
}  // namespace

UpdateByOperation cumSum(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_sum);
  return ubb.build();
}

UpdateByOperation cumProd(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_product);
  return ubb.build();
}

UpdateByOperation cumMin(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_min);
  return ubb.build();
}

UpdateByOperation cumMax(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_max);
  return ubb.build();
}

UpdateByOperation forwardFill(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.touchEmpty(&UpdateBySpec::mutable_fill);
  return ubb.build();
}

UpdateByOperation delta(std::vector<std::string> cols, DeltaControl deltaControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setNullBehavior(&UpdateBySpec::mutable_delta, deltaControl);
  return ubb.build();
}

UpdateByOperation emaTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_ema, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emaTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_ema, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emsTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_ems, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emsTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_ems, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emminTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_min, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emminTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_min, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emmaxTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_max, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emmaxTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_max, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation emstdTick(double decayTicks, std::vector<std::string> cols,
    const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTicks(&UpdateBySpec::mutable_em_std, decayTicks, opControl);
  return ubb.build();
}

UpdateByOperation emstdTime(std::string timestampCol, durationSpecifier_t decayTime,
    std::vector<std::string> cols, const OperationControl &opControl) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setTime(&UpdateBySpec::mutable_em_std, std::move(timestampCol), std::move(decayTime), opControl);
  return ubb.build();
}

UpdateByOperation rollingSumTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_sum, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingSumTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_sum, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingGroupTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_group, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingGroupTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_group, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingAvgTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_avg, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingAvgTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_avg, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingMinTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_min, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingMinTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_min, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingMaxTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_max, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingMaxTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_max, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingProdTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_product, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingProdTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_product, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingCountTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_count, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingCountTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_count, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingStdTick(std::vector<std::string> cols, int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTicks(&UpdateBySpec::mutable_rolling_std, revTicks, fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingStdTime(std::string timestampCol, std::vector<std::string> cols,
    durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setRevAndFwdTime(&UpdateBySpec::mutable_rolling_std, std::move(timestampCol),
      std::move(revTime), std::move(fwdTime));
  return ubb.build();
}

UpdateByOperation rollingWavgTick(std::string weightCol, std::vector<std::string> cols,
    int revTicks, int fwdTicks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setWeightedRevAndFwdTicks(&UpdateBySpec::mutable_rolling_wavg, std::move(weightCol), revTicks,
      fwdTicks);
  return ubb.build();
}

UpdateByOperation rollingWavgTime(std::string timestampCol, std::string weightCol,
    std::vector<std::string> cols, durationSpecifier_t revTime, durationSpecifier_t fwdTime) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.setWeightedRevAndFwdTime(&UpdateBySpec::mutable_rolling_wavg, std::move(timestampCol),
      std::move(weightCol), std::move(revTime), std::move(fwdTime));
  return ubb.build();
}
}  // namespace deephaven::client::update_by
