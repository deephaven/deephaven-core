/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/client.h"

#include "deephaven_core/proto/table.pb.h"
#include "deephaven_core/proto/table.grpc.pb.h"
#include "deephaven/client/impl/update_by_operation_impl.h"
#include "deephaven/client/impl/util.h"
#include "deephaven/client/update_by.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::impl::MoveVectorData;
using deephaven::client::impl::UpdateByOperationImpl;
// typedef io::deephaven::proto::backplane::grpc::UpdateByDelta UpdateByDelta;
using io::deephaven::proto::backplane::grpc::UpdateByEmOptions;

using BadDataBehaviorProtoEnum = io::deephaven::proto::backplane::grpc::BadDataBehavior;
using MathContextProto = io::deephaven::proto::backplane::grpc::MathContext;
using RoundingModeProtoEnum = io::deephaven::proto::backplane::grpc::MathContext::RoundingMode;
using UpdateByNullBehavior = io::deephaven::proto::backplane::grpc::UpdateByNullBehavior;
//typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn UpdateByColumn;
using UpdateBySpec = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation::UpdateByColumn::UpdateBySpec;
using UpdateByOperationProto = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;
using UpdateByWindowTime = io::deephaven::proto::backplane::grpc::UpdateByWindowScale::UpdateByWindowTime;
using DurationSpecifier = deephaven::client::utility::DurationSpecifier;

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
    case DeltaControl::kNullDominates: return UpdateByNullBehavior::NULL_DOMINATES;
    case DeltaControl::kValueDominates: return UpdateByNullBehavior::VALUE_DOMINATES;
    case DeltaControl::kZeroDominates: return UpdateByNullBehavior::ZERO_DOMINATES;
    default: {
      auto message = fmt::format("Unexpected DeltaControl {}", static_cast<int>(dc));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

BadDataBehaviorProtoEnum convertBadDataBehavior(BadDataBehavior bdb) {
  switch (bdb) {
    case BadDataBehavior::kReset: return BadDataBehaviorProtoEnum::RESET;
    case BadDataBehavior::kSkip: return BadDataBehaviorProtoEnum::SKIP;
    case BadDataBehavior::kThrow: return BadDataBehaviorProtoEnum::THROW;
    case BadDataBehavior::kPoison: return BadDataBehaviorProtoEnum::POISON;
    default: {
      auto message = fmt::format("Unexpected BadDataBehavior {}", static_cast<int>(bdb));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

MathContextProto convertMathContext(MathContext mctx) {
  int32_t precision;
  RoundingModeProtoEnum rounding_mode;
  switch (mctx) {
    // For the values used here, please see the documentation for java.math.MathContext:
    // https://docs.oracle.com/javase/8/docs/api/java/math/MathContext.html
    case MathContext::kUnlimited: {
      // "A MathContext object whose settings have the values required for unlimited precision arithmetic."
      precision = 0;
      rounding_mode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_UP;
      break;
    }
    case MathContext::kDecimal32: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal32 format, 7 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 7;
      rounding_mode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::kDecimal64: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal64 format, 16 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 16;
      rounding_mode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    case MathContext::kDecimal128: {
      // "A MathContext object with a precision setting matching the IEEE 754R Decimal128 format, 34 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      precision = 34;
      rounding_mode = RoundingModeProtoEnum::MathContext_RoundingMode_HALF_EVEN;
      break;
    }
    default: {
      auto message = fmt::format("Unexpected MathContext {}", static_cast<int>(mctx));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
  MathContextProto result;
  result.set_precision(precision);
  result.set_rounding_mode(rounding_mode);
  return result;
}

UpdateByEmOptions convertOperationControl(const OperationControl &oc) {
  auto on_null = convertBadDataBehavior(oc.on_null);
  auto on_nan = convertBadDataBehavior(oc.on_nan);
  auto big_value_context = convertMathContext(oc.big_value_context);

  UpdateByEmOptions result;
  result.set_on_null_value(on_null);
  result.set_on_nan_value(on_nan);
  *result.mutable_big_value_context() = std::move(big_value_context);
  return result;
}

/**
 * decayTime will be specified as either std::chrono::nanoseconds, or as a string.
 * If it is nanoseconds, we set the nanos field of the UpdateByWindowTime proto. Otherwise (if it is
 * a string), then we set the duration_string field.
 */
UpdateByWindowTime convertDecayTime(std::string timestamp_col, DurationSpecifier decay_time) {
  struct Visitor {
    void operator()(std::chrono::nanoseconds nanos) {
      result.set_nanos(nanos.count());
    }
    void operator()(int64_t nanos) {
      result.set_nanos(nanos);
    }
    void operator()(std::string duration) {
      *result.mutable_duration_string() = std::move(duration);
    }
    UpdateByWindowTime result;
  };
  Visitor v;
  // Unconditionally set the column field with the value from timestampCol
  *v.result.mutable_column() = std::move(timestamp_col);

  // Conditionally set either the nanos field or the duration_string with the nanoseconds or string
  // part of the variant.
  std::visit(v, std::move(decay_time));
  return std::move(v.result);
}

class UpdateByBuilder {
public:
  explicit UpdateByBuilder(std::vector<std::string> cols) {
    MoveVectorData(std::move(cols), gup_.mutable_column()->mutable_match_pairs());
  }

  template<typename Member>
  void TouchEmpty(Member mutable_member) {
    (void)(gup_.mutable_column()->mutable_spec()->*mutable_member)();
  }

  template<typename Member>
  void SetNullBehavior(Member mutable_member, const DeltaControl delta_control) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    auto nb = convertDeltaControl(delta_control);
    which->mutable_options()->set_null_behavior(nb);
  }

  template<typename Member>
  void SetTicks(Member mutable_member, double decay_ticks, const OperationControl &op_control) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    *which->mutable_options() = convertOperationControl(op_control);
    which->mutable_window_scale()->mutable_ticks()->set_ticks(decay_ticks);
  }

  template<typename Member>
  void SetTime(Member mutable_member, std::string timestamp_col, DurationSpecifier decay_time,
      const OperationControl &op_control) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    *which->mutable_options() = convertOperationControl(op_control);
    *which->mutable_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestamp_col), std::move(decay_time));
  }

  template<typename Member>
  void SetRevAndFwdTicks(Member mutable_member, int rev_ticks, int fwd_ticks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    which->mutable_reverse_window_scale()->mutable_ticks()->set_ticks(rev_ticks);
    which->mutable_forward_window_scale()->mutable_ticks()->set_ticks(fwd_ticks);
  }

  template<typename Member>
  void SetRevAndFwdTime(Member mutable_member, std::string timestamp_col,
      DurationSpecifier rev_time, DurationSpecifier fwd_time) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    *which->mutable_reverse_window_scale()->mutable_time() =
        convertDecayTime(timestamp_col, std::move(rev_time));
    *which->mutable_forward_window_scale()->mutable_time() =
        convertDecayTime(std::move(timestamp_col), std::move(fwd_time));
  }

  template<typename Member>
  void SetWeightedRevAndFwdTicks(Member mutable_member, std::string weight_col, const int rev_ticks,
      const int fwd_ticks) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    *which->mutable_weight_column() = std::move(weight_col);
    SetRevAndFwdTicks(mutable_member, rev_ticks, fwd_ticks);
  }

  template<typename Member>
  void SetWeightedRevAndFwdTime(Member mutable_member, std::string timestamp_col,
      std::string weight_col, DurationSpecifier rev_time, DurationSpecifier fwd_time) {
    auto *which = (gup_.mutable_column()->mutable_spec()->*mutable_member)();
    *which->mutable_weight_column() = std::move(weight_col);
    SetRevAndFwdTime(mutable_member, std::move(timestamp_col), std::move(rev_time), std::move(fwd_time));
  }

  UpdateByOperation Build() {
    auto impl = std::make_shared<UpdateByOperationImpl>(std::move(gup_));
    return UpdateByOperation(std::move(impl));
  }

  UpdateByOperationProto gup_;
};
}  // namespace

UpdateByOperation cumSum(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.TouchEmpty(&UpdateBySpec::mutable_sum);
  return ubb.Build();
}

UpdateByOperation cumProd(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.TouchEmpty(&UpdateBySpec::mutable_product);
  return ubb.Build();
}

UpdateByOperation cumMin(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.TouchEmpty(&UpdateBySpec::mutable_min);
  return ubb.Build();
}

UpdateByOperation cumMax(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.TouchEmpty(&UpdateBySpec::mutable_max);
  return ubb.Build();
}

UpdateByOperation forwardFill(std::vector<std::string> cols) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.TouchEmpty(&UpdateBySpec::mutable_fill);
  return ubb.Build();
}

UpdateByOperation delta(std::vector<std::string> cols, DeltaControl delta_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetNullBehavior(&UpdateBySpec::mutable_delta, delta_control);
  return ubb.Build();
}

UpdateByOperation emaTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTicks(&UpdateBySpec::mutable_ema, decay_ticks, op_control);
  return ubb.Build();
}

UpdateByOperation emaTime(std::string timestamp_col, DurationSpecifier decay_time,
    std::vector<std::string> cols, const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTime(&UpdateBySpec::mutable_ema, std::move(timestamp_col), std::move(decay_time), op_control);
  return ubb.Build();
}

UpdateByOperation emsTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTicks(&UpdateBySpec::mutable_ems, decay_ticks, op_control);
  return ubb.Build();
}

UpdateByOperation emsTime(std::string timestamp_col, DurationSpecifier decay_time,
    std::vector<std::string> cols, const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTime(&UpdateBySpec::mutable_ems, std::move(timestamp_col), std::move(decay_time), op_control);
  return ubb.Build();
}

UpdateByOperation emminTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTicks(&UpdateBySpec::mutable_em_min, decay_ticks, op_control);
  return ubb.Build();
}

UpdateByOperation emminTime(std::string timestamp_col, DurationSpecifier decay_time,
    std::vector<std::string> cols, const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTime(&UpdateBySpec::mutable_em_min, std::move(timestamp_col), std::move(decay_time), op_control);
  return ubb.Build();
}

UpdateByOperation emmaxTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTicks(&UpdateBySpec::mutable_em_max, decay_ticks, op_control);
  return ubb.Build();
}

UpdateByOperation emmaxTime(std::string timestamp_col, DurationSpecifier decay_time,
    std::vector<std::string> cols, const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTime(&UpdateBySpec::mutable_em_max, std::move(timestamp_col), std::move(decay_time), op_control);
  return ubb.Build();
}

UpdateByOperation emstdTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTicks(&UpdateBySpec::mutable_em_std, decay_ticks, op_control);
  return ubb.Build();
}

UpdateByOperation emstdTime(std::string timestamp_col, DurationSpecifier decay_time,
    std::vector<std::string> cols, const OperationControl &op_control) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetTime(&UpdateBySpec::mutable_em_std, std::move(timestamp_col), std::move(decay_time), op_control);
  return ubb.Build();
}

UpdateByOperation rollingSumTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_sum, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingSumTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_sum, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingGroupTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_group, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingGroupTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_group, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingAvgTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_avg, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingAvgTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_avg, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingMinTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_min, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingMinTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_min, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingMaxTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_max, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingMaxTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_max, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingProdTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_product, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingProdTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_product, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingCountTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_count, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingCountTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_count, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingStdTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTicks(&UpdateBySpec::mutable_rolling_std, rev_ticks, fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingStdTime(std::string timestamp_col, std::vector<std::string> cols,
    DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetRevAndFwdTime(&UpdateBySpec::mutable_rolling_std, std::move(timestamp_col),
      std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}

UpdateByOperation rollingWavgTick(std::string weight_col, std::vector<std::string> cols,
    int rev_ticks, int fwd_ticks) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetWeightedRevAndFwdTicks(&UpdateBySpec::mutable_rolling_wavg, std::move(weight_col), rev_ticks,
      fwd_ticks);
  return ubb.Build();
}

UpdateByOperation rollingWavgTime(std::string timestamp_col, std::string weight_col,
    std::vector<std::string> cols, DurationSpecifier rev_time, DurationSpecifier fwd_time) {
  UpdateByBuilder ubb(std::move(cols));
  ubb.SetWeightedRevAndFwdTime(&UpdateBySpec::mutable_rolling_wavg, std::move(timestamp_col),
      std::move(weight_col), std::move(rev_time), std::move(fwd_time));
  return ubb.Build();
}
}  // namespace deephaven::client::update_by
