//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface UpdateBySpecUnion {
    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeSum asCumulativeSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeMin asCumulativeMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeMax asCumulativeMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeProduct asCumulativeProduct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByFill asFill() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEma asEma() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingSum asRollingSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingGroup asRollingGroup() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingAvg asRollingAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingMin asRollingMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingMax asRollingMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingProduct asRollingProduct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByDelta asDelta() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEms asEms() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmMin asEmMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmMax asEmMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmStd asEmStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingCount asRollingCount() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingStd asRollingStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingWAvg asRollingWAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingFormula asRollingFormula() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingCountWhere asRollingCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeCountWhere asCumulativeCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsProperty
    String getType();

    @JsOverlay
    default UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.Builder makeOperation() {
        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.Builder builder =
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.newBuilder();

        switch (getType()) {
            case "UpdateByCumulativeSum":
                builder.setSum(UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum
                        .getDefaultInstance());
                break;
            case "UpdateByCumulativeMin":
                builder.setMin(UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMin
                        .getDefaultInstance());
                break;
            case "UpdateByCumulativeMax":
                builder.setMax(UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMax
                        .getDefaultInstance());
                break;
            case "UpdateByCumulativeProduct":
                builder.setProduct(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct
                                .getDefaultInstance());
                break;
            case "UpdateByFill":
                builder.setFill(UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByFill
                        .getDefaultInstance());
                break;
            case "UpdateByEma": {
                io.deephaven.web.client.api.updateby.UpdateByEma ema = asEma();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.newBuilder()
                                .setWindowScale(ema.windowScale.toProto());
                if (ema.emOptions != null) {
                    b.setOptions(ema.emOptions.toProto());
                }
                builder.setEma(b);
                break;
            }
            case "UpdateByEms": {
                io.deephaven.web.client.api.updateby.UpdateByEms ems = asEms();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEms.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEms.newBuilder()
                                .setWindowScale(ems.windowScale.toProto());
                if (ems.emOptions != null) {
                    b.setOptions(ems.emOptions.toProto());
                }
                builder.setEms(b);
                break;
            }
            case "UpdateByEmMin": {
                io.deephaven.web.client.api.updateby.UpdateByEmMin emMin = asEmMin();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmMin.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmMin.newBuilder()
                                .setWindowScale(emMin.windowScale.toProto());
                if (emMin.emOptions != null) {
                    b.setOptions(emMin.emOptions.toProto());
                }
                builder.setEmMin(b);
                break;
            }
            case "UpdateByEmMax": {
                io.deephaven.web.client.api.updateby.UpdateByEmMax emMax = asEmMax();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmMax.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmMax.newBuilder()
                                .setWindowScale(emMax.windowScale.toProto());
                if (emMax.emOptions != null) {
                    b.setOptions(emMax.emOptions.toProto());
                }
                builder.setEmMax(b);
                break;
            }
            case "UpdateByEmStd": {
                io.deephaven.web.client.api.updateby.UpdateByEmStd emStd = asEmStd();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmStd.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmStd.newBuilder()
                                .setWindowScale(emStd.windowScale.toProto());
                if (emStd.emOptions != null) {
                    b.setOptions(emStd.emOptions.toProto());
                }
                builder.setEmStd(b);
                break;
            }
            case "UpdateByDelta": {
                io.deephaven.web.client.api.updateby.UpdateByDelta delta = asDelta();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByDelta.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByDelta.newBuilder();
                if (delta.nullBehavior != null) {
                    b.setOptions(io.deephaven.proto.backplane.grpc.UpdateByDeltaOptions.newBuilder()
                            .setNullBehavior(io.deephaven.proto.backplane.grpc.UpdateByNullBehavior
                                    .valueOf(delta.nullBehavior.toString())));
                }
                builder.setDelta(b);
                break;
            }
            case "UpdateByRollingSum": {
                io.deephaven.web.client.api.updateby.UpdateByRollingSum rs = asRollingSum();
                builder.setRollingSum(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingSum.newBuilder()
                                .setReverseWindowScale(rs.reverseWindowScale.toProto())
                                .setForwardWindowScale(rs.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingGroup": {
                io.deephaven.web.client.api.updateby.UpdateByRollingGroup rg = asRollingGroup();
                builder.setRollingGroup(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingGroup.newBuilder()
                                .setReverseWindowScale(rg.reverseWindowScale.toProto())
                                .setForwardWindowScale(rg.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingAvg": {
                io.deephaven.web.client.api.updateby.UpdateByRollingAvg ra = asRollingAvg();
                builder.setRollingAvg(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingAvg.newBuilder()
                                .setReverseWindowScale(ra.reverseWindowScale.toProto())
                                .setForwardWindowScale(ra.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingMin": {
                io.deephaven.web.client.api.updateby.UpdateByRollingMin rm = asRollingMin();
                builder.setRollingMin(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingMin.newBuilder()
                                .setReverseWindowScale(rm.reverseWindowScale.toProto())
                                .setForwardWindowScale(rm.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingMax": {
                io.deephaven.web.client.api.updateby.UpdateByRollingMax rx = asRollingMax();
                builder.setRollingMax(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingMax.newBuilder()
                                .setReverseWindowScale(rx.reverseWindowScale.toProto())
                                .setForwardWindowScale(rx.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingProduct": {
                io.deephaven.web.client.api.updateby.UpdateByRollingProduct rp = asRollingProduct();
                builder.setRollingProduct(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingProduct
                                .newBuilder()
                                .setReverseWindowScale(rp.reverseWindowScale.toProto())
                                .setForwardWindowScale(rp.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingCount": {
                io.deephaven.web.client.api.updateby.UpdateByRollingCount rc = asRollingCount();
                builder.setRollingCount(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingCount.newBuilder()
                                .setReverseWindowScale(rc.reverseWindowScale.toProto())
                                .setForwardWindowScale(rc.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingStd": {
                io.deephaven.web.client.api.updateby.UpdateByRollingStd rstd = asRollingStd();
                builder.setRollingStd(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingStd.newBuilder()
                                .setReverseWindowScale(rstd.reverseWindowScale.toProto())
                                .setForwardWindowScale(rstd.forwardWindowScale.toProto()));
                break;
            }
            case "UpdateByRollingWAvg": {
                io.deephaven.web.client.api.updateby.UpdateByRollingWAvg rwavg = asRollingWAvg();
                builder.setRollingWavg(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingWAvg.newBuilder()
                                .setReverseWindowScale(rwavg.reverseWindowScale.toProto())
                                .setForwardWindowScale(rwavg.forwardWindowScale.toProto())
                                .setWeightColumn(Column.ColumnOrName.COLUMN_NAME.apply(rwavg.weightColumn)));
                break;
            }
            case "UpdateByRollingFormula": {
                io.deephaven.web.client.api.updateby.UpdateByRollingFormula rf = asRollingFormula();
                UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingFormula.Builder b =
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingFormula
                                .newBuilder()
                                .setReverseWindowScale(rf.reverseWindowScale.toProto())
                                .setForwardWindowScale(rf.forwardWindowScale.toProto())
                                .setFormula(rf.formula);
                if (rf.paramToken != null) {
                    b.setParamToken(rf.paramToken);
                }
                builder.setRollingFormula(b);
                break;
            }
            case "UpdateByRollingCountWhere": {
                io.deephaven.web.client.api.updateby.UpdateByRollingCountWhere rcw =
                        asRollingCountWhere();
                builder.setRollingCountWhere(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingCountWhere
                                .newBuilder()
                                .setReverseWindowScale(rcw.reverseWindowScale.toProto())
                                .setForwardWindowScale(rcw.forwardWindowScale.toProto())
                                .setResultColumn(rcw.resultColumn)
                                .addAllFilters(rcw.filters.asList()));
                break;
            }
            case "UpdateByCumulativeCountWhere": {
                io.deephaven.web.client.api.updateby.UpdateByCumulativeCountWhere ccw =
                        asCumulativeCountWhere();
                builder.setCountWhere(
                        UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeCountWhere
                                .newBuilder()
                                .setResultColumn(ccw.resultColumn)
                                .addAllFilters(ccw.filters.asList()));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported UpdateBy spec type: " + getType());
        }

        return builder;
    }

}
