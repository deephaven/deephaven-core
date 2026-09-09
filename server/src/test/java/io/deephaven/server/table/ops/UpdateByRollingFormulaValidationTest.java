//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingFormula;
import io.deephaven.proto.backplane.grpc.UpdateByWindowScale;
import io.deephaven.proto.backplane.grpc.UpdateByWindowScale.UpdateByWindowTicks;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Server-level regression tests for the {@code UpdateByGrpcImpl} rolling-formula validation, specifically the
 * param-token substitution used to build the string the
 * {@link io.deephaven.engine.validation.ColumnExpressionValidator} inspects. The validator must substitute the token
 * exactly as the engine's {@code BaseRollingFormulaOperator} does (via {@code FormulaUtil.replaceFormulaTokens} — a
 * literal, token-aware replace), NOT with {@code String.replaceAll}, which would treat the user-supplied token as a
 * regex.
 */
public class UpdateByRollingFormulaValidationTest extends GrpcTableOperationTestBase<UpdateByRequest> {

    @Override
    public ExportedTableCreationResponse send(UpdateByRequest request) {
        return channel().tableBlocking().updateBy(request);
    }

    private static UpdateByWindowScale ticksScale(double ticks) {
        return UpdateByWindowScale.newBuilder()
                .setTicks(UpdateByWindowTicks.newBuilder().setTicks(ticks).build())
                .build();
    }

    private UpdateByRequest rollingFormulaRequest(String formula, String paramToken, String matchPair) {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "Value=(int)ii"));
        final UpdateByColumn.Builder column = UpdateByColumn.newBuilder()
                .setSpec(UpdateBySpec.newBuilder()
                        .setRollingFormula(UpdateByRollingFormula.newBuilder()
                                .setReverseWindowScale(ticksScale(5))
                                .setForwardWindowScale(ticksScale(0))
                                .setFormula(formula)
                                .setParamToken(paramToken)
                                .build())
                        .build());
        if (matchPair != null) {
            column.addMatchPairs(matchPair);
        }
        return UpdateByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addOperations(UpdateByOperation.newBuilder().setColumn(column.build()).build())
                .addGroupByColumns("Key")
                .build();
    }

    /**
     * A param token consisting of regex metacharacters ({@code .*}) that does not appear as a literal token in the
     * formula. With the old {@code String.replaceAll} substitution, {@code .*} matches (and replaces) the entire
     * formula, so the validator would inspect a benign column reference and let the disallowed {@code Runtime}
     * invocation through. With token-aware literal substitution, the token does not match, so the validator inspects
     * the real formula and rejects it.
     */
    @Test
    public void rollingFormulaRegexParamTokenDoesNotHideDisallowedExpression() {
        final UpdateByRequest request = rollingFormulaRequest(
                "Runtime.getRuntime().exec(\"pwned\") == null", ".*", "Out=Value");
        // The validator runs at export time, so its rejection is sanitized to "Details Logged w/ID"; a synchronous
        // request error (e.g. a bad window scale) would surface with its raw message instead, so this also confirms
        // the failure is the deferred ColumnExpressionValidator rather than earlier request validation.
        assertError(request, Code.INVALID_ARGUMENT, "Details Logged w/ID");
    }

    /**
     * A benign rolling formula whose param token is a substring of an unrelated identifier in the formula
     * ({@code sum}); a naive {@code replaceAll} would corrupt {@code sum} into {@code s<input>m}, breaking validation
     * of an otherwise valid request. Token-aware substitution only replaces the standalone token, so this validates and
     * builds successfully.
     */
    @Test
    public void rollingFormulaTokenInsideIdentifierStillSucceeds() {
        final UpdateByRequest request = rollingFormulaRequest("sum(u)", "u", "Out=Value");
        final ExportedTableCreationResponse response = send(request);
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }
}
