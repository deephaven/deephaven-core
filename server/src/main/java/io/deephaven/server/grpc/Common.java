package io.deephaven.server.grpc;

import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableReference.RefCase;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.grpc.GrpcErrorHelper;

public class Common {

    public static void validate(Ticket ticket) {
        GrpcErrorHelper.checkHasNoUnknownFields(ticket);
        if (ticket.getTicket().isEmpty()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Ticket is empty");
        }
    }

    public static void validate(TableReference tableReference) {
        // It's a bit unfortunate that generated protobuf objects don't have the names as constants (like it does with
        // field numbers). For example, TableReference.REF_NAME.
        GrpcErrorHelper.checkHasOneOf(tableReference, "ref");
        GrpcErrorHelper.checkHasNoUnknownFields(tableReference);
        final RefCase ref = tableReference.getRefCase();
        switch (ref) {
            case TICKET:
                validate(tableReference.getTicket());
                break;
            case BATCH_OFFSET:
                // valid
                // Should "structural" validation check it's >= 0?
                break;
            case REF_NOT_SET:
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL,
                        String.format("Server missing TableReference type %s", ref));
        }
    }
}
