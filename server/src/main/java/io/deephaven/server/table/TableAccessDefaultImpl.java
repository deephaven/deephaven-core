package io.deephaven.server.table;

import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.Condition.DataCase;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.auth.AuthContext.SuperUser;

public class TableAccessDefaultImpl extends TableAccessOpenImpl {

    public TableAccessDefaultImpl(SessionService sessionService) {
        super(sessionService);
    }

    @Override
    public SessionState update(SelectOrUpdateRequest request) {
        return superUserOnly("update");
    }

    @Override
    public SessionState lazyUpdate(SelectOrUpdateRequest request) {
        return superUserOnly("lazyUpdate");
    }

    @Override
    public SessionState view(SelectOrUpdateRequest request) {
        return superUserOnly("view");
    }

    @Override
    public SessionState updateView(SelectOrUpdateRequest request) {
        return superUserOnly("updateView");
    }

    @Override
    public SessionState select(SelectOrUpdateRequest request) {
        return superUserOnly("select");
    }

    @Override
    public SessionState headBy(HeadOrTailByRequest request) {
        return superUserOnly("headBy");
    }

    @Override
    public SessionState tailBy(HeadOrTailByRequest request) {
        return superUserOnly("tailBy");
    }

    @Override
    public SessionState comboAggregate(ComboAggregateRequest request) {
        return superUserOnly("comboAggregate");
    }

    @Override
    public SessionState filter(FilterTableRequest request) {
        return check(() -> {
            for (Condition condition : request.getFiltersList()) {
                if (condition.getDataCase() == DataCase.INVOKE) {
                    throw GrpcUtil.statusRuntimeException(Code.PERMISSION_DENIED, "filter condition invoke denied");
                }
            }
        });
    }

    @Override
    public SessionState unstructuredFilter(UnstructuredFilterTableRequest request) {
        return superUserOnly("unstructuredFilter");
    }

    private SessionState superUserOnly(String name) {
        return check(() -> {
            throw GrpcUtil.statusRuntimeException(Code.PERMISSION_DENIED, String.format("%s denied", name));
        });
    }

    private SessionState check(Runnable runnable) {
        final SessionState currentSession = sessionService.getCurrentSession();
        if (currentSession.getAuthContext() instanceof SuperUser) {
            return currentSession;
        }
        runnable.run();
        return currentSession;
    }
}
