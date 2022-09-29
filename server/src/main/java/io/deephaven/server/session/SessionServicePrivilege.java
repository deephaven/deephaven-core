package io.deephaven.server.session;

import io.deephaven.auth.Privilege;

public enum SessionServicePrivilege implements Privilege {
    CAN_RELEASE, CAN_EXPORT_FROM_TICKET, CAN_SUBSCRIBE_EXPORT_NOTIFICATIONS, CAN_REQUEST_TERMINATION_NOTIFICATION,
}
