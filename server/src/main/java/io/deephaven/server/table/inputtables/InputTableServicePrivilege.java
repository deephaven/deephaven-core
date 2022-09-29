package io.deephaven.server.table.inputtables;

import io.deephaven.auth.Privilege;

public enum InputTableServicePrivilege implements Privilege {
    CAN_ADD_TO_INPUT_TABLE, CAN_DELETE_FROM_INPUT_TABLE,
}
