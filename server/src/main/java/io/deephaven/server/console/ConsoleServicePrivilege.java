package io.deephaven.server.console;

import io.deephaven.auth.Privilege;

public enum ConsoleServicePrivilege implements Privilege {
    CAN_GET_CONSOLE_TYPES, CAN_START_CONSOLE, CAN_GET_HEAP_INFO, CAN_SUBSCRIBE_TO_LOGS, CAN_EXECUTE_COMMAND, CAN_CANCEL_COMMAND, CAN_BIND_TABLE_TO_VARIABLE, CAN_AUTO_COMPLETE,
}
