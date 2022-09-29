package io.deephaven.server.partitionedtable;

import io.deephaven.auth.Privilege;

public enum PartitionedTableServicePrivilege implements Privilege {
    CAN_PARTITION_BY, CAN_MERGE, CAN_GET_TABLE,
}
