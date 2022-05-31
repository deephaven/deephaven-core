package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationFieldId;
import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.PathId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.Option;

public class ApplicationField implements HasTicketId, HasPathId {
    @Option(names = {"--app-id"}, required = true, description = "The application id.")
    String applicationId;

    @Option(names = {"--app-field"}, required = true, description = "The field name.")
    String fieldName;

    public ApplicationFieldId applicationFieldId() {
        return new ApplicationFieldId(applicationId, fieldName);
    }

    @Override
    public TicketId ticketId() {
        return applicationFieldId().ticketId();
    }

    @Override
    public PathId pathId() {
        return applicationFieldId().pathId();
    }
}
