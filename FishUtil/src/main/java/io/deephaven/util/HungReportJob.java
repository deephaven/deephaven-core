/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.TimedJob;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class HungReportJob extends TimedJob {

    private final Logger log;
    private final String errorEmailRecipientAddress;
    private final String monitoredJobName;
    private final String actionMessage;

    public HungReportJob(@NotNull final Logger log,
            @NotNull final String errorEmailRecipientAddress,
            @NotNull final String monitoredJobName,
            @NotNull final String actionMessage) {
        this.log = log;
        this.errorEmailRecipientAddress = errorEmailRecipientAddress;
        this.monitoredJobName = monitoredJobName;
        this.actionMessage = actionMessage;
    }

    @Override
    public void timedOut() {
        try {
            new SMTPMailer().sendEmail(null, errorEmailRecipientAddress,
                    "[ERROR] " + monitoredJobName + " may be hung!", actionMessage);
        } catch (IOException e) {
            log.warn().append("HungReportJob: Failed to send delay report email for monitored job ")
                    .append(monitoredJobName).append(": ").append(e).endl();
        }
    }
}
