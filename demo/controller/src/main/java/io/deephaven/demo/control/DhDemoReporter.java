package io.deephaven.demo.control;

import com.google.common.base.Strings;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiTextResponse;
import com.slack.api.methods.request.auth.AuthTestRequest;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.users.UsersConversationsRequest;
import com.slack.api.methods.response.auth.AuthTestResponse;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.users.UsersConversationsResponse;
import com.slack.api.model.Conversation;
import com.slack.api.model.ConversationType;
import io.deephaven.demo.manager.Execute;
import io.deephaven.demo.manager.NameConstants;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * DhDemoReporter:
 * <p>
 * <p>This class is responsible for collecting and reporting information about the running demo system.
 */
public class DhDemoReporter {

    private static final List<ConversationType> CHANNEL_TYPES = Arrays.asList(ConversationType.PRIVATE_CHANNEL, ConversationType.PUBLIC_CHANNEL);

    private static final Logger LOG = Logger.getLogger(DhDemoReporter.class);
    private static final String PREFIX_REPORT_VAR = "DH_REPORT_CHANNEL_";
    private static final String PREFIX_ERROR_VAR = "DH_ERROR_CHANNEL_";
    private static final float MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final float MILLIS_PER_SECOND = 1000;

    private final DemoStats stats;
    private final String slackToken;
    private final boolean setup;

    private final Set<String> reportChannels = new HashSet<>();
    private final Set<String> errorChannels = new HashSet<>();
    private final String appId;

    private final Supplier<Boolean> shouldSlack;

    private String generateReport(final boolean useMarkdown) {
        final StringBuilder b = new StringBuilder();
        final Collection<String> users = stats.getUniqueUsers();
        final Collection<String> errors = stats.getErrorUsage();
        final int peakUsage = stats.getPeakUsage();
        final float avgUsage = stats.getAverageUsage();
        final int peakMachines = stats.getPeakMachines();
        final int avgMachines = stats.getAverageMachines();

        final long startTime = stats.getStartTime();
        final long doneTime = System.currentTimeMillis();
        final long runTime = doneTime - startTime;
        final float hours = runTime / MILLIS_PER_HOUR;
        final String fmtedHours = shortenFloat(hours);
        String hostname;
        try {
            hostname = Execute.executeQuiet("hostname").out;
        } catch (Exception e) {
            hostname = "unknown";
        }
        b.append("Greetings, esteemed Deephaven users!")
                .append("\n")
                .append("\n")
                .append("In the last ").append("0".equals(fmtedHours) ?
                        shortenFloat(runTime / MILLIS_PER_SECOND) + " seconds" : fmtedHours + " hours")
                .append(", machine ").append(hostname).append(" (")

                .append(NameConstants.DOMAIN).append(") received ").append(users.size()).append(" unique visitors")
                .append(users.isEmpty() ? "." : ":\n * ").append(String.join("\n * ", users))
                .append("\n\n")
                .append("`Peak leased machines:   \t").append(peakUsage).append("`\n")
                .append("`Average leased machines:\t").append(shortenFloat(avgUsage)).append("`\n")
                .append("\nThe site also recorded ").append(errors.size()).append( " unique errors")
                .append(errors.isEmpty() ? "." : ":\n * ").append(String.join("\n * ", errors))
                .append("\n");

        return b.toString();
    }

    private static String shortenFloat(final float value) {
        if (value < 0.01) {
            return "0";
        }
        return Float.toString(value).replaceFirst("(\\d+[.]\\d{1,2}).*", "$1");
    }

    public DhDemoReporter(Supplier<Boolean> shouldSlack) {
        this(shouldSlack, getConfiguredSlackToken());
    }

    public DhDemoReporter(final Supplier<Boolean> shouldSlack, final String slackToken) {
        this.stats = new DemoStats();
        this.shouldSlack = shouldSlack;
        this.slackToken = slackToken;
        boolean success;
        String appId;
        if (slackToken == null) {
            this.setup = false;
            this.appId = "unknown";
            return;
        }
        try(final Slack slack = Slack.getInstance()) {

            final MethodsClient mthds = slack.methods(slackToken);
            final AuthTestResponse selfInfo = mthds.authTest(AuthTestRequest.builder()
                    .token(slackToken)
                    .build());
            debugResult(selfInfo);
            appId = selfInfo.getAppId();

            String slackChannel = System.getenv("DH_SLACK_CHANNEL");
            if (Strings.isNullOrEmpty(slackChannel)) {
                LOG.warn("No DH_SLACK_CHANNEL set, attempting to search for channel to join");
                final UsersConversationsResponse botConvos = mthds.usersConversations(UsersConversationsRequest.builder()
                        .token(slackToken)
                        .user(selfInfo.getUserId())
                        .types(CHANNEL_TYPES)
                        .build());
                debugResult(botConvos);
                if (botConvos.isOk()) {
                    for (Conversation channel : botConvos.getChannels()) {
                        // Be default, all channels that have added our slack app will get reports and errors
                        final String channelUnderscore = channel.getName().replace('-', '_');
                        String key = PREFIX_REPORT_VAR + channelUnderscore;
                        if (!"false".equals(System.getenv(key))) {
                            // setting DH_REPORT_CHANNEL_my_underscore_channel_name=false will disable reports for the given channel
                            LOG.infof("Sending reports to %s because %s!=false", channel.getName(), key);
                            reportChannels.add(channel.getName());
                        }
                        key = PREFIX_ERROR_VAR + channelUnderscore;
                        if (!"false".equals(System.getenv(key))) {
                            LOG.infof("Sending errors to %s because %s!=false", channel.getName(), key);
                            // setting DH_ERROR_CHANNEL_my_underscore_channel_name=false will disable errors for the given channel
                            errorChannels.add(channel.getName());
                        }
                    }
                } else {
                    LOG.error("Could not list slack channels");
                }
            } else {
                // The user supplied a single slack channel. Just use it.
                reportChannels.add(slackChannel);
                errorChannels.add(slackChannel);
            }
            // Allow adding target slack channels using env vars; DH_REPORT_CHANNEL_my_channel=my-channel enables logging to my-channel
            // If your channel name uses underscores, you can use DH_REPORT_CHANNEL_my_channel=true to add my_channel
            // Use DH_REPORT_CHANNEL_my_channel=false will prevent channels named my_channel or my-channel from receiving reports.
            // Errors use the same semantics, except with a variable prefix of DH_ERROR_CHANNEL
            for (Map.Entry<String, String> e : System.getenv().entrySet()) {
                if ("false".equals(e.getValue())) {
                    continue;
                }
                String name = e.getKey();
                boolean isReport = name.startsWith(PREFIX_REPORT_VAR);
                boolean isError = name.startsWith(PREFIX_ERROR_VAR);
                if (isReport || isError) {
                    if ("true".equals(e.getValue())) {
                        name = e.getKey().replace(isReport ? PREFIX_REPORT_VAR : PREFIX_ERROR_VAR, "");
                    } else {
                        name = e.getValue();
                    }
                    (isReport ? reportChannels : errorChannels).add(name);
                }
            }

            success = true;
        } catch (Exception e) {
            LOG.error("Unable to setup slack integration, continuing without slack.", e);
            success = false;
            appId = "unknown";
        }
        this.setup = success;
        this.appId = appId;
    }

    private static String getConfiguredSlackToken() {
        String slackToken = System.getenv("DH_SLACK_TOKEN");
        if (slackToken == null) {
            String msg = "No DH_SLACK_TOKEN set; cannot send any slack messages";
            LOG.error(new IllegalStateException(msg));
        }
        return slackToken;
    }

    public void reportToSlack(final boolean sendToSlack) {
        String report = generateReport(true);
        if (setup) {
            LOG.info("Sending report over slack:");
            LOG.info(report);
            if (!sendToSlack) {
                return;
            }
            String channelId = null;
            try(final Slack slack = Slack.getInstance()) {
                for (String reportChannel : reportChannels) {
                    channelId = reportChannel;
                    final ChatPostMessageResponse result = slack.methods(slackToken).chatPostMessage(ChatPostMessageRequest.builder()
                            .token(slackToken)
                            .channel(reportChannel)
                            .text(report)
                            .build());
                    debugResult(result);
                    if (!result.isOk()) {
                        LOG.errorf("UNABLE TO SEND REPORT TO SLACK CHANNEL %s", reportChannel);
                    }
                }

            } catch (Exception e) {
                LOG.errorf(e, "Unable to send chat to slack; channel=%s", channelId);
            }
        } else {
            LOG.error("Slack is not setup; dumping slack report to stdout:");
            LOG.info(report);
        }
    }

    private void debugResult(final SlackApiTextResponse result) {
        if (!Strings.isNullOrEmpty(result.getError())) {
            LOG.errorf("ERROR: %s", result.getError());
        }
        if (!Strings.isNullOrEmpty(result.getWarning())) {
            LOG.warnf("WARNING: %s", result.getWarning());
        }
        if (!Strings.isNullOrEmpty(result.getNeeded())) {
            LOG.errorf("Your slack app, %s, requires additional permissions: %s", appId, result.getNeeded());
        }
    }

    public static void main(String ... args) throws Exception {
        final DhDemoReporter reporter = new DhDemoReporter(()->false);
        reporter.recordUsage(1, 3);
        reporter.recordVisitor("test1");
        reporter.recordVisitor("test2");
        reporter.recordVisitor("test1");
        Thread.sleep(200);
        reporter.recordUsage(3, 5);
        Thread.sleep(300);
        reporter.recordUsage(6, 7);
        reporter.recordError("bad1", null);
        reporter.recordError("bad2", null);
        reporter.recordError("bad1", null);
        reporter.recordError("bad1", null);
        reporter.recordError("bad1", null);
        reporter.recordError("bad1", null);
        reporter.recordError("bad1", null);
        Thread.sleep(800);
        reporter.recordUsage(3, 4);
        Thread.sleep(400);

        reporter.reportToSlack(false);
    }

    public void recordUsage(final int usage, final int machines) {
        stats.recordUsedLeases(usage);
    }

    public void recordError(String error, Throwable t) {
        if (t == null) {
            LOG.error(error);
        } else {
            LOG.error(error, t);
            error = error + "\n" + dump("", t);
        }
        if (stats.recordError(error)) {
            if (shouldSlack.get() && setup) {
                String channelId = null;
                String errorPrefix = System.getenv("DH_ERROR_PREFIX");
                if (errorPrefix == null) {
                    errorPrefix = "";
                } else if (!errorPrefix.endsWith("\n")) {
                    errorPrefix += "\n";
                }
                try(final Slack slack = Slack.getInstance()) {
                    for (String errorChannel : errorChannels) {
                        channelId = errorChannel;
                        final ChatPostMessageResponse result = slack.methods(slackToken).chatPostMessage(ChatPostMessageRequest.builder()
                                .token(slackToken)
                                .channel(errorChannel)
                                .text(errorPrefix + error)
                                .build());
                        debugResult(result);
                        if (!result.isOk()) {
                            LOG.errorf("UNABLE TO SEND ERROR TO SLACK CHANNEL %s", errorChannel);
                        }
                    }

                } catch (Exception e) {
                    LOG.errorf(e, "Unable to send chat to slack; channel=%s", channelId);
                }
            }
        }
    }

    private String dump(String prefix, final Throwable t) {
        StringBuilder msg = new StringBuilder(prefix);
        msg.append(t.getMessage())
                .append(" (")
                .append(t.getClass().getName())
                .append(")");
        for (StackTraceElement stackTraceElement : t.getStackTrace()) {
            msg.append("\n ").append(prefix).append("- ").append(stackTraceElement.toString());
        }
        if (t.getCause() != null) {
            msg.append(prefix).append(" caused by:\n").append(dump(prefix + "  ", t.getCause()));
        }
        if (t.getSuppressed() != null) {
            for (Throwable suppressed : t.getSuppressed()) {
                msg.append(prefix).append(" suppressed:\n").append(dump(prefix + "  ", suppressed));
            }
        }

        return msg.toString();
    }

    public void recordVisitor(final String visitor) {
        stats.recordVisit(visitor);
    }
}
