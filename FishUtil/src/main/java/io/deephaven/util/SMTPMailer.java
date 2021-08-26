/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.configuration.Configuration;
import org.apache.commons.mail.*;
import org.apache.commons.net.smtp.SMTPClient;
import org.apache.commons.net.smtp.SMTPReply;
import org.apache.commons.net.smtp.SimpleSMTPHeader;

import javax.mail.*;
import javax.mail.internet.*;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.IOException;
import java.io.Writer;
import java.net.*;
import java.util.*;

public class SMTPMailer implements Mailer {

    // Delay resolution of Configuration.getInstance() to avoid classloading circular dependency
    // issues.
    // See IDS-5126 for more details, but essentially:
    // Config -> log4j -> SimpleMailAppender -> SMTPMailer -> Config -> PropertyInputStreamLoaderKV
    // -> etcd read call
    // PropertyInputStreamLoaderKV starts executor, Etcd Executor -> log4j
    private enum Props {
        INSTANCE;

        private final String smtpMxDomain;
        private final boolean sendEmailDisabled;

        Props() {
            smtpMxDomain = Configuration.getInstance().getProperty("smtp.mx.domain");
            sendEmailDisabled =
                Configuration.getInstance().getBooleanWithDefault("smtp.sendEmail.disabled", false);
        }
    }

    private final SMTPClient client;

    public SMTPMailer() {
        client = new SMTPClient() {
            public void connect(String hostname, int port) throws IOException {
                _socket_ = new Socket();
                _socket_.connect(new InetSocketAddress(hostname, port), 1000);
                _socket_.setSoTimeout(1000);
                _connectAction_();
            }
        };
    }

    /**
     *
     * @param list comma separated list of emails
     * @return array of strings representing the same emails
     */
    public static String[] parseEmailList(String list) {
        StringTokenizer myS = new StringTokenizer(list, ", ");
        ArrayList<String> emails = new ArrayList<>();
        while (myS.hasMoreTokens()) {
            emails.add(myS.nextToken());
        }
        return emails.toArray(new String[emails.size()]);
    }

    private String getMXRecord() {
        try {
            // Do a DNS lookup
            DirContext ictx = new InitialDirContext();
            Attributes attributes =
                ictx.getAttributes("dns:/" + Props.INSTANCE.smtpMxDomain, new String[] {"MX"});
            Attribute attribute = attributes.get("MX");

            // Otherwise, return the first MX record we find
            for (NamingEnumeration all = attribute.getAll(); all.hasMore();) {
                String mailhost = (String) all.next();
                mailhost = mailhost.substring(1 + mailhost.indexOf(" "), mailhost.length() - 1);
                // NOTE: DON'T LOG HERE, WE MIGHT ALREADY BE PART OF LOG_MAILER, AND IF THE QUEUE IS
                // FULL WE NEVER ESCAPE!
                return mailhost;
            }
        } catch (Exception e) {
            // If there was no MX record for the domain, use the domain itself
            return Props.INSTANCE.smtpMxDomain;
        }

        return "";
    }

    @Override
    public void sendEmail(String sender, String[] recipients, String subject, String msg)
        throws IOException {
        if (sender == null) {
            String hostname = InetAddress.getLocalHost().getHostName();
            sender = System.getProperty("user.name") + "@" + hostname;
        }

        for (int i = 0; i < recipients.length; i++) {
            sendEmail(sender, recipients[i], subject, msg);
        }
    }

    @Override
    public void sendEmail(String sender, String recipient, String subject, String msg)
        throws IOException {
        sendEmail(sender, recipient, subject, msg, null);
    }

    @Override
    public void sendHTMLEmail(String sender, String recipient, String subject, String msg)
        throws IOException {
        List<Map.Entry<String, String>> extraHeaderEntries = new ArrayList<>();

        extraHeaderEntries.add(new AbstractMap.SimpleEntry<>("Mime-Version", "1.0;"));
        extraHeaderEntries.add(
            new AbstractMap.SimpleEntry<>("Content-Type", "text/html; charset=\"ISO-8859-1\";"));
        extraHeaderEntries.add(new AbstractMap.SimpleEntry<>("Content-Transfer-Encoding", "7bit;"));
        sendEmail(sender, recipient, subject, msg, extraHeaderEntries);
    }

    @Override
    public void sendEmail(String sender, String recipient, String subject, String msg,
        List<Map.Entry<String, String>> extraHeaderEntries) throws IOException {
        if (Props.INSTANCE.sendEmailDisabled) {
            return;
        }

        if (sender == null) {
            String hostname = InetAddress.getLocalHost().getHostName();
            sender = System.getProperty("user.name") + "@" + hostname;
        }

        client.connect(getMXRecord());

        int reply = client.getReplyCode();
        if (!SMTPReply.isPositiveCompletion(reply)) {
            client.disconnect();
            throw new IOException("SMTP server refused connection.");
        }
        client.login();
        client.setSender(sender);

        for (String s : recipient.split(",")) {
            client.addRecipient(s);
        }
        SimpleSMTPHeader header = new SimpleSMTPHeader(sender, recipient, subject);

        if (extraHeaderEntries != null)
            for (Map.Entry<String, String> entry : extraHeaderEntries)
                header.addHeaderField(entry.getKey(), entry.getValue());

        Writer writer = client.sendMessageData();

        if (writer != null) {
            writer.write(header.toString());
            writer.write(msg);
            writer.close();
            client.completePendingCommand();
        }

        client.logout();
        client.disconnect();
    }

    final private static Object lastUpdateLock = new Object();
    private static long lastUpdateTime = 0;

    /**
     * Bug reporter, sends mail but limits the mail to 1 email per second and automatically includes
     * hostname.
     * <P>
     * Note there is no guarantee your email goes through because of the 1/second limit and because
     * this function eats IOExceptions.
     * <P>
     *
     * @param from "from" address, must not contain spaces, e.g. "RiskProfiler"
     * @param to "to" address, e.g. bob@smith.com
     * @param subject email subject line
     * @param message email body
     */
    public static void reportBug(final String from, final String to, final String subject,
        final String message) {
        // return if it's been less than 1 second since the last email
        final long now = System.currentTimeMillis();
        synchronized (lastUpdateLock) {
            if (now < lastUpdateTime + 1000) {
                return;
            } else {
                lastUpdateTime = now;
            }
        }

        Thread currentThread = Thread.currentThread();
        StackTraceElement[] sts = currentThread.getStackTrace();
        String addMessage = "\n===============\n";
        if (sts == null) {
            addMessage = "stack trace was null";
        } else {
            for (StackTraceElement st : sts) {
                addMessage += "\tat " + st + "\n";
            }
        }

        // get hostname
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "Unknown host";
        }

        // send mail
        try {
            new SMTPMailer().sendEmail(from, to, subject,
                "bug report from " + hostname + ":\n\n" + message + addMessage);
        } catch (IOException e) {
            // ignore it, we do not promise to deliver.
        }
    }

    public void sendEmailWithAttachments(String sender, String[] recipients, String subject,
        String msg, String attachmentPaths[]) throws Exception {
        // Create the email message
        MultiPartEmail email = new MultiPartEmail();
        email.setHostName(getMXRecord());
        email.setFrom(sender);
        email.setSubject(subject);
        email.setMsg(msg);

        for (String recipient : recipients) {
            email.addTo(recipient);
        }

        // add the attachment
        for (String attachmentPath : attachmentPaths) {
            EmailAttachment attachment = new EmailAttachment();
            attachment.setPath(attachmentPath);
            attachment.setDisposition(EmailAttachment.ATTACHMENT);

            email.attach(attachment);
        }

        // send the email
        email.send();
    }

    public void sendHTMLEmailWithInline(String sender, String recipients[], String subject,
        String msg, String attachmentPaths[]) throws Exception {
        Properties sessionProperties = System.getProperties();
        sessionProperties.put("mail.smtp.host", getMXRecord());
        Session session = Session.getDefaultInstance(sessionProperties, null);

        Message message = new MimeMessage(session);
        message.setFrom(sender == null ? null : new InternetAddress(sender));
        message.setSubject(subject);

        Address recipientsAddresses[] = new Address[recipients.length];

        for (int i = 0; i < recipients.length; i++) {
            recipientsAddresses[i] = new InternetAddress(recipients[i]);
        }

        message.addRecipients(Message.RecipientType.TO, recipientsAddresses);

        // creates message part
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setContent(msg, "text/html");

        // creates multi-part
        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(messageBodyPart);

        // attach images as MimeBodyParts
        int i = 0;
        for (String attachmentPath : attachmentPaths) {
            i += 1;
            MimeBodyPart imagePart = new MimeBodyPart();
            imagePart.setHeader("Content-ID", "image" + i);
            imagePart.setDisposition(MimeBodyPart.INLINE);
            imagePart.attachFile(attachmentPath);

            multipart.addBodyPart(imagePart);
        }

        message.setContent(multipart);
        Transport.send(message);
    }

    /*
     * public void sendEmail_Authenticated(String user, String password, String sender, String
     * recipient, String subject, String body) throws IOException, javax.mail.MessagingException {
     * String mailer = "zzz"; Transport tr = null; try { Properties props = System.getProperties();
     * props.put("mail.smtp.auth", "true");
     * 
     * // Get a Session object Session mailSession = Session.getDefaultInstance(props, null);
     * 
     * // construct the message Message msg = new MimeMessage(mailSession); msg.setFrom(new
     * InternetAddress(sender));
     * 
     * msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient, false));
     * msg.setSubject(subject);
     * 
     * msg.setText(body); msg.setHeader("X-Mailer", mailer); msg.setSentDate(new Date());
     * 
     * tr = mailSession.getTransport("smtp"); tr.connect(SMTPHOST, user, password);
     * msg.saveChanges(); tr.sendMessage(msg, msg.getAllRecipients()); tr.close(); } catch
     * (Exception e) { e.printStackTrace(); } finally { if (tr != null) tr.close(); } }
     */
}
