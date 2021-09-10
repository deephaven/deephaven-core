package io.deephaven.client.examples.ui;

import com.intellij.uiDesigner.core.GridConstraints;
import com.intellij.uiDesigner.core.GridLayoutManager;
import com.intellij.uiDesigner.core.Spacer;
import io.deephaven.client.examples.BarrageSupport;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactory;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

public class Ui {
    private JTabbedPane tabbedPane1;
    private JPanel panel1;
    private JTextField locationField;
    private JButton addButton;
    private JButton removeButton;
    private JComboBox<String> statCombo;
    private JComboBox<String> valueCombo;
    private JTable summaryTable;
    private JLabel statusLabel;
    private JTextField hostField;
    private JButton connectButton;
    private JCheckBox plaintextCheckBox;
    private FlightSession flightSession;

    // Connection related items both for GRPC and Flight.
    private final ScheduledExecutorService flightScheduler = Executors.newScheduledThreadPool(8);
    private ManagedChannel managedChannel;
    private BarrageSupport support;


    private Set<String> monitoredPlaces = new HashSet<>();

    public Ui() {
        // Start the LTM.  This module is responsible for deterministically handling table updates
        //  In other words,  it drives the 'ticking' of tables.
        LiveTableMonitor.DEFAULT.start();

        connectButton.addActionListener(this::doConnect);
        addButton.addActionListener(this::onAdd);

        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }

    private void onAdd(final ActionEvent ev) {
        // First we'll just ship the request to the server and wait for the response.
        // If we get an Ack (true) We'll stuff this thing in the list.  This is crude on purpose --
        // We don't actually care about having complex logic to track places, only that we can submit them.
        final String place = locationField.getText();
        if (place == null || place.isEmpty()) {
            return;
        }

        try (final ConsoleSession console = flightSession.session().console("python").get()) {
            final Changes changes = console.executeCode("beginWatch(" + place + ")");

            // TODO: Something
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * Connect to the Deephaven Community Core server via Arrow FLight and fetch the relevant tables for this example.
     *
     * @param ev
     */
    private void doConnect(final ActionEvent ev) {
        // This bit is an oddity of the Swing UI implementation -- we just ensure we are not
        // executing potentially blocking I/O on the thread that handles UI management & drawing.
        if (SwingUtilities.isEventDispatchThread()) {
            flightScheduler.schedule(() -> doConnect(ev), 0, TimeUnit.SECONDS);
            return;
        }

        // Do we have everything we need to know to connect?  Host / port?
        final String host = hostField.getText();
        if (host == null || host.isEmpty()) {
            return;
        }

        // Close any pre-existing session and wait for it to terminate
        if (flightSession != null) {
            try {
                support.close();
                flightSession.close();
                managedChannel.shutdownNow();
                managedChannel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // No error handling for now.
                e.printStackTrace();
                return;
            }
        }

        boolean plaintext = plaintextCheckBox.isSelected();

        // Create the new connection
        final ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(host);
        if (plaintext || "localhost:10000".equals(host)) {
            channelBuilder.usePlaintext();
        } else {
            channelBuilder.useTransportSecurity();
        }

        managedChannel = channelBuilder.build();

        final BufferAllocator bufferAllocator = new RootAllocator();
        final FlightSessionFactory flightSessionFactory =
                DaggerDeephavenFlightRoot.create().factoryBuilder()
                        .managedChannel(managedChannel)
                        .scheduler(flightScheduler)
                        .allocator(bufferAllocator)
                        .build();

        flightSession = flightSessionFactory.newFlightSession();
        support = new BarrageSupport(managedChannel, flightSession);
        tryFetchTables();
    }

    /**
     * Fetch each of the tables that this example cares about.
     */
    private void tryFetchTables() {
        BarrageTable awesomeCool = support.fetchSubscribedTable("s/LastByCityState");

        final Object[] rec = awesomeCool.getRecord(0);
        System.out.println("WOOHOO " + Arrays.toString(rec));
    }

    /**
     * When the application shuts down, ensure that we clean up our connections
     * and shutdown any running threads.
     */
    private void onShutdown() {
        flightScheduler.shutdownNow();
        managedChannel.shutdownNow();
        try {
            if (!flightScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Scheduler not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (!managedChannel.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Channel not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    {
// GUI initializer generated by IntelliJ IDEA GUI Designer
// >>> IMPORTANT!! <<<
// DO NOT EDIT OR ADD ANY CODE HERE!
        $$$setupUI$$$();
    }

    /**
     * Method generated by IntelliJ IDEA GUI Designer
     * >>> IMPORTANT!! <<<
     * DO NOT edit this method OR call it in your code!
     *
     * @noinspection ALL
     */
    private void $$$setupUI$$$() {
        panel1 = new JPanel();
        panel1.setLayout(new GridLayoutManager(2, 1, new Insets(0, 0, 0, 0), -1, -1));
        tabbedPane1 = new JTabbedPane();
        tabbedPane1.setTabLayoutPolicy(1);
        tabbedPane1.setTabPlacement(1);
        panel1.add(tabbedPane1, new GridConstraints(0, 0, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_BOTH, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, null, new Dimension(200, 200), null, 0, false));
        final JPanel panel2 = new JPanel();
        panel2.setLayout(new GridLayoutManager(4, 4, new Insets(0, 0, 0, 0), -1, -1));
        tabbedPane1.addTab("Main", panel2);
        locationField = new JTextField();
        panel2.add(locationField, new GridConstraints(1, 1, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_WANT_GROW, GridConstraints.SIZEPOLICY_FIXED, null, new Dimension(150, -1), null, 0, false));
        addButton = new JButton();
        addButton.setText("Add");
        panel2.add(addButton, new GridConstraints(1, 2, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        removeButton = new JButton();
        removeButton.setText("Remove");
        panel2.add(removeButton, new GridConstraints(1, 3, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final JLabel label1 = new JLabel();
        label1.setText("Location");
        panel2.add(label1, new GridConstraints(1, 0, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_NONE, GridConstraints.SIZEPOLICY_FIXED, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final JPanel panel3 = new JPanel();
        panel3.setLayout(new GridLayoutManager(1, 1, new Insets(0, 0, 0, 0), -1, -1));
        panel2.add(panel3, new GridConstraints(3, 0, 1, 4, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_BOTH, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, null, null, null, 0, false));
        final JScrollPane scrollPane1 = new JScrollPane();
        panel3.add(scrollPane1, new GridConstraints(0, 0, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_BOTH, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_WANT_GROW, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_WANT_GROW, null, null, null, 0, false));
        summaryTable = new JTable();
        scrollPane1.setViewportView(summaryTable);
        final JPanel panel4 = new JPanel();
        panel4.setLayout(new GridLayoutManager(1, 2, new Insets(0, 0, 0, 0), -1, -1));
        panel2.add(panel4, new GridConstraints(2, 1, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_BOTH, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, null, null, null, 0, false));
        statCombo = new JComboBox();
        final DefaultComboBoxModel defaultComboBoxModel1 = new DefaultComboBoxModel();
        defaultComboBoxModel1.addElement("Min");
        defaultComboBoxModel1.addElement("Max");
        defaultComboBoxModel1.addElement("Average");
        statCombo.setModel(defaultComboBoxModel1);
        panel4.add(statCombo, new GridConstraints(0, 0, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        valueCombo = new JComboBox();
        final DefaultComboBoxModel defaultComboBoxModel2 = new DefaultComboBoxModel();
        defaultComboBoxModel2.addElement("Temperature");
        defaultComboBoxModel2.addElement("Humidity");
        valueCombo.setModel(defaultComboBoxModel2);
        panel4.add(valueCombo, new GridConstraints(0, 1, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final JLabel label2 = new JLabel();
        label2.setText("Display Statistic");
        panel2.add(label2, new GridConstraints(2, 0, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_NONE, GridConstraints.SIZEPOLICY_FIXED, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final JPanel panel5 = new JPanel();
        panel5.setLayout(new GridLayoutManager(1, 3, new Insets(0, 0, 0, 0), -1, -1));
        panel2.add(panel5, new GridConstraints(0, 0, 1, 4, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_BOTH, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, GridConstraints.SIZEPOLICY_CAN_SHRINK | GridConstraints.SIZEPOLICY_CAN_GROW, null, null, null, 0, false));
        final JLabel label3 = new JLabel();
        label3.setText("Status:");
        panel5.add(label3, new GridConstraints(0, 0, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_NONE, GridConstraints.SIZEPOLICY_FIXED, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final Spacer spacer1 = new Spacer();
        panel5.add(spacer1, new GridConstraints(0, 2, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_WANT_GROW, 1, null, null, null, 0, false));
        statusLabel = new JLabel();
        statusLabel.setText("Label");
        panel5.add(statusLabel, new GridConstraints(0, 1, 1, 1, GridConstraints.ANCHOR_WEST, GridConstraints.FILL_NONE, GridConstraints.SIZEPOLICY_FIXED, GridConstraints.SIZEPOLICY_FIXED, null, null, null, 0, false));
        final JToolBar toolBar1 = new JToolBar();
        toolBar1.setFloatable(false);
        panel1.add(toolBar1, new GridConstraints(1, 0, 1, 1, GridConstraints.ANCHOR_CENTER, GridConstraints.FILL_HORIZONTAL, GridConstraints.SIZEPOLICY_WANT_GROW, GridConstraints.SIZEPOLICY_FIXED, null, new Dimension(-1, 20), null, 0, false));
        final JLabel label4 = new JLabel();
        label4.setText("Host");
        toolBar1.add(label4);
        hostField = new JTextField();
        hostField.setText("localhost:10000");
        toolBar1.add(hostField);
        plaintextCheckBox = new JCheckBox();
        plaintextCheckBox.setText("Plaintext?");
        toolBar1.add(plaintextCheckBox);
        connectButton = new JButton();
        connectButton.setText("Connect");
        toolBar1.add(connectButton);
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return panel1;
    }

}
