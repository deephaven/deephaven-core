package io.deephaven.client.examples;

import io.deephaven.client.examples.ui.Ui;

import javax.swing.*;
import java.awt.*;

public class WeatherDash {
    public static void main(String[] args) {
        // Assign properties that need to be set to even turn on
        System.setProperty("Configuration.rootFile", "grpc-api.prop");
        System.setProperty("io.deephaven.configuration.PropertyInputStreamLoader.override",
                "io.deephaven.configuration.PropertyInputStreamLoaderTraditional");

        JFrame mainFrame = new JFrame("Weather Example Dashboard");

        // Defaults settings (workspace may override the size / location)
        mainFrame.setSize(800, 600);
        mainFrame.setLocationRelativeTo(null);
        mainFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        final Ui theUi = new Ui();

        final JPanel mainPanel = new JPanel(new BorderLayout());
        mainPanel.add(theUi.$$$getRootComponent$$$(), BorderLayout.CENTER);
        mainFrame.getContentPane().add(mainPanel);
        mainFrame.setVisible(true);
    }
}
