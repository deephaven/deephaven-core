/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;

public class DbTimeConverter extends JFrame implements ActionListener, FocusListener {

    private JButton currentTimeButton, fromClipboardButton;
    private JTextField input;
    private JTextField output[];
    private JComboBox timezones[];
    private JTextField outputNanos;
    private JTextField outputMillis;

    private Color defaultColors[] =
            {Color.decode("0xFF8A8A"), Color.decode("0xFFFFAA"), Color.decode("0xC0FF97"), Color.decode("0xCACAFF")};

    public DbTimeConverter() {
        super("DbTime Converter 2011 Clippy Edition");

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        GridBagLayout gridbag = new GridBagLayout();
        GridBagConstraints c = new GridBagConstraints();
        JPanel p = new JPanel(gridbag);
        p.setBorder(BorderFactory.createLineBorder(Color.black));

        currentTimeButton = new JButton("Current");
        currentTimeButton.setFont(new Font("Dialog", Font.BOLD, 11));
        currentTimeButton.setHorizontalAlignment(JLabel.CENTER);
        currentTimeButton.addActionListener(this);

        fromClipboardButton = new JButton("Paste clipboard");
        fromClipboardButton.setFont(new Font("Dialog", Font.BOLD, 11));
        fromClipboardButton.setHorizontalAlignment(JLabel.CENTER);
        fromClipboardButton.addActionListener(this);

        input = new JTextField(getClipboard());

        input.setSelectionStart(0);
        input.setSelectionEnd(input.getText().length());
        input.setPreferredSize(new Dimension(300, 20));
        input.addActionListener(this);
        input.addFocusListener(this);

        c.fill = GridBagConstraints.BOTH;
        c.gridy = 0;
        c.gridx = 0;
        c.weightx = 1.0;
        p.add(input, c);
        c.gridx = 1;
        c.weightx = 0.0;
        p.add(fromClipboardButton, c);
        c.gridx = 2;
        c.weightx = 0.0;
        p.add(currentTimeButton, c);
        c.gridy = 1;

        output = new JTextField[DBTimeZone.values().length];
        timezones = new JComboBox[DBTimeZone.values().length];

        for (int i = 0; i < DBTimeZone.values().length; i++) {
            output[i] = new JTextField();
            output[i].setEditable(false);
            output[i].setBackground(defaultColors[i]);
            output[i].setHorizontalAlignment(SwingConstants.CENTER);
            output[i].setFont(new Font("Arial", Font.BOLD, 12));
            c.gridx = 0;
            c.gridwidth = 1;
            c.weightx = 1.0;
            c.fill = GridBagConstraints.BOTH;
            p.add(output[i], c);

            timezones[i] = new JComboBox(DBTimeZone.values());
            timezones[i].setFont(new Font("Arial", Font.PLAIN, 10));
            timezones[i].setBackground(defaultColors[i % defaultColors.length]);
            timezones[i].setSelectedItem(DBTimeZone.values()[i]);
            timezones[i].addActionListener(this);
            c.gridx = 1;
            c.gridwidth = 2;
            c.weightx = 0.0;
            p.add(timezones[i], c);

            c.gridy++;
        }

        outputNanos = new JTextField();
        outputNanos.setEditable(false);
        outputNanos.setBackground(Color.WHITE);
        outputNanos.setHorizontalAlignment(SwingConstants.CENTER);
        outputNanos.setFont(new Font("Arial", Font.BOLD, 12));
        c.gridx = 0;
        c.gridwidth = 1;
        c.weightx = 1.0;
        c.fill = GridBagConstraints.BOTH;
        p.add(outputNanos, c);

        outputMillis = new JTextField();
        outputMillis.setEditable(false);
        outputMillis.setBackground(Color.WHITE);
        outputMillis.setHorizontalAlignment(SwingConstants.CENTER);
        outputMillis.setFont(new Font("Arial", Font.BOLD, 12));
        c.gridx = 1;
        c.gridwidth = 2;
        c.weightx = 0.0;
        c.fill = GridBagConstraints.BOTH;
        p.add(outputMillis, c);
        c.gridy++;


        getContentPane().add(p);
        pack();
        setLocationRelativeTo(null);
        try {
            actionPerformed(null);
        } catch (Throwable e) {
            // nothin
        }
        setVisible(true);
    }

    private String getClipboard() {
        Clipboard clippy = Toolkit.getDefaultToolkit().getSystemClipboard();
        Transferable xfer = clippy.getContents(this);
        String returnText = "";
        if (xfer.isDataFlavorSupported(DataFlavor.stringFlavor)) {
            // return text content
            try {
                returnText = xfer.getTransferData(DataFlavor.stringFlavor) + " (from clipboard)";
            } catch (Exception e) {
                // oh well
            }
        }
        return returnText;
    }

    public static void main(String[] args) {
        new DbTimeConverter();
    }

    public void actionPerformed(ActionEvent e) {
        if (e != null && e.getSource() == currentTimeButton) {
            input.setText(String.valueOf(DBTimeUtils.millisToTime(System.currentTimeMillis())));
            input.setSelectionStart(0);
            input.setSelectionEnd(input.getText().length());
        }

        if (e != null && e.getSource() == fromClipboardButton) {
            input.setText(getClipboard());
            input.setSelectionStart(0);
            input.setSelectionEnd(input.getText().length());
        }

        DBDateTime time = getTime();

        for (int i = 0; i < output.length; i++) {
            try {
                if (time == null) {
                    output[i].setText("?????");
                } else {
                    output[i].setText(time.toString(DBTimeZone.values()[timezones[i].getSelectedIndex()]));
                }
            } catch (Exception ex) {
                // who cares
            }
        }

        if (time == null) {
            outputNanos.setText("?????");
            outputMillis.setText("?????");
        } else {
            outputNanos.setText(Long.toString(time.getNanos()));
            outputMillis.setText(Long.toString(time.getMillis()));
        }
    }

    private DBDateTime getTime() {
        String s = getText();

        if (s.matches("[0-9]+[ ]*ms")) {
            s = s.substring(0, s.indexOf("m")).trim();

            return DBTimeUtils.millisToTime(Long.parseLong(s));
        }

        if (s.matches("[0-9]+[ ]*ns")) {
            s = s.substring(0, s.indexOf("n")).trim();

            return DBTimeUtils.nanosToTime(Long.parseLong(s));
        }

        try {
            return DBTimeUtils.convertDateTime(getText());
        } catch (RuntimeException ee) {
            return null;
        }
    }

    private String getText() {
        String s = input.getText();
        if (s.indexOf('(') > -1) {
            return s.substring(0, s.indexOf('(')).trim();
        }
        return s.trim();
    }

    public void focusGained(FocusEvent e) {
        if (e.getSource() == input) {
            input.setSelectionStart(0);
            input.setSelectionEnd(input.getText().length());
        }
    }

    public void focusLost(FocusEvent e) {}
}
