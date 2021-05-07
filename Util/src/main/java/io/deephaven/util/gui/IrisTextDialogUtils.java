/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.gui;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.ExceptionDetails;
import org.jetbrains.annotations.NotNull;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

/**
 * Text area dialog with the Iris icon.
 */
public class IrisTextDialogUtils {

    private static final Image defaultIcon;
    private static final String IRIS_ICON_PROPERTY = "IrisConsole.icon";
    static {
        Image tmpIcon = null;
        try {
            final String irisIconResource = Configuration.getInstance().getProperty(IRIS_ICON_PROPERTY);
            tmpIcon = new ImageIcon(IrisTextDialogUtils.class.getResource(irisIconResource)).getImage();
        } catch (Exception ignored) {
            // if we have an exception, we'll skip setting our icon
        } finally {
            defaultIcon = tmpIcon;
        }
    }

    private static Image getDefaultDialogIcon() {
        return defaultIcon;
    }

    /**
     * Creates a dialog displaying the full stack trace of the {@link Throwable}.
     *
     * @param parent parent of the dialog
     * @param title title of the dialog
     * @param throwable error message. Must not be null
     */
    public static void showErrorDialog(Component parent, final String title, @NotNull final Throwable throwable) {
        showTextAreaDialog(parent, title, new ExceptionDetails(throwable).getFullStackTrace());
    }

    /**
     * Creates a dialog displaying the message.
     *
     * @param parent parent of the dialog
     * @param title title of the dialog
     * @param value message to display
     */
    public static void showTextAreaDialog(Component parent, final String title, final String value) {
        final JFrame dialog = new JFrame(title);
        Image icon = getDefaultDialogIcon();
        if (icon != null) {
            dialog.setIconImage(icon);
        }
        final JPanel newPanel = new JPanel();
        newPanel.setLayout(new BoxLayout(newPanel, BoxLayout.Y_AXIS));

        JScrollPane scrollPane = getScrollableTextArea(value);
        newPanel.add(scrollPane);
        JButton okButton = new JButton(new AbstractAction("OK") {
            @Override
            public void actionPerformed(ActionEvent e1) {
                dialog.dispose();
            }
        });
        okButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        newPanel.add(okButton);
        newPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        dialog.setSize(800, 600);
        dialog.add(newPanel);
        dialog.setLocationRelativeTo(parent);

        dialog.getRootPane().registerKeyboardAction(e -> dialog.dispose(), KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
        dialog.getRootPane().registerKeyboardAction(e -> dialog.dispose(), KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);

        dialog.setVisible(true);
    }

    @NotNull
    public static JScrollPane getScrollableTextArea(String value) {
        final JTextArea textArea = new JTextArea(value);
        textArea.setEditable(false);

        JScrollPane scrollPane = new JScrollPane(textArea){
            @Override
            public Dimension getPreferredSize() {
                return new Dimension(580, 320);
            }
        };

        textArea.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isRightMouseButton(e)) {
                    JPopupMenu popupMenu = new JPopupMenu();
                    popupMenu.add(new JMenuItem(new AbstractAction("Copy") {
                        @Override
                        public void actionPerformed(ActionEvent e) {
                            final String selectedText = textArea.getSelectedText();

                            final StringSelection data;

                            if (selectedText == null || selectedText.isEmpty()) {
                                // if no selection, we should copy the entire value, otherwise copy the selection
                                data = new StringSelection(value);
                            } else {
                                data = new StringSelection(selectedText);
                            }

                            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(data, data);
                        }
                    }));
                    popupMenu.show(textArea, e.getX(), e.getY());
                }
            }
        });
        return scrollPane;
    }
}
