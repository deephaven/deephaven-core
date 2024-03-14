//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.junit;

import com.google.gwt.core.ext.TreeLogger;
import com.google.gwt.core.ext.UnableToCompleteException;
import com.google.gwt.junit.JUnitShell;
import com.google.gwt.junit.RunStyle;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RunStyle implementation to delegate to Selenium RemoteWebDriver implementations. Simplified version of implementation
 * found in <a href="https://github.com/gwtproject/gwt-core">gwt-core</a>.
 */
public class RunStyleRemoteWebDriver extends RunStyle {
    private final List<RemoteWebDriver> browsers = new ArrayList<>();
    private final Thread keepalive;

    public RunStyleRemoteWebDriver(JUnitShell shell) {
        super(shell);

        keepalive = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
                for (RemoteWebDriver browser : browsers) {
                    // As in RunStyleSelenium, simple way to poll the browser and ensure it is still alive, even if
                    // not actively being used at the moment.
                    browser.getTitle();
                }
            }
        });
        keepalive.setDaemon(true);
    }


    @Override
    public final int initialize(String args) {
        URL remoteWebDriverUrl;
        if (args == null || args.isEmpty()) {
            getLogger().log(TreeLogger.ERROR,
                    "RemoteWebDriver runstyle requires a parameter of the form protocol://hostname:port?browser1[,browser2]");
            return -1;
        }
        String[] parts = args.split("\\?");
        String url = parts[0];
        try {
            remoteWebDriverUrl = new URL(url);
            if (remoteWebDriverUrl.getPath().isEmpty()
                    || (remoteWebDriverUrl.getPath().equals("/") && !url.endsWith("/"))) {
                getLogger().log(TreeLogger.INFO, "No path specified in webdriver remote url, using default of /wd/hub");
                remoteWebDriverUrl = new URL(url + "/wd/hub");
            }
        } catch (MalformedURLException e1) {
            getLogger().log(TreeLogger.ERROR, e1.getMessage(), e1);
            return -1;
        }

        String[] browserNames = parts[1].split(",");
        for (String browserName : browserNames) {
            DesiredCapabilities capabilities = new DesiredCapabilities();
            capabilities.setBrowserName(browserName);

            try {
                RemoteWebDriver wd = new RemoteWebDriver(remoteWebDriverUrl, capabilities);
                browsers.add(wd);
            } catch (Exception exception) {
                getLogger().log(TreeLogger.ERROR, "Failed to find desired browser", exception);
                return -1;
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            keepalive.interrupt();
            for (RemoteWebDriver browser : browsers) {
                try {
                    browser.close();
                } catch (Exception ignored) {
                    // ignore, we're shutting down, continue shutting down others
                }
            }
        }));
        return browsers.size();
    }

    @Override
    public void launchModule(String moduleName) throws UnableToCompleteException {
        // Since WebDriver.get is blocking, start the keepalive thread first
        keepalive.start();

        // Starts each browser to run the tests at the url specified by JUnit+GWT.
        for (RemoteWebDriver browser : browsers) {
            browser.get(shell.getModuleUrl(moduleName));
        }
    }

    /**
     * <a href="https://groups.google.com/d/msg/google-web-toolkit/jLGhwUrKVRY/eQaDO6EUqdYJ">Workaround</a> until GWT's
     * JUnitShell handles IPv6 addresses correctly.
     */
    public String getLocalHostName() {
        String host = System.getProperty("webdriver.test.host");
        if (host != null) {
            return host;
        }
        InetAddress a;
        try {
            a = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to determine my ip address", e);
        }
        if (a instanceof Inet6Address) {
            return "[" + a.getHostAddress() + "]";
        } else {
            return a.getHostAddress();
        }
    }
}
