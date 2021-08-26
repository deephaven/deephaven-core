package io.deephaven.utils.test;

import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.PropertyException;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class PropertySaverTest {

    // Add this to IntelliJ Unit Test Run Configuration to mimic gradle test
    //
    // -ea -DConfiguration.rootFile=dh-tests.prop -Dworkspace=./tmp/workspace -Ddevroot=.
    // -Dlog4j.configuration=log4j.teamcity.xml
    // -ea is for JVM to honor assertions
    //
    // CommandLine:
    // ./gradlew :Util:test --tests io.deephaven.utils.test.PropertySaverTest
    // open Util/build/reports/tests/test/index.html

    private final Logger log = new StreamLoggerImpl(System.out, LogLevel.DEBUG);

    @Test
    public void propertySaverRemoveSomeProp() throws Exception {
        Configuration configuration = Configuration.getInstance();
        final int initialSize = configuration.getProperties().size();
        log.info().append("configuration initialSize: " + initialSize).endl();
        Assert.assertTrue(initialSize > 0);

        final PropertySaver propertySaver = new PropertySaver();
        try {
            final String someProperty = configuration.getProperties().stringPropertyNames().iterator().next();
            log.info().append("Remove someProperty: " + someProperty).endl();
            propertySaver.remove(someProperty);
            log.info().append("configuration currentSize: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(configuration.getProperties().size(), initialSize - 1);
        } finally {
            propertySaver.restore();
            log.info().append("configuration restored size: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(initialSize, configuration.getProperties().size());
        }
    }

    @Test
    public void propertySaverRemoveAllProps() throws Exception {
        Configuration configuration = Configuration.getInstance();
        final int initialSize = configuration.getProperties().size();
        log.info().append("configuration initialSize: " + initialSize).endl();
        Assert.assertTrue(initialSize > 0);

        final PropertySaver propertySaver = new PropertySaver();
        try {
            Set<String> props = configuration.getProperties().stringPropertyNames();
            props.forEach((k) -> propertySaver.remove(k));
            log.info().append("configuration currentSize: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(configuration.getProperties().size(), 0);
        } finally {
            propertySaver.restore();
            log.info().append("configuration restored size: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(initialSize, configuration.getProperties().size());
        }
    }

    @Test
    public void propertySaverRemoveNonExistentProp() throws Exception {
        Configuration configuration = Configuration.getInstance();
        final int initialSize = configuration.getProperties().size();
        log.info().append("configuration initialSize: " + initialSize).endl();
        Assert.assertTrue(initialSize > 0);

        final PropertySaver propertySaver = new PropertySaver();
        try {
            final String randomProperty = UUID.randomUUID().toString();
            try {
                // trying to get a non-existent property throws a PropertyException
                configuration.getProperty(randomProperty);
            } catch (Throwable th) {
                assertThat(th, instanceOf(PropertyException.class));
                log.info().append("====> Got expected exception: " + th).endl();
            }
            log.info().append("Remove random (non-existing) Property: " + randomProperty).endl();
            propertySaver.remove(randomProperty);
            log.info().append("configuration currentSize: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(configuration.getProperties().size(), initialSize);
        } finally {
            propertySaver.restore();
            log.info().append("configuration restored size: " + configuration.getProperties().size()).endl();
            Assert.assertEquals(initialSize, configuration.getProperties().size());
        }
    }
}
