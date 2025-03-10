package org.apache.jena.fuseki.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.sys.JenaSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;

public class TestConfig {

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    @Test
    public void givenConfigurationWithNonUniqueGroupId_whenStartingFusekiKafka_thenFails() {
        verifyBadConfiguration("src/test/files/bad-config-shared-group-id.ttl", true, "MUST have a unique");
    }

    @Test
    public void givenConfigurationWithNoTopic_whenStartingFusekiKafka_thenFails() {
        verifyBadConfiguration("src/test/files/bad-config-no-topic.ttl", false, "Failed to build a connector");
    }

    @Test
    public void givenConfigurationWithNoStateFile_whenStartingFusekiKafka_thenFails() {
        verifyBadConfiguration("src/test/files/bad-config-no-state-file.ttl", false, "Failed to build a connector");
    }

    @Test
    public void givenConfigurationWithNoConnectors_whenStartingFusekiKafka_thenSuccess_andNoConnectorsRegistered() {
        // Given
        String configFile = "src/test/files/no-connectors.ttl";

        // When
        FusekiServer server = FusekiServer.create()
                                          .port(0)
                                          .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                          .parseConfigFile(configFile)
                                          .build();
        try {
            server.start();

            // Then
            Assert.assertTrue(server.getJettyServer().isRunning());

            // And
            Assert.assertTrue(FKRegistry.get().getConnectors().isEmpty());
        } finally {
            server.stop();
        }
    }

    private static void verifyBadConfiguration(String configFile, boolean unwrapCause, String expectedErrorMessage) {
        // Given and When
        AtomicReference<FusekiServer> server = new AtomicReference<>();
        try {
            server.set(FusekiServer.create()
                                   .port(0)
                                   .fusekiModules(FusekiModules.create(new FMod_FusekiKafka()))
                                   .parseConfigFile(configFile)
                                   .build());
            server.get().start();
            Assert.fail("Did not throw an exception as expected");
        } catch (Throwable e) {
            // Then
            if (unwrapCause) {
                Throwable cause = e.getCause();
                Assert.assertNotNull(cause, "Cause should not be null");
                Assert.assertTrue(StringUtils.contains(cause.getMessage(), expectedErrorMessage),
                                  "Error message missing '" + expectedErrorMessage + "' - got '" + cause.getMessage() + "'");
            } else {
                Assert.assertTrue(StringUtils.contains(e.getMessage(), expectedErrorMessage),
                                  "Error message missing '" + expectedErrorMessage + "' - got '" + e.getMessage() + "'");
            }
        } finally {
            if (server.get() != null) {
                server.get().stop();
            }
        }
    }
}
