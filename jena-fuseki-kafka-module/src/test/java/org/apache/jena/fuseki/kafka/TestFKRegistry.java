package org.apache.jena.fuseki.kafka;

import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.sys.JenaSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFKRegistry {

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    @BeforeClass
    public void setup() {
        FKRegistry.get().reset();
    }

    @AfterClass
    public void teardown() {
        FKRegistry.get().reset();
    }

    @Test(expectedExceptions = JenaKafkaException.class, expectedExceptionsMessageRegExp = "Multiple connectors.*")
    public void givenMultipleConnectors_whenRegisteringForSameTopic_thenError() {
        // Given
        KConnectorDesc conn = mock(KConnectorDesc.class);
        List<String> topics = List.of(KafkaTestCluster.DEFAULT_TOPIC);

        // When and Then
        try {
            FKRegistry.get().register(topics, conn);
            Assert.assertEquals(FKRegistry.get().getConnectorDescriptor(KafkaTestCluster.DEFAULT_TOPIC), conn);
            FKRegistry.get().register(topics, conn);
        } finally {
            FKRegistry.get().unregister(topics, conn);
        }
    }

    @Test(expectedExceptions = JenaKafkaException.class, expectedExceptionsMessageRegExp = ".*DLQ topic.*")
    public void givenDlqLoop_whenRegistering_thenError() {
        // Given
        KConnectorDesc conn1 = mock(KConnectorDesc.class);
        when(conn1.getDlqTopic()).thenReturn("b");
        List<String> topics1 = List.of("a");
        KConnectorDesc conn2 = mock(KConnectorDesc.class);
        when(conn2.getDlqTopic()).thenReturn("a");
        List<String> topics2 = List.of("b");

        // When and Then
        FKRegistry.get().register(topics1, conn1);
        FKRegistry.get().register(topics2, conn2);
    }
}
