package org.apache.jena.fuseki.kafka;

import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

public class TestFKRegistry {

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
            FKRegistry.get().unregister(topics);
        }
    }
}
