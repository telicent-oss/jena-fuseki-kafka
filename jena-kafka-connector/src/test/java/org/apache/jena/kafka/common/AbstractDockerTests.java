package org.apache.jena.kafka.common;

import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.sys.JenaSystem;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.Properties;

@SuppressWarnings("java:S2187")
abstract class AbstractDockerTests {
    protected static final KafkaTestCluster KAFKA = new BasicKafkaTestCluster();

    static {
        JenaSystem.init();
    }

    @BeforeAll
    static void setupKafka() {
        KAFKA.setup();
        KAFKA.resetTestTopic();
    }

    @AfterAll
    static void teardown() {
        KAFKA.teardown();
    }

    protected static KafkaRdfPayloadSource<Bytes> prepareSource(int batchSize, KConnectorDesc conn,
                                                                String consumerGroupBaseId) {
        return KafkaRdfPayloadSource.<Bytes>createRdfPayload()
                                    .bootstrapServers(KAFKA.getBootstrapServers())
                                    .topic(KafkaTestCluster.DEFAULT_TOPIC)
                                    .consumerGroup(consumerGroupBaseId + batchSize)
                                    .consumerConfig(KAFKA.getClientProperties())
                                    .commitOnProcessed()
                                    .maxPollRecords(conn.getMaxPollRecords())
                                    .keyDeserializer(BytesDeserializer.class)
                                    .build();
    }

    protected static KConnectorDesc prepareConnector(Properties props) {
        return new KConnectorDesc(List.of(KafkaTestCluster.DEFAULT_TOPIC), KAFKA.getBootstrapServers(), "/ds", null,
                                  true, false, null, props);
    }

    protected static Properties prepareProperties() {
        Properties props = SysJenaKafka.consumerProperties(KAFKA.getBootstrapServers());
        props.putAll(KAFKA.getClientProperties());
        return props;
    }
}
