package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.kafka.BasicKafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource;
import io.telicent.smart.cache.sources.kafka.KafkaTestCluster;
import io.telicent.smart.cache.sources.kafka.serializers.RdfPayloadSerializer;
import io.telicent.smart.cache.sources.kafka.sinks.KafkaSink;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.Txn;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class AbstractDockerTests {
    static {
        JenaSystem.init();
    }

    private static final int TEST_SIZE = 10_000;
    private static final KafkaTestCluster KAFKA = new BasicKafkaTestCluster();
    private static final Random RANDOM = new Random();
    private static int GENERATED = 0;

    @BeforeAll
    public static void setup() throws InterruptedException {
        KAFKA.setup();
        KAFKA.resetTestTopic();

        // Generate test events
        List<Node> predicates = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            predicates.add(NodeFactory.createURI("https://example.com/predicate/" + i));
        }
        try (KafkaSink<Bytes, RdfPayload> sink = KafkaSink.<Bytes, RdfPayload>create()
                                                          .bootstrapServers(KAFKA.getBootstrapServers())
                                                          .producerConfig(KAFKA.getClientProperties())
                                                          .topic(KafkaTestCluster.DEFAULT_TOPIC)
                                                          .keySerializer(BytesSerializer.class)
                                                          .valueSerializer(RdfPayloadSerializer.class)
                                                          .build()) {
            Node predicate = predicates.get(RANDOM.nextInt(predicates.size()));
            for (int i = 0; i < TEST_SIZE; i++) {
                DatasetGraph dsg = DatasetGraphFactory.create();
                int graphSize = RANDOM.nextInt(1, 1_000);
                GENERATED += graphSize;

                for (int j = 0; j < graphSize; j++) {
                    dsg.add(Quad.defaultGraphIRI, NodeFactory.createURI("https://example.org/subjects/" + i), predicate,
                            NodeFactory.createLiteralString(Integer.toString(j)));
                }

                sink.send(new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(dsg)));
            }
        }
    }

    @AfterAll
    public static void teardown() {
        KAFKA.teardown();
    }

    private static KafkaRdfPayloadSource<Bytes> prepareSource(int batchSize, KConnectorDesc conn,
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

    private static KConnectorDesc prepareConnector(Properties props) {
        return new KConnectorDesc(List.of(KafkaTestCluster.DEFAULT_TOPIC), KAFKA.getBootstrapServers(), "/ds", null,
                                  true, false, null, props);
    }

    private static Properties prepareProperties() {
        Properties props = SysJenaKafka.consumerProperties(KAFKA.getBootstrapServers());
        props.putAll(KAFKA.getClientProperties());
        return props;
    }

    public static Stream<Arguments> batchSizes() {
        return Stream.of(Arguments.of(500), Arguments.of(5000));
    }

    private void verifyProjection(EventSource<Bytes, RdfPayload> source, FusekiProjector projector, DatasetGraph dsg) {
        long count = 0;
        try (FusekiSink sink = FusekiSink.builder().dataset(dsg).build()) {
            while (!source.isExhausted()) {
                Event<Bytes, RdfPayload> event = source.poll(Duration.ofSeconds(5));
                if (event != null) {
                    projector.project(event, sink);
                    count++;
                }

                if (count == TEST_SIZE) {
                    Assertions.assertEquals(0L, source.remaining());
                    break;
                }
            }

            // Then
            Assertions.assertEquals(TEST_SIZE, count);
        }
    }

    protected void verifyProjection(int batchSize, DatasetGraph dsg, String consumerGroupBaseId) {
        // Given
        Properties props = AbstractDockerTests.prepareProperties();
        KConnectorDesc conn = AbstractDockerTests.prepareConnector(props);
        KafkaRdfPayloadSource<Bytes> source = AbstractDockerTests.prepareSource(batchSize, conn, consumerGroupBaseId);
        FusekiProjector projector = TestFusekiProjector.buildProjector(conn, source, dsg, batchSize);

        // When
        verifyProjection(source, projector, dsg);

        // Then
        Assertions.assertEquals(GENERATED, Txn.calculateRead(dsg, () -> dsg.stream().count()));
    }
}
