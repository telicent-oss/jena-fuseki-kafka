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
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

public class DockerTestFusekiProjector {

    private static final int TEST_SIZE = 10_000;
    private static int GENERATED = 0;

    private static final KafkaTestCluster KAFKA = new BasicKafkaTestCluster();
    private static final Random RANDOM = new Random();

    @BeforeAll
    public static void setup() throws InterruptedException {
        KAFKA.setup();
        KAFKA.resetTestTopic();
        Thread.sleep(1000);

        // Generate test events
        try (KafkaSink<Bytes, RdfPayload> sink = KafkaSink.<Bytes, RdfPayload>create()
                                                          .bootstrapServers(KAFKA.getBootstrapServers())
                                                          .producerConfig(KAFKA.getClientProperties())
                                                          .topic(KafkaTestCluster.DEFAULT_TOPIC)
                                                          .keySerializer(BytesSerializer.class)
                                                          .valueSerializer(RdfPayloadSerializer.class)
                                                          .build()) {
            Node predicate = NodeFactory.createURI("https://example.org/predicate");
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

    private void verifyProjection(int batchSize, DatasetGraph dsg, String consumerGroupBaseId) {
        // Given
        Properties props = prepareProperties();
        KConnectorDesc conn = prepareConnector(props);
        KafkaRdfPayloadSource<Bytes> source = prepareSource(batchSize, conn, consumerGroupBaseId);
        FusekiProjector projector = TestFusekiProjector.buildProjector(conn, source, dsg, batchSize);

        // When
        verifyProjection(source, projector, dsg);

        // Then
        Assertions.assertEquals(GENERATED, Txn.calculateRead(dsg, () -> dsg.stream().count()));
    }

    public static Stream<Arguments> batchSizes() {
        return Stream.of(Arguments.of(500), Arguments.of(1000), Arguments.of(5000));
    }

    @MethodSource("batchSizes")
    @ParameterizedTest
    public void givenFusekiProjector_whenProjectingFromKafkaToInMemoryTransactionalDataset_thenDataIsLoaded(
            int batchSize) {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When and Then
        verifyProjection(batchSize, dsg, "fuseki-kafka-projection-mem-transactional-");
    }

    @MethodSource("batchSizes")
    @ParameterizedTest
    public void givenFusekiProjector_whenProjectingFromKafkaToTdb2Dataset_thenDataIsLoaded(int batchSize) throws IOException {
        // Given
        Location location = Location.create(Files.createTempDirectory("fuseki-kafka"));
        DatasetGraph dsg = TDB2Factory.connectDataset(location).asDatasetGraph();

        // When and Then
        verifyProjection(batchSize, dsg, "fuseki-kafka-projection-tdb2-");
    }
}
