package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.SinkException;
import io.telicent.smart.cache.projectors.sinks.Sinks;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.kafka.KafkaRdfPayloadSource;
import io.telicent.smart.cache.sources.kafka.serializers.RdfPayloadSerializer;
import io.telicent.smart.cache.sources.kafka.sinks.KafkaSink;
import io.telicent.smart.cache.sources.memory.SimpleEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

@SuppressWarnings({ "unchecked", "java:S119", "java:S3577" })
class DockerTestFusekiProjectorDlq extends AbstractDockerTests {

    protected static final RuntimeException SINK_ERROR = new SinkException("fails");
    private static final AtomicInteger CONSUMER_ID = new AtomicInteger(1);
    protected static final String DLQ_TOPIC = "dlq";

    private static String largePayload(int literalSize) {
        return "<https://subject> <https://predicate> \"" + RandomStringUtils.insecure()
                                                                             .nextAlphabetic(literalSize) + "\".";
    }

    /**
     * A 5MB+ event which is well over the default Kafka event size limit
     */
    protected static final SimpleEvent<Bytes, RdfPayload> TOO_LARGE_EVENT = largeEvent(largePayload(1024 * 1024 * 5));

    /**
     * A 1MB event which is effectively at the default Kafka event size limit
     */
    protected static final SimpleEvent<Bytes, RdfPayload> MAX_SIZE_EVENT =
            largeEvent(largePayload(1024 * 1024));

    /**
     * A barely sub-1MB event which once the DLQ headers are added will be over the default Kafka event size limit
     */
    protected static final SimpleEvent<Bytes, RdfPayload> NEAR_MAX_SIZE_EVENT =
            largeEvent(largePayload((1024 * 1024) - 256));

    private static SimpleEvent<Bytes, RdfPayload> largeEvent(String payload) {
        return new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(WebContent.contentTypeNTriples,
                                                                              payload.getBytes(
                                                                                      StandardCharsets.UTF_8)));
    }

    protected static final RdfPayload BLANK_RDF_PAYLOAD = RdfPayload.of(null, new byte[0]);

    @BeforeEach
    void setupDlq() {
        KAFKA.resetTopic(DLQ_TOPIC);
    }

    private static KafkaSink.KafkaSinkBuilder<Bytes, RdfPayload> dlqBuilder(
            Properties props) {
        return KafkaSink.<Bytes, RdfPayload>create()
                        .bootstrapServers(KAFKA.getBootstrapServers())
                        .topic(DLQ_TOPIC)
                        .producerConfig(props)
                        .keySerializer(BytesSerializer.class)
                        .valueSerializer(RdfPayloadSerializer.class)
                        .noAsync()
                        .noLinger();
    }

    protected static KafkaRdfPayloadSource<Bytes> prepareDlqSource(String consumerGroupBaseId) {
        return KafkaRdfPayloadSource.<Bytes>createRdfPayload()
                                    .bootstrapServers(KAFKA.getBootstrapServers())
                                    .topic(DLQ_TOPIC)
                                    .consumerGroup(consumerGroupBaseId + CONSUMER_ID.incrementAndGet())
                                    .consumerConfig(KAFKA.getClientProperties())
                                    .keyDeserializer(BytesDeserializer.class)
                                    .fromBeginning()
                                    // Don't ignore tombstones as DlqRetryHandler might have inserted tombstone events
                                    // to work around the edge case of too large events being sent to DLQ
                                    .ignoreTombstones(false)
                                    .build();
    }

    private static Stream<Arguments> largeEvents() {
        return Stream.of(Arguments.of(TOO_LARGE_EVENT, "Too Large Event"),
                         Arguments.of(MAX_SIZE_EVENT, "At Max Size Event"),
                         Arguments.of(NEAR_MAX_SIZE_EVENT, "Near Max Size Event"));
    }

    @ParameterizedTest(name = "{1} Events are sent to DLQ with Value Blanked")
    @MethodSource("largeEvents")
    void givenKafkaDlq_whenProjectingLargeEvents_thenSentToDlqWithValueBlanked(Event<Bytes, RdfPayload> event,
                                                                               String name) {
        // Given
        Properties props = prepareProperties();
        try (KafkaSink<Bytes, RdfPayload> dlq = dlqBuilder(props)
                .forDlq(BLANK_RDF_PAYLOAD)
                .build()) {

            // When
            KConnectorDesc conn = prepareConnector(props);
            EventSource<Bytes, RdfPayload> source = mock(EventSource.class);
            DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
            FusekiProjector projector =
                    TestFusekiProjector.buildProjector(conn, source, dsg, 1, dlq);
            projector.project(event, e -> {throw new RuntimeException("fails");});

            // Then
            KafkaRdfPayloadSource<Bytes> dlqSource = prepareDlqSource(name.toLowerCase(Locale.ROOT).replace(' ', '-'));
            try {
                Event<Bytes, RdfPayload> dlqEvent = dlqSource.poll(Duration.ofSeconds(5));
                Assertions.assertNotNull(dlqEvent, name + " should have been successfully sent to the DLQ");
                Assertions.assertEquals(0L, dlqEvent.value().sizeInBytes(),
                                        name + " should have had its value blanked");
                Assertions.assertNull(dlqSource.poll(Duration.ofSeconds(5)), "Should only be a single event on the DLQ topic");
            } finally {
                dlqSource.close();
            }
        }
    }

    @Test
    void givenKafkaDlqWithoutCustomBlankValue_whenProjectingEventAtMaxSize_thenFailsToSendToDlqThrowingOriginalError() {
        // Given
        Properties props = prepareProperties();
        try (KafkaSink<Bytes, RdfPayload> dlq = dlqBuilder(props)
                .forDlq()
                .build()) {

            // When
            KConnectorDesc conn = prepareConnector(props);
            EventSource<Bytes, RdfPayload> source = mock(EventSource.class);
            DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
            FusekiProjector projector =
                    TestFusekiProjector.buildProjector(conn, source, dsg, CONSUMER_ID.incrementAndGet(), dlq);
            Exception thrown =
                    Assertions.assertThrows(Exception.class, () -> projector.project(MAX_SIZE_EVENT, event -> {
                        throw SINK_ERROR;
                    }));

            // Then
            Assertions.assertEquals(SINK_ERROR, thrown);
        }
    }

    @Test
    void givenFailingDlq_whenProjectionFails_thenFailsToSendToDlqThrowingOriginalError() {
        // Given
        Properties props = prepareProperties();
        try (Sink<Event<Bytes, RdfPayload>> dlq = Sinks.<Event<Bytes, RdfPayload>>reject()
                                                       .predicate(x -> false)
                                                       .build()) {

            // When
            KConnectorDesc conn = prepareConnector(props);
            EventSource<Bytes, RdfPayload> source = mock(EventSource.class);
            DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
            FusekiProjector projector =
                    TestFusekiProjector.buildProjector(conn, source, dsg, CONSUMER_ID.incrementAndGet(), dlq);
            Exception thrown =
                    Assertions.assertThrows(Exception.class, () -> projector.project(MAX_SIZE_EVENT, event -> {
                        throw SINK_ERROR;
                    }));

            // Then
            Assertions.assertEquals(SINK_ERROR, thrown);
        }
    }
}
