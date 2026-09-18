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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

@SuppressWarnings({ "unchecked", "java:S119", "java:S3577" })
class DockerTestFusekiProjectorDlq extends AbstractDockerTests {

    protected static final RuntimeException SINK_ERROR = new SinkException("fails");
    private static final AtomicInteger CONSUMER_ID = new AtomicInteger(1);
    protected static final String DLQ_TOPIC = "dlq";

    protected static final String MAX_SIZE_PAYLOAD_DATA =
            "<https://subject> <https://predicate> \"" + RandomStringUtils.insecure()
                                                                          .nextAlphabetic(1024 * 1024) + "\".";
    protected static final SimpleEvent<Bytes, RdfPayload> MAX_SIZE_EVENT =
            new SimpleEvent<>(Collections.emptyList(), null, RdfPayload.of(WebContent.contentTypeNTriples,
                                                                           MAX_SIZE_PAYLOAD_DATA.getBytes(
                                                                                   StandardCharsets.UTF_8)));
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

    @Test
    void givenKafkaDlq_whenProjectingEventAtMaxSize_thenSentToDlqWithValueBlanked() {
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
                    TestFusekiProjector.buildProjector(conn, source, dsg, CONSUMER_ID.incrementAndGet(), dlq);
            projector.project(MAX_SIZE_EVENT, event -> {throw new RuntimeException("fails");});

            // Then
            KafkaRdfPayloadSource<Bytes> dlqSource = prepareDlqSource("max-size-event");
            try {
                Event<Bytes, RdfPayload> dlqEvent = dlqSource.poll(Duration.ofSeconds(5));
                Assertions.assertNotNull(dlqEvent);
                Assertions.assertEquals(0L, dlqEvent.value().sizeInBytes());
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
