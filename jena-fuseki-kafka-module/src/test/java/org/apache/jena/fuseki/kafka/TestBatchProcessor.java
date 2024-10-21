package org.apache.jena.fuseki.kafka;

import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.common.DataState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.jena.fuseki.kafka.FKBatchProcessor.createBatchProcessor;
import static org.apache.jena.fuseki.kafka.FKBatchProcessor.MIN_BATCH_CHECK_THRESHOLD;
import static org.apache.jena.kafka.SysJenaKafka.KAFKA_FETCH_POLL_SIZE;
import static org.apache.jena.kafka.SysJenaKafka.KAFKA_FETCH_BYTE_SIZE;
import static org.apache.jena.kafka.common.DataState.createEphemeral;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestBatchProcessor {

    private final static String TOPIC = "test-topic";
    @SuppressWarnings("unchecked")
    private final static Consumer<String, RequestFK> consumerMock = mock(Consumer.class);
    private final static FKProcessor mockProcessor = mock(FKProcessor.class);
    private final static FKBatchProcessor processor = createBatchProcessor(mockProcessor);
    private final static ConsumerRecords<String, RequestFK> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
    private final static int ORIGINAL_POLL_SIZE = KAFKA_FETCH_POLL_SIZE;
    private final static int ORIGINAL_BYTE_SIZE = KAFKA_FETCH_BYTE_SIZE;
    private final static int ORIGINAL_MIN_CHECK = MIN_BATCH_CHECK_THRESHOLD;


    @AfterMethod
    public void cleanup() {
        reset(consumerMock, mockProcessor);
        KAFKA_FETCH_POLL_SIZE = ORIGINAL_POLL_SIZE;
        KAFKA_FETCH_BYTE_SIZE = ORIGINAL_BYTE_SIZE;
        MIN_BATCH_CHECK_THRESHOLD = ORIGINAL_MIN_CHECK;
    }
    /**
     * In this test we illustrate the scenario whereby we poll kafka and get nothing back.
     * There is obviously no point polling kafka again and so continue as normal.
     */
    @Test
    public void test_empty_messages() {
        // given
        when(consumerMock.poll(any())).thenReturn(emptyRecords);

        // when
        boolean result = processor.receiver(consumerMock, createEphemeral(TOPIC), Duration.ofMillis(1));

        //then
        Assert.assertFalse(result);
        verify(consumerMock, times(1)).poll(any());
        verifyNoInteractions(mockProcessor);
    }

    /**
     * In this we test the case where a single message comes in.
     * Our batching will poll Kafka again, due to a single record falling
     * beneath the min batch threshold (MIN_BATCH_CHECK_THRESHOLD).
     * Our subsequent kafka poll returns an empty result. So this is a single
     * isolated updated. We then process it as normal.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_simple_case_single_message() {
        // given
        ConsumerRecords<String, RequestFK> cRecords = generateConsumerRecords(1);
        when(consumerMock.poll(any())).thenReturn(cRecords,emptyRecords);

        // when
        boolean result = processor.receiver(consumerMock, createEphemeral(TOPIC), Duration.ofMillis(1));

        //then
        Assert.assertTrue(result);
        verify(mockProcessor).startBatch(anyInt(),anyLong());
        verify(mockProcessor).process(any());
        verify(mockProcessor).finishBatch(anyInt(),anyLong(),anyLong());
    }


    /**
     * In this test, we receive a batch of messages in a single poll that are
     * above the batch threshold, and so we do not poll kafka again, processing
     * as normal.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_simple_case_multiple_messages_above_threshold() {
        // given
        MIN_BATCH_CHECK_THRESHOLD = 5; // Reducing threshold for purposes of test.
        int RECORD_COUNT = 6; // Just above threshold
        ConsumerRecords<String, RequestFK> cRecords = generateConsumerRecords(RECORD_COUNT);
        when(consumerMock.poll(any())).thenReturn(cRecords,emptyRecords);

        // when
        boolean result = processor.receiver(consumerMock, createEphemeral(TOPIC), Duration.ofMillis(1));

        //then
        Assert.assertTrue(result);
        verify(mockProcessor).startBatch(anyInt(),anyLong());
        verify(mockProcessor, times(6)).process(any());// 1 message of 6 records
        verify(mockProcessor).finishBatch(anyInt(),anyLong(),anyLong());
    }


    /**
     * In this test, we replicate an ongoing update whereby messages come in
     * constantly in small amounts. We will then batch the message till we
     * trip one of the limits, in this case the records size (KAFKA_FETCH_POLL_SIZE).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_batch_case_poll_limit() {
        // given
        KAFKA_FETCH_POLL_SIZE = 20; // Reducing threshold for purposes of test.
        final int RECORD_COUNT = 4; // Will take five iterations to hit threshold
        ConsumerRecords<String, RequestFK> cRecords = generateConsumerRecords(RECORD_COUNT);
        // It will take 5 messages to hit threshold
        when(consumerMock.poll(any())).thenReturn(cRecords,cRecords,cRecords,cRecords,cRecords,emptyRecords);

        // when
        boolean result = processor.receiver(consumerMock, createEphemeral(TOPIC), Duration.ofMillis(1));

        // then
        Assert.assertTrue(result);
        verify(mockProcessor).startBatch(anyInt(),anyLong());
        verify(mockProcessor, times(20)).process(any());// 5 messages of 4 records
        verify(mockProcessor).finishBatch(anyInt(),anyLong(),anyLong());
    }

    /**
     * In this test, we replicate an ongoing update whereby messages come in
     * constantly in small amounts. We will then batch the message till we
     * trip one of the limits, in this case the records size (KAFKA_FETCH_BYTE_SIZE).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void test_batch_case_size_limit() {
        // given
        KAFKA_FETCH_BYTE_SIZE = 40; // Reducing threshold for purposes of test (in bytes).
        DataState dataState = createEphemeral(TOPIC);
        final int RECORD_COUNT = 4;// Each generated message is approximately 6 bytes (x4=28 Bytes).
        ConsumerRecords<String, RequestFK> cRecords = generateConsumerRecords(RECORD_COUNT);
        // It will take two messages to pass the limit (56 Bytes)
        when(consumerMock.poll(any())).thenReturn(cRecords,cRecords,emptyRecords);

        // when
        boolean result = processor.receiver(consumerMock, dataState, Duration.ofMillis(1));

        // then
        Assert.assertTrue(result);
        verify(mockProcessor).startBatch(anyInt(),anyLong());
        verify(mockProcessor, times(8)).process(any()); // 2 messages of 4 records
        verify(mockProcessor).finishBatch(anyInt(),anyLong(),anyLong());
    }


    private ConsumerRecords<String, RequestFK> generateConsumerRecords(int records) {
        List<ConsumerRecord<String,RequestFK>> consumerRecordList = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            consumerRecordList.add(new ConsumerRecord<>(TOPIC, 1, i, "Key", new RequestFK("test", Map.of(), String.format("Test-%s", i).getBytes())));
        }
        return new ConsumerRecords<>(Map.of(
                new TopicPartition(TOPIC, 1), consumerRecordList)
        );
    }
}