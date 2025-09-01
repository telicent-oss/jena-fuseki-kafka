package org.apache.jena.kafka.common;

import io.telicent.smart.cache.payloads.RdfPayload;
import io.telicent.smart.cache.payloads.RdfPayloadException;
import io.telicent.smart.cache.projectors.Sink;
import io.telicent.smart.cache.projectors.driver.StallAwareProjector;
import io.telicent.smart.cache.sources.Event;
import io.telicent.smart.cache.sources.EventSource;
import io.telicent.smart.cache.sources.Header;
import io.telicent.smart.cache.sources.TelicentHeaders;
import io.telicent.smart.cache.sources.kafka.KafkaEvent;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.kafka.FusekiKafka;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.kafka.KConnectorDesc;
import org.apache.jena.kafka.SysJenaKafka;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

/**
 * A projector that processes RDF Payload events handling the management of transactions
 * <p>
 * The batch size parameter controls roughly how many events are processed in one transaction, if set to {@code 1} then
 * each event will be processed in its own transaction.  When set to {@code 1} all other batching behaviour described
 * here is disabled.
 * </p>
 * <h3>Batching</h3>
 * <p>
 * Assuming a batch size greater than 1 then regardless of the configured batch size the projector tries to maximise the
 * actual batch size wherever possible. For example if you set the batch size to 25 but the event source has fetched 100
 * events into memory then it will attempt to process all 100 events prior to committing the transaction unless it
 * reaches one of the other batching thresholds that trigger a commit sooner.
 * </p>
 * <p>
 * It uses metadata from the event source, if available, to decide when to commit the batch.  Again supposed you set the
 * batch size to 25 but there are fewer than 25 events available, once the event source reports that it has no events
 * remaining (i.e. lag of zero), then the projector guarantees to commit the batch unless Low Volume Batching Mode has
 * been engaged. Note that in pathological cases - a slow upstream producer writing to the consumed topic - this could
 * result in single event transactions which can have adverse effect on database disk size when using TDB2.  However,
 * this scenario is hopefully fairly unusual and should generally be mitigated by the Kafka poll interval, and the
 * automatic detection and engagement of the Low Volume Batching mode.
 * </p>
 * <p>
 * It will also use time based triggering to commit a batch.  If you are reading from topic(s) being populated by a
 * reasonably fast, but low volume producer, and using a high batch size, it is possible that the batch size is rarely
 * reached, but we don't ever completely run out of new events to process.  The maximum transaction duration protects
 * the projector from holding a transaction open endlessly, if a transaction is open for longer than this duration then
 * it will be committed regardless of batch size/remaining events.  This prevents undue delays in data being visible in
 * the graph that could otherwise be caused in this scenario.
 * </p>
 * <p>
 * The projector is also stall aware, if the {@link io.telicent.smart.cache.projectors.driver.ProjectorDriver} finds
 * that there are no new events available, or it loses the connection to Kafka, then it informs the projector.  At this
 * point the projector immediately commits the current batch (if one exists).  Once it starts receiving events again,
 * and/or reconnects to Kafka, then a new batch will be started.
 * </p>
 * <h4>Low Volume Batching Mode</h4>
 * <p>
 * From {@code 2.1.0} onwards the projector now automatically detects when low volumes of input data are leading to
 * small batch sizes and switches into Low Volume Batching Mode.  Detection is done by tracking the size of the last
 * {@code N} batches committed, where {@code N} is configured via {@link KConnectorDesc#getBatchSizeTrackingWindow()}.
 * If the average batch size over this window is less than or equal to the low volume batch size threshold, as
 * configured by {@link KConnectorDesc#getLowVolumeBatchSizeThreshold()}, then low volume batching mode is engaged.
 * </p>
 * <p>
 * While in low volume batching mode the projector does not commit batches upon reaching zero lag, instead it waits
 * until either the batch size, or maximum transaction duration, is reached prior to committing.  This avoids the
 * projector committing lots of small batches when connecting to a topic being populated by a slow/low volume producer.
 * The trade-off being that it introduces some additional delay between events arriving at a topic and their data being
 * visible in the dataset.
 * </p>
 * <p>
 * Once the average batch size increases above the configured {@link KConnectorDesc#getLowVolumeBatchSizeThreshold()}
 * then low volume mode is automatically disabled and the projector returns to normal batching behaviour.
 * </p>
 * <p>
 * This mode may be disabled entirely by setting the average batch size threshold to {@code 0}.
 * </p>
 * <h4>High Lag Batching Mode</h4>
 * <p>
 * From {@code 2.1.0} onwards the projector now automatically detects topics with high lag and engages High Lag Batching
 * mode.  When connecting to topics with lag that exceeds the configured {@link KConnectorDesc#getHighLagThreshold()},
 * then the projector engages high lag batching mode.
 * </p>
 * <p>
 * When in high lag batching mode it ignores the configured batch size in favour of the configured
 * {@link KConnectorDesc#getBatchSizeBytes()}, committing only when the batch reaches that number of bytes, or another
 * batching threshold is reached (e.g. lag reaches zero, maximum transaction duration exceeded).  This mode assumes that
 * most events are relatively small therefore batching events by their total size in bytes, rather than number of
 * events, leads to fewer transactions and thus improved overall performance.
 * </p>
 * <p>
 * Once the lag reaches zero this mode is automatically disabled and the projector returns to normal batching
 * behaviour.
 * </p>
 * <p>
 * This mode may be disabled entirely by setting the high lag threshold to {@link Long#MAX_VALUE}.
 * </p>
 * <h4>Multiple Batching Modes Engaged</h4>
 * <p>
 * While unlikely, it is possible, depending on configuration and the throughput of your Kafka clusters for both high
 * lag and low volume batching modes to be enabled at the same time, e.g. if you have high lag but events are quite
 * large. If this happens then behaviours of both modes apply, so transactions will be committed either when the batch
 * exceeds the configured number of bytes, or the maximum transaction duration. If you are seeing both modes engage
 * simultaneously, which will be noted in the logs, then you may wish to tweak the various configuration parameters to
 * adjust the detection thresholds, or disable one/both of these modes.
 * </p>
 * <h3>Error Handling</h3>
 * <p>
 * If a Dead Letter Queue (DLQ) is configured when creating the projector then any malformed event will be sent to the
 * DLQ.  Assuming that is successful then the projector will ensure that the batch of events prior to the malformed
 * event is properly committed before continuing on with processing.
 * </p>
 * <p>
 * If no DLQ is configured, or sending to it fails, then the projector will throw a {@link JenaKafkaException} which
 * should abort further processing.
 * </p>
 */
public class FusekiProjector implements StallAwareProjector<Event<Bytes, RdfPayload>, Event<Bytes, RdfPayload>> {

    @Getter
    private final KConnectorDesc connector;
    @Getter
    private final DatasetGraph dataset;
    private final EventSource<Bytes, RdfPayload> source;
    @Getter
    private final long batchSize;
    @Getter
    private final Duration maxTransactionDuration;
    private long lastCommitTime = -1L;
    private final List<Event<Bytes, RdfPayload>> eventsSinceLastCommit = new ArrayList<>();
    private final String topicNames;
    private final Sink<Event<Bytes, RdfPayload>> dlq;
    @Getter
    private boolean lowVolumeDetected = false, highLagDetected = false;
    private final DescriptiveStatistics recentBatchSizes;
    @Getter
    private final long batchSizeBytes, highLagThreshold;
    @Getter
    private final int recentBatchSizeWindow, lowVolumeBatchSizeThreshold;

    /**
     * Creates a new projector
     *
     * @param connector Kafka Connector
     * @param source    Event Source
     * @param dataset   Dataset
     * @param batchSize Batch Size, use {@code 1} to disable batching
     * @param dlq       Dead Letter Queue, configuring this to a non-null value allows projection to continue when
     *                  encountering a malformed payload rather than experiencing Head of Line blocking
     */
    @Builder
    private FusekiProjector(KConnectorDesc connector, EventSource<Bytes, RdfPayload> source, DatasetGraph dataset,
                            long batchSize, Duration maxTransactionDuration, Sink<Event<Bytes, RdfPayload>> dlq) {
        this.connector = Objects.requireNonNull(connector, "Kafka Connector descriptor cannot be null");
        this.topicNames = StringUtils.join(this.connector.getTopics(), ", ");
        this.source = Objects.requireNonNull(source, "Event Source cannot be null");
        this.dataset = Objects.requireNonNull(dataset, "Dataset cannot be null");
        this.batchSize = batchSize > 0 ? batchSize : SysJenaKafka.DEFAULT_BATCH_SIZE;
        this.maxTransactionDuration = SysJenaKafka.isValidDuration(maxTransactionDuration) ? maxTransactionDuration :
                                      connector.getMaxTransactionDuration();
        this.dlq = dlq;

        // Obtain advanced parameters from connector definition
        this.batchSizeBytes = connector.getBatchSizeBytes();
        this.highLagThreshold = connector.getHighLagThreshold();
        this.lowVolumeBatchSizeThreshold = connector.getLowVolumeBatchSizeThreshold();
        this.recentBatchSizeWindow = connector.getBatchSizeTrackingWindow();
        this.recentBatchSizes = new DescriptiveStatistics(this.recentBatchSizeWindow);
    }

    @Override
    public void project(Event<Bytes, RdfPayload> event, Sink<Event<Bytes, RdfPayload>> sink) {
        try {
            // If this is our first projected event then treat the current point in time as our last commit point, this
            // is used for determining if we've exceeded our maximum transaction duration.
            // See commitTransactionIfNeeded()
            if (this.lastCommitTime == -1L) {
                this.lastCommitTime = System.currentTimeMillis();
            }

            // Setup for projecting the event
            materialiseValue(event);
            startTransactionIfNeeded();

            // Send the event to the output sink
            sink.send(event);
            this.eventsSinceLastCommit.add(event);

            // Decide whether to commit transaction now, or wait to commit later
            commitTransactionIfNeeded(event);
        } catch (JenaKafkaException e) {
            // In this scenario something has gone wrong while we were processing the event so our current transaction
            // may now be polluted with partial changes from this event.  Therefore, we need to abort the transaction
            // and potentially replay the uncommitted events to ensure their data is not lost.
            if (!sendToDlq(event, e)) {
                abort();
                throw e;
            }
            abortAndReplay(sink);
        } catch (RdfPayloadException e) {
            // Note that in this scenario we hadn't started processing the event, we merely failed to deserialise it so
            // we don't have any risk of uncommitted changes that need replaying.  The transaction up to this point was
            // good so commit it now just in case we're about to encounter a whole block of malformed events.
            commit();

            // Then try and send to the DLQ before proceeding
            if (!sendToDlq(event, e)) {
                throw new JenaKafkaException("Malformed Kafka event", e);
            }
        }
    }

    /**
     * Tries to send an event to the DLQ indicating whether that happened successfully or not
     *
     * @param event Event
     * @param e     Error processing the event
     * @return True if sent to DLQ successfully, false otherwise
     */
    protected final boolean sendToDlq(Event<Bytes, RdfPayload> event, Throwable e) {
        // Log the error
        if (event instanceof KafkaEvent<Bytes, RdfPayload> kafkaEvent) {
            ConsumerRecord<Bytes, RdfPayload> record = kafkaEvent.getConsumerRecord();
            FusekiKafka.LOG.error("[{}] Partition {} Offset {}: {}", record.topic(), record.partition(),
                                  record.offset(), e.getMessage());
        } else {
            FusekiKafka.LOG.error("[{}] Malformed Event: {}", topicNames, event);
        }

        // Try to send to DLQ if configured
        if (this.dlq == null) {
            return false;
        }

        try {
            this.dlq.send(event.addHeaders(Stream.of(new Header(TelicentHeaders.DEAD_LETTER_REASON, e.getMessage()))));
            return true;
        } catch (Throwable dlqError) {
            FusekiKafka.LOG.warn("[{}] Failed to send event to DLQ: {}", this.topicNames, dlqError.getMessage());
        }
        return false;
    }

    /**
     * Aborts the transaction and replays preceding events
     * <p>
     * This is called when processing fails during application of an event since we can't guarantee that the event was
     * applied cleanly.  Thus, we need to abort the whole transaction and then replay the prior uncommitted events that
     * did apply cleanly to ensure we don't lose any data.
     * </p>
     *
     * @param sink The destination sink
     */
    protected final void abortAndReplay(Sink<Event<Bytes, RdfPayload>> sink) {
        abort();

        // Replay the intervening events and commit immediately as we know up to this point of the events we could
        // apply them successfully
        if (!this.eventsSinceLastCommit.isEmpty()) {
            startTransactionIfNeeded();
            FusekiKafka.LOG.info("[{}] Replaying {} uncommitted events", this.topicNames,
                                 this.eventsSinceLastCommit.size());
            for (Event<Bytes, RdfPayload> event : this.eventsSinceLastCommit) {
                sink.send(event);
            }
        }

        // Do internal commit to reset our state regardless
        this.commit();
    }

    /**
     * Aborts the ongoing transaction (if any)
     */
    protected final void abort() {
        // Abort the ongoing transaction as failure to process an event has left us in an indeterminate state
        if (this.dataset.isInTransaction()) {
            FusekiKafka.LOG.warn("[{}] Aborting write transaction due to prior failure", this.topicNames);
            this.dataset.abort();
        }
    }

    /**
     * Commits the ongoing transaction if we've reached an appropriate point to do so
     * <p>
     * See class level Javadoc - {@link FusekiProjector} - for discussion on the batching behaviour.
     * </p>
     *
     * @param event Current event
     */
    protected void commitTransactionIfNeeded(Event<Bytes, RdfPayload> event) {
        // Make a decision about whether to commit
        Duration elapsed = Duration.ofMillis(System.currentTimeMillis() - this.lastCommitTime);

        if (event.value().isPatch() && !this.dataset.isInTransaction()) {
            // Just processed an RDF Patch that committed the transaction for us
            // Need to call our own commit() now or our state will be incorrect
            FusekiKafka.LOG.debug("[{}] Committing due to previous RDF patch event committing", this.topicNames);
            this.commit();
        } else if (this.batchSize == 1) {
            // No batching enabled, always commit immediately
            FusekiKafka.LOG.trace("[{}] Committing due to batching disabled", this.topicNames);
            this.commit();
        } else if (!this.highLagDetected && this.eventsSinceLastCommit.size() >= this.batchSize) {
            // Reached batch size
            // Check whether further events are available immediately, if not commit now
            if (!this.source.availableImmediately()) {
                // All in-memory events consumed so commit now
                FusekiKafka.LOG.debug(
                        "[{}] Committing due to exceeding batch size ({}) and no buffered events available",
                        this.topicNames, this.batchSize);
                commit();
            }
            // If we have more events in-memory we should be being called again shortly with those
        } else if (elapsed.compareTo(this.maxTransactionDuration) >= 0) {
            // Have we exceeded the maximum transaction time?
            // This can happen if we have a slow low volume producer writing to the input topics, this can cause the
            // projector to never hit zero events remaining and means that we would otherwise leave the transaction
            // open far longer than we should.  This both impacts our memory usage, and delays new data appearing in
            // the graph which is a bad user experience.
            FusekiKafka.LOG.warn(
                    "[{}] Committing due to exceeding maximum transaction duration ({}), this is most likely caused by a slow low volume producer writing to these topics.",
                    this.topicNames, this.maxTransactionDuration);
            commit();
        } else {
            if (this.highLagDetected && calculateBatchSizeInBytes() > batchSizeBytes) {
                // We are in high lag batching mode AND we've exceeded the batch size threshold
                long sizeInBytes = calculateBatchSizeInBytes();
                FusekiKafka.LOG.debug("[{}] Committing due to exceeding high lag data size threshold ({})",
                                      this.topicNames, FileUtils.byteCountToDisplaySize(sizeInBytes));
                commit();
            } else if (!this.lowVolumeDetected) {
                // Batch size not reached BUT we could be caught up with the Kafka topic(s) i.e. zero lag
                Long remaining = this.source.remaining();
                if (remaining != null && remaining == 0L) {
                    // Caught up, commit now!
                    FusekiKafka.LOG.info("[{}] Completely up to date with Kafka topic(s)", this.topicNames);
                    commit();

                    // Reset high lag detection state if we've caught up
                    if (this.highLagDetected) {
                        FusekiKafka.LOG.info("[{}] Lag is now zero, disabling high lag batching mode for these topics",
                                             this.topicNames);
                        this.highLagDetected = false;
                    }
                } else {
                    if (!this.highLagDetected && remaining != null && remaining > highLagThreshold) {
                        // Enable high lag batching mode
                        FusekiKafka.LOG.info(
                                "[{}] Lag is currently {}, switching to high lag batching mode for these topics",
                                this.topicNames, remaining);
                        this.highLagDetected = true;
                    }
                }
                // Not caught up so we should be being called again shortly with further events after the next Kafka
                // poll completes
            }
        }
    }

    /**
     * Starts a new transaction if we aren't already in one
     */
    protected final void startTransactionIfNeeded() {
        // Start a new write transaction if we aren't currently in one
        if (!this.dataset.isInTransaction()) {
            this.dataset.begin(TxnType.WRITE);
            FusekiKafka.LOG.debug("[{}] Started new write transaction for incoming Kafka Events", this.topicNames);
        }
    }

    /**
     * Materialises the values of the event, a {@link RdfPayloadException} will be thrown if the event has a malformed
     * payload
     * <p>
     * {@link RdfPayloadException}'s are handled in our main {@link #project(Event, Sink)} method.
     * </p>
     *
     * @param event Event
     * @throws RdfPayloadException Thrown if the event has a malformed payload
     */
    protected final void materialiseValue(Event<Bytes, RdfPayload> event) {
        if (event.value().isDataset()) {
            event.value().getDataset();
        } else {
            event.value().getPatch();
        }
    }

    /**
     * Commits the current transaction and updates our internal state used by {@link #commitTransactionIfNeeded(Event)}
     * to decide if this method should be called.
     */
    @SuppressWarnings("rawtypes")
    protected final void commit() {
        // Commit changes to the dataset
        this.lastCommitTime = System.currentTimeMillis();
        FusekiKafka.LOG.debug("[{}] Committing write transaction for {} Kafka events", this.topicNames,
                              this.eventsSinceLastCommit.size());
        if (this.dataset.isInTransaction()) {
            this.dataset.commit();
        }

        // Assuming that succeeds tell the event source that the events were processed, this will cause any offsets to
        // be committed
        source.processed(this.eventsSinceLastCommit.stream().map(e -> (Event) e).toList());

        // Log where we've got to with Kafka offsets when applicable
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = KafkaEventSource.determineCommitOffsetsFromEvents(
                this.eventsSinceLastCommit.stream().map(e -> (Event) e).toList());
        if (MapUtils.isNotEmpty(committedOffsets)) {
            StringBuilder offsetLogMessage = new StringBuilder();
            offsetLogMessage.append("[").append(this.topicNames).append("] Processed up to offsets: ");
            for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : committedOffsets.entrySet()) {
                offsetLogMessage.append(offset.getKey().topic())
                                .append('-')
                                .append(offset.getKey().partition())
                                .append('=')
                                .append(offset.getValue().offset())
                                .append(", ");
            }
            offsetLogMessage.delete(offsetLogMessage.length() - 2, offsetLogMessage.length());
            long sizeInBytes = calculateBatchSizeInBytes();
            offsetLogMessage.append(" (")
                            .append(String.format("%,d", this.eventsSinceLastCommit.size()))
                            .append(" events");
            if (sizeInBytes > 0) {
                offsetLogMessage.append(", ").append(FileUtils.byteCountToDisplaySize(sizeInBytes));
            }
            offsetLogMessage.append(")");
            FusekiKafka.LOG.info("{}", offsetLogMessage);
        }

        // Track the sizes of batches to help detect low volume conditions
        // Only detect this once we have sufficient number of batches observed
        this.recentBatchSizes.addValue(this.eventsSinceLastCommit.size());
        if (this.recentBatchSizes.getN() >= recentBatchSizeWindow) {
            long avgBatchSize = Math.round(this.recentBatchSizes.getMean());
            if (!this.lowVolumeDetected && avgBatchSize <= lowVolumeBatchSizeThreshold) {
                FusekiKafka.LOG.info(
                        "[{}] Average batch size is currently {}, switching to low volume batching mode for these topics",
                        this.topicNames, avgBatchSize);
                this.lowVolumeDetected = true;
            } else if (this.lowVolumeDetected && avgBatchSize > lowVolumeBatchSizeThreshold) {
                FusekiKafka.LOG.info(
                        "[{}] Average batch size increased to {}, disabling low volume batching mode for these topics",
                        this.topicNames, avgBatchSize);
                this.lowVolumeDetected = false;
            }
        }

        // Reset our state ready for next batch
        this.eventsSinceLastCommit.clear();
    }

    private Long calculateBatchSizeInBytes() {
        return this.eventsSinceLastCommit.stream().map(e -> e.value().sizeInBytes()).reduce(0L, Long::sum);
    }

    @Override
    public void stalled(Sink<Event<Bytes, RdfPayload>> sink) {
        if (this.dataset.isInTransaction()) {
            // If we're in a transaction then we have pending uncommitted changes and should commit upon a stall
            // occurring
            // A stall means that there's no new events available from Kafka, i.e. we're caught up, or we're not
            // currently connected to Kafka.  This latter case might be a transient error but as we don't know which
            // case we've encountered, nor in the latter case when it might resolve, safer to commit the open
            // transaction for now and start a new one as and when we start receiving new data again.
            FusekiKafka.LOG.warn(
                    "[{}] Committing due to stall, either no new events are currently available for these topic(s) or the connection to Kafka was lost",
                    this.topicNames);
            this.commit();
        }
    }
}
