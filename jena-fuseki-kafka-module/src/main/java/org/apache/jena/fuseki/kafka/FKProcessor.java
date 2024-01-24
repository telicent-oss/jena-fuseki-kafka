/*
 *  Copyright (c) Telicent Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.jena.fuseki.kafka;

import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.ResponseFK;
import org.apache.jena.sparql.core.Transactional;

/**
 * Process incoming {@link RequestFK} and return a {@link ResponseFK}.
 * <p>
 * This is the interface called from {@link FKBatchProcessor} with one Kafka message
 * per call.
 * <p>
 * It is called within a {@link Transactional} given to {@link FKBatchProcessor} if
 * is given a {@link Transactional} otherwise the implement of this interface is
 * responsible for transactions.
 */
public interface FKProcessor {

    /**
     * Start batch.
     */
    public void startBatch(int batchSize, long offsetStart);

    /**
     * Process one request.
     */
    public ResponseFK process(RequestFK request);

    /**
     * Finished batch.
     */
    public void finishBatch(int processedCount, long finishOffset, long startOffset);
}
