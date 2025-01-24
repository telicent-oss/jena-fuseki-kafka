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

package org.apache.jena.kafka;

import org.apache.jena.shared.JenaException;

/**
 * Exception indicating something went wrong with the Jena Fuseki Kafka integration
 */
public class JenaKafkaException extends JenaException {
    /**
     * Creates a new exception
     *
     * @param message Message
     */
    public JenaKafkaException(String message) {
        super(message);
    }

    /**
     * Creates a new exception
     *
     * @param message Message
     * @param cause   Cause
     */
    public JenaKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}

