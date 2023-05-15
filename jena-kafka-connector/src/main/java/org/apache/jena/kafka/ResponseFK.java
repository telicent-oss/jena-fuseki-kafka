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

import java.util.Map;

/**
 * Response from handling a {@link RequestFK}.
 */
public class ResponseFK extends ActionKafka {

    private static byte[] noBytes = new byte[0];

    /** A simple {@link ResponseFK} for "success no body" */
    public static ResponseFK success(String topic) {
        return create(topic, Map.of(), noBytes);
    }

    /** A general {@link ResponseFK} */
    public static ResponseFK create(String topic, Map<String, String> headers, byte[] bytes) {
        ResponseFK response = new ResponseFK(topic, headers, bytes);
        return response;
    }

    private ResponseFK(String topic, Map<String, String> headers, byte[] bytes) {
        super(topic, headers, bytes);
    }
}
