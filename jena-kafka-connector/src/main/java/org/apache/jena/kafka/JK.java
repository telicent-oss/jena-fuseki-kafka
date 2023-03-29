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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.kafka.common.header.Header;

public class JK {

    /**
     * Kafka headers to a Map. If there are multiple headers with the same key name,
     * only the last header value goes in the map.
     */
    public static Map<String, String> headerToMap(Iterable<Header> headers) {
        Map<String, String> map = new HashMap<>();
        headers.forEach(header->{
            String hName = header.key();
            String hValue = Bytes.bytes2string(header.value());
            map.put(hName,  hValue);
        });
        return map;
    }

    /**
     * Map to Kafka headers. No support for multiple headers with the same key name.
     */
    public static Iterable<Header> mapToHeaders(Map<String, String> headerMap) {
        List<Header> headers = new ArrayList<>();
        headerMap.forEach((name, value)->{
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            headers.add(new RecordHeader(name, bytes));
        });
        return headers;
    }
}
