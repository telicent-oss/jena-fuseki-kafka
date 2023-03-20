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

import org.apache.jena.util.Metadata;

public class SysJenaKafka {

    public static final String PATH         = "org.apache.jena.kafka";
    private static String metadataLocation  = "org/apache/jena/kafka/jena-kafka.xml";
    private static Metadata metadata        = new Metadata(metadataLocation);

    /** The product name */
    public static final String NAME         = "Apache Jena Kafka Connector";
    /** The full name of the current ARQ version */
    public static final String VERSION      = metadata.get(PATH+".version", "unknown");
    /** The date and time at which this release was built */
    public static final String BUILD_DATE   = metadata.get(PATH+".build.datetime", "unset");

    public static void init() {}

}
