/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.fuseki.kafka;

import org.apache.jena.util.Metadata;

public class Meta {
    /** The root name for metadata */
    public static final String MPATH         = "io.telicent.jena.fuseki-kafka";

    static private String metadataLocation  = "io/telicent/jena/fuseki-kafka.properties.xml";

    static private Metadata metadata        = new Metadata(metadataLocation);

    /** The product name */
    public static final String NAME         = metadata.get(MPATH+".name", "FK");

    /** The full name of the current ARQ version */
    public static final String VERSION      = metadata.get(MPATH+".version", "unknown");

    /** The date and time at which this release was built */
    public static final String BUILD_DATE   = metadata.get(MPATH+".build.datetime", "unset");
}
