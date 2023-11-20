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

/**
 * A base {@link FKProcessor} that passes on one {@link RequestFK at a time}.
 * <p>
 * The subclass is responsible for handling transactions per {@link RequestFK}.
 */
public abstract class FKProcessorBase1 implements FKProcessor {

    protected abstract ResponseFK process1(RequestFK request);

    protected FKProcessorBase1() {}

    @Override
    public final ResponseFK process(RequestFK request) {
        return process1(request);
    }
}
