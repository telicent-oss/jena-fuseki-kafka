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

import java.io.ByteArrayOutputStream;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.fuseki.kafka.lib.HttpServletRequestMinimal;
import org.apache.jena.fuseki.kafka.lib.HttpServletResponseMinimal;
import org.apache.jena.fuseki.server.Dispatcher;
import org.apache.jena.kafka.RequestFK;
import org.apache.jena.kafka.ResponseFK;

/**
 * A {@link FKProcessor} that sends {@link RequestFK} to Fuseki via the usual Fuseki
 * dispatch process.
 * <p>
 * This implementation of {@link FKProcessor} puts one transaction around each Kafka
 * message processing because it is done by the Fuseki dispatch and action.
 */
public class FKProcessorFusekiDispatch extends FKProcessorBase1 {
    private static byte[] emptyBytes = new byte[0];
    private String requestURI;
    private ServletContext servletContext;

    public FKProcessorFusekiDispatch(String requestURI, ServletContext servletContext) {
        this.requestURI = requestURI;
        this.servletContext = servletContext;
    }

    @Override
    public void startBatch(int batchSize, long offsetStart) {}

    @Override
    public void finishBatch(int processedCount, long finishOffset, long startOffset) {}

    @Override
    protected ResponseFK process1(RequestFK requestFK) {
        Map<String, String> requestParameters = Map.of();
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        HttpServletRequest request = new HttpServletRequestMinimal(requestURI, requestFK.getHeaders(), requestParameters,
                                                                   requestFK.getInputStream(), servletContext);
        HttpServletResponseMinimal response = new HttpServletResponseMinimal(bytesOut);

        // Full dispatch via Fuseki.
        Dispatcher.dispatch(request, response);

        byte[] responseBytes = ( bytesOut.size() != 0 ) ? bytesOut.toByteArray() : emptyBytes;
        ResponseFK result = ResponseFK.create(requestFK.getTopic(), response.headers(), responseBytes);
        return result;
    }

}
