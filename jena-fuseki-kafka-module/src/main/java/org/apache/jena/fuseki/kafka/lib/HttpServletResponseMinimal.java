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

package org.apache.jena.fuseki.kafka.lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.web.HttpSC;

public class HttpServletResponseMinimal implements HttpServletResponse {

    // Single occurrence headers only.
    private final Map<String, String> headers = new HashMap<>();
    private final OutputStream output;
    private boolean hasCommitted = false;
    private int status = 200;

    public HttpServletResponseMinimal(OutputStream output) {
        this.output= output ;
    }

    @Override
    public ServletOutputStream getOutputStream() {
        if ( hasCommitted )
            throw new IllegalStateException();
        return new ServletOutputStreamX(output, ()-> hasCommitted = true);
    }

    static class ServletOutputStreamX extends ServletOutputStream {
        private final OutputStream output;
        private final Runnable commitHook;
        ServletOutputStreamX(OutputStream out, Runnable commitHook) {
            output = out;
            this.commitHook = commitHook;
        }

        @Override
        public boolean isReady() { return true; }

        @Override
        public void setWriteListener(WriteListener writeListener) {}

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            output.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException { output.write(b); }

        @Override
        public void close() throws IOException { flush(); output.close(); }

        @Override
        public void flush() throws IOException { output.flush(); commitHook.run(); }
    }

    @Override
    public PrintWriter getWriter() {
        if ( hasCommitted )
            throw new IllegalStateException();
        hasCommitted = true;
        return new PrintWriter(output, false , StandardCharsets.UTF_8);
    }

    @Override
    public void sendError(int sc, String msg) throws IOException { sendError(sc); }

    @Override
    public void sendError(int sc) {
        if ( hasCommitted )
            throw new IllegalStateException();
        hasCommitted = true;
    }

    @Override
    public void sendRedirect(String location) {
        if ( hasCommitted )
            throw new IllegalStateException();
        setStatus(HttpSC.FOUND_302);
        setHeader(HttpNames.hLocation, location);
        hasCommitted = true;
    }

    @Override
    public void setStatus(int sc) {
        status = sc;
    }

    @Override
    public void setStatus(int sc, String sm) { setStatus(sc); }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public int getBufferSize() {
        // No buffering.
        return 0;
    }

    @Override
    public void setBufferSize(int size) {
        if ( hasCommitted )
            throw new IllegalStateException();
        throw new UnsupportedOperationException();
    }

    @Override
    public void flushBuffer() {
        hasCommitted = true;
    }

    @Override
    public void resetBuffer() {
        if ( hasCommitted )
            throw new IllegalStateException();
    }

    @Override
    public boolean isCommitted() {
        return hasCommitted;
    }

    @Override
    public void reset() {
        if ( hasCommitted )
            throw new IllegalStateException();
    }

    @Override
    public String getCharacterEncoding() {
        return WebContent.charsetUTF8;
    }

    @Override
    public String getContentType() {
        return getHeader(HttpNames.hContentType);
    }

    @Override
    public void setCharacterEncoding(String charset) {}

    @Override
    public void setContentLength(int len) {
        setHeader(HttpNames.hContentLength, Integer.toString(len));
    }

    @Override
    public void setContentLengthLong(long len) {
        setHeader(HttpNames.hContentLength, Long.toString(len));
    }


    @Override
    public void setContentType(String type) {
        setHeader(HttpNames.hContentType, type);
    }

    @Override
    public void setDateHeader(String name, long date) {
        setHeader(name, new Date(date).toString());
    }

    @Override
    public void addDateHeader(String name, long date) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHeader(String name, String value) {
        headers.put(name, value);
    }

    @Override
    public void addHeader(String name, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIntHeader(String name, int value) {
        setHeader(name, Integer.toString(value));
    }

    @Override
    public void addIntHeader(String name, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHeader(String name) {
        return headers.get(name);
    }

    public Map<String, String> headers() { return headers; }

    @Override
    public Collection<String> getHeaders(String name) {
        String x = getHeader(name);
        if ( x == null )
            return List.of();
        return List.of(x);
    }

    @Override
    public Collection<String> getHeaderNames() {
        return Collections.unmodifiableCollection(headers.keySet());
    }

    @Override
    public boolean containsHeader(String name) {
        return headers.containsKey(name);
    }

    @Override
    public void setLocale(Locale loc) {}

    @Override
    public Locale getLocale() {
        return Locale.ROOT;
    }

    @Override
    public void addCookie(Cookie cookie) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeURL(String url) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectURL(String url) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeUrl(String url) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectUrl(String url) {
        throw new UnsupportedOperationException();
    }
}

