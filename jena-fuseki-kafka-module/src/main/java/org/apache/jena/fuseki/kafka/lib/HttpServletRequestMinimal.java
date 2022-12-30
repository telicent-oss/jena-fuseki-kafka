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

package org.apache.jena.fuseki.kafka.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.*;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.sparql.util.Convert;

/** Reduced HttpServletRequest */
public class HttpServletRequestMinimal implements HttpServletRequest {

    private final String requestURI;
    private final Map<String, String> headers;
    private final Map<String, String> parameters;
    private final InputStream inputStream;
    private final ServletContext servletContext;

    public HttpServletRequestMinimal(String requestURI, Map<String, String> headers, Map<String, String> parameters,
                                     InputStream inputStream, ServletContext servletContext) {
        this.requestURI = requestURI;
        this.headers = headers;
        this.parameters = parameters;
        this.inputStream = inputStream;
        this.servletContext = servletContext;
    }

    // ---- Headers

    @Override
    public String getCharacterEncoding() {
        // Workaround for jena 4.7.0 (getCharacterEncoding must be LC for RDF patch).
        return "UTF-8".toLowerCase();
    }

    @Override
    public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
        //throw new UnsupportedOperationException();
    }

    @Override
    public int getContentLength() {
        String x = getHeader(HttpNames.hContentLength);
        if ( x == null )
            return -1;
        try {
            return Integer.parseInt(x);
        } catch (NumberFormatException ex) { return -1; }
    }

    @Override
    public long getContentLengthLong() {
        String x = getHeader(HttpNames.hContentLength);
        if ( x == null )
            return -1;
        try {
            return Long.parseLong(x);
        } catch (NumberFormatException ex) { return -1; }
    }

    @Override
    public String getContentType() {
        return getHeader(HttpNames.hContentType);
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        return new ServletInputStream() {
            @Override
            public int read() throws IOException { return inputStream.read(); }

            @Override
            public int read(byte[] dst, int off, int len) throws IOException {return inputStream.read(dst, off, len); }

            @Override
            public boolean isFinished() {
                return ! isReady();
            }

            @Override
            public boolean isReady() {
                try {
                    return inputStream.available() > 0 ;
                } catch (IOException ex) { return false; }
            }

            @Override
            public void setReadListener(ReadListener readListener) {
                throw new UnsupportedOperationException();
            }
        };
    }

    // ---- Parameters

    @Override
    public String getQueryString() {
        if ( parameters.isEmpty() )
            return null;

        StringJoiner sj = new StringJoiner("&", "?", "");
        parameters.forEach((k,v)->{
            String xk = Convert.encWWWForm(k);
            if ( v == null ) {
                sj.add(xk);
            } else {
                String xv = Convert.encWWWForm(v);
                sj.add(xk+"="+xv);
            }
        });
        return sj.toString();
    }

    @Override
    public String getParameter(String name) {
        return parameters.get(name);
    }

    @Override
    public Enumeration<String> getParameterNames() {
        return Collections.enumeration(parameters.keySet());
    }

    @Override
    public String[] getParameterValues(String name) {
        String v = getParameter(name);
        if ( v == null )
            return null;
        return new String[]{v};
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        HashMap<String, String[]> x = new HashMap<>();
        parameters.forEach((key,value)->x.put(key, new String[]{value}));
        return x;
    }

    // ---- Protocol

    @Override
    public String getProtocol() {
        return "kafka/1.0";
    }

    @Override
    public String getScheme() {
        return "kafka";
    }

    // ---- Server, Remote, Local

    @Override
    public String getRequestURI() {
        return requestURI;
    }

    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer("kafka-http://here"+getRequestURI());
    }

    @Override
    public String getServletPath() {
        return "";
    }

    @Override
    public String getMethod() {
        return HttpNames.METHOD_POST;
    }

    @Override
    public String getPathInfo() {
        return null;
    }

    /**
     * Returns any extra path information after the servlet name
     * but before the query string, and translates it to a real
     * path. Same as the value of the CGI variable PATH_TRANSLATED.
     */
    @Override
    public String getPathTranslated() {
        return null;
    }

    /**
     * Returns the portion of the request URI that indicates the context
     * of the request. The context path always comes first in a request
     * URI. The path starts with a "/" character but does not end with a "/"
     * character. For servlets in the default (root) context, this method
     * returns "". The container does not decode this string.
     */
    @Override
    public String getContextPath() {
        return null;
    }

    /**
     * Returns the host name of the server to which the request was sent.
     * It is the value of the part before ":" in the <code>Host</code>
     * header value, if any, or the resolved server name, or the server IP
     * address.
     */
    @Override
    public String getServerName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getServerPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteAddr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteHost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRemotePort() {
        return 0;
    }

    @Override
    public String getLocalName() {
        return null;
    }

    @Override
    public String getLocalAddr() {
        return null;
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    /**
     * Retrieves the body of the request as character data using
     * a <code>BufferedReader</code>.  The reader translates the character
     * data according to the character encoding used on the body.
     * Either this method or {@link #getInputStream} may be called to read the
     * body, not both.
     */

    @Override
    public BufferedReader getReader() throws IOException {
        // Discouraged.
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the preferred <code>Locale</code> that the client will
     * accept content in, based on the Accept-Language header.
     */
    @Override
    public Locale getLocale() {
        return null;
    }

    @Override
    public Enumeration<Locale> getLocales() {
        return null;
    }

    // ---- User and Auth

    @Override
    public String getRemoteUser() {
        return null;
    }

    @Override
    public boolean isUserInRole(String role) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Principal getUserPrincipal() {
        return null;
    }

    /**
     * Returns a boolean indicating whether this request was made using a
     * secure channel, such as HTTPS.
     */
    @Override
    public boolean isSecure() {
        return false;
    }

    // -- Attributes

    /**
     * Returns the value of the named attribute as an <code>Object</code>,
     * or <code>null</code> if no attribute of the given name exists.
     */
    @Override
    public Object getAttribute(String name) {
        return null;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return null;
    }

    @Override
    public void setAttribute(String name, Object o) {}

    @Override
    public void removeAttribute(String name) {}

    // ---- Other

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DispatcherType getDispatcherType() {
        return null;
    }

    @Deprecated
    @Override
    public String getRealPath(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServletContext getServletContext() {
        return servletContext;
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncStarted() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncSupported() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext getAsyncContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAuthType() {
        return null;
    }

    @Override
    public long getDateHeader(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHeader(String name) {
        return headers.get(name);
    }

    @Override
    public Enumeration<String> getHeaders(String name) {

        String v = headers.get(name);
        if ( v == null )
            return Collections.enumeration(List.of());
        return Collections.enumeration(List.of(v));
    }

    @Override
    public Enumeration<String> getHeaderNames() {
        return Collections.enumeration(headers.keySet());
    }

    @Override
    public int getIntHeader(String name) {
        return 0;
    }

    // ---- Unsupported.
    // Cookies, Sessions, Multi-part

    @Override
    public Cookie[] getCookies() {
        return null;
    }

    @Override
    public String getRequestedSessionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSession getSession(boolean create) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSession getSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String changeSessionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void login(String username, String password) throws ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void logout() throws ServletException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

}
