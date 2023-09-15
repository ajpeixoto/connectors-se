/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.httpclient.impl.cxf.servers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.cxf.helpers.HttpHeaderHelper;
import org.talend.components.common.httpclient.api.pagination.PaginationParametersLocation;
import org.talend.components.common.httpclient.impl.cxf.ResourcesUtils;
import org.talend.components.common.httpclient.impl.cxf.servers.handlers.BasicOAuthClientCredentialHandler;
import org.talend.components.common.httpclient.impl.cxf.servers.handlers.FormOAuthClientCredentialHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicHTTPServerFactory implements AbstractHTTPServerFactory<BasicHTTPServerFactory.JVMTestHTTPServer> {

    public final static String HELLO_WORLD = "Hello world!";

    public final static String EXPECTED_TOKEN_KEY = "expected_token";

    public final static String HTTP_SIMPLE = "/simple";

    public final static String HTTP_ECHO = "/echo";

    public final static String HTTP_REDIRECT = "/redirect";

    public final static String HTTP_REDIRECT_CONFIGURE_HOST = "/redirect_configure_host";

    public final static String HTTP_OAUTH_CLIENT_CREDENTIALS_FORM_TOKEN = "/oauth/client_credentials/form/token";

    public final static String HTTP_OAUTH_CLIENT_CREDENTIALS_BASIC_TOKEN = "/oauth/client_credentials/basic/token";

    public final static String HTTP_OAUTH_RESOURCE = "/oauth/resource";

    public final static String HTTP_DELAYED_RESPONSE = "/delay";

    public final static String HTTP_GZIP = "/gzip";

    public final static String HTTP_PAGINATION_OFFSET_LIMIT = "/pagination/offsetlimit/";

    public final static String HTTP_OAUTH_CLIENT_CREDENTIALS_TOKEN_CLIENT_ID = "1234567890";

    public final static String HTTP_OAUTH_CLIENT_CREDENTIALS_TOKEN_CLIENT_SECRET = "SECRET1234567890SECRET";

    public final static String HTTP_PAGINATION_OFFSET_LIMIT_OFFSETNAME = "offsetParam";

    public final static String HTTP_PAGINATION_OFFSET_LIMIT_LIMITNAME = "limitParam";

    public final static String HTTP_PAGINATION_OFFSET_LIMIT_ELEMENTS = "elts";

    public final static String HTTP_PAGINATION_OFFSET_LIMIT_LOCALISATION = "confLocalisation";

    private static BasicHTTPServerFactory instance;

    private BasicHTTPServerFactory() {
        /** Don't instantiate **/
    }

    public static synchronized BasicHTTPServerFactory getInstance() {
        if (instance == null) {
            instance = new BasicHTTPServerFactory();
        }

        return instance;
    }

    @Override
    public JVMTestHTTPServer createServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            int port = server.getAddress().getPort();

            configureServer(server);

            return new JVMTestHTTPServer(server, port);
        } catch (IOException e) {
            log.error(String.format("Can't start the test HTTP server from %s : %s",
                    BasicHTTPServerFactory.class.getName(), e.getMessage()));
            throw new RuntimeException(e);
        }
    }

    private static void configureServer(HttpServer server) {
        simpleContext(server);
        echoContext(server);
        redirectSameURL(server);
        redirectConfiguringHost(server);
        delayContext(server);
        oauth20ClientCredentialFormAuthentToken(server);
        oauth20ClientCredentialBasicAuthentToken(server);
        oauth20ClientCredentialResource(server);
        gzippedPayload(server);
        paginationOffsetLimit(server);
    }

    private static void paginationOffsetLimit(HttpServer server) {
        server.createContext(HTTP_PAGINATION_OFFSET_LIMIT, new HttpHandler() {

            private JsonArray elements = null;

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (elements == null) {
                    String json = ResourcesUtils.loadResource("/responses/longListOfElements.json");
                    JsonObject jsonObject = ResourcesUtils.getJsonObject(json);
                    elements = jsonObject.getJsonArray("elements");
                }

                Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());

                String confLocalisation = params.getOrDefault(HTTP_PAGINATION_OFFSET_LIMIT_LOCALISATION,
                        PaginationParametersLocation.QUERY_PARAMETERS.name());

                String offsetValue;
                String limitValue;
                if (PaginationParametersLocation.QUERY_PARAMETERS == PaginationParametersLocation
                        .valueOf(confLocalisation)) {
                    offsetValue = params.get(HTTP_PAGINATION_OFFSET_LIMIT_OFFSETNAME);
                    limitValue = params.get(HTTP_PAGINATION_OFFSET_LIMIT_LIMITNAME);
                } else {
                    offsetValue = exchange.getRequestHeaders().get(HTTP_PAGINATION_OFFSET_LIMIT_OFFSETNAME).get(0);
                    limitValue = exchange.getRequestHeaders().get(HTTP_PAGINATION_OFFSET_LIMIT_LIMITNAME).get(0);
                }

                int iOffsetValue = Integer.parseInt(offsetValue);
                int iLimitValue = Integer.parseInt(limitValue);

                JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder()
                        .add(HTTP_PAGINATION_OFFSET_LIMIT_OFFSETNAME, iOffsetValue)
                        .add(HTTP_PAGINATION_OFFSET_LIMIT_LIMITNAME, iLimitValue);

                JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                for (int i = iOffsetValue; i < (iOffsetValue + iLimitValue) && i < elements.size(); i++) {
                    arrayBuilder.add(elements.get(i));
                }

                JsonObject payload = jsonObjectBuilder.add(HTTP_PAGINATION_OFFSET_LIMIT_ELEMENTS, arrayBuilder.build())
                        .build();
                byte[] bPayload = payload.toString().getBytes("UTF-8");

                int status = HttpURLConnection.HTTP_OK;

                exchange.sendResponseHeaders(status, bPayload.length);
                OutputStream os = exchange.getResponseBody();
                os.write(bPayload);
                os.close();
            }
        });
    }

    private static void gzippedPayload(HttpServer server) {
        server.createContext(HTTP_GZIP, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                int status = HttpURLConnection.HTTP_OK;

                exchange.getResponseHeaders().add("Content-encoding", "gzip");

                String body = HELLO_WORLD;
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(byteArrayOutputStream);
                gzip.write(body.getBytes("UTF-8"));
                gzip.flush();
                gzip.close();
                byte[] bb = byteArrayOutputStream.toByteArray();

                exchange.sendResponseHeaders(status, bb.length);
                OutputStream os = exchange.getResponseBody();
                os.write(bb);
                os.close();
            }
        });

    }

    private static void oauth20ClientCredentialFormAuthentToken(HttpServer server) {
        server.createContext(HTTP_OAUTH_CLIENT_CREDENTIALS_FORM_TOKEN, new FormOAuthClientCredentialHandler());
    }

    private static void oauth20ClientCredentialBasicAuthentToken(HttpServer server) {
        server.createContext(HTTP_OAUTH_CLIENT_CREDENTIALS_BASIC_TOKEN, new BasicOAuthClientCredentialHandler());
    }

    private static void oauth20ClientCredentialResource(HttpServer server) {
        server.createContext(HTTP_OAUTH_RESOURCE, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                JsonObject query = exchange2Json(exchange);

                Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());

                int status = HttpURLConnection.HTTP_UNAUTHORIZED;
                String expected = params.getOrDefault(EXPECTED_TOKEN_KEY, "Bearer a-valide-token");

                String authorizationKey = HttpHeaderHelper.AUTHORIZATION.toLowerCase();
                boolean hasAuthorization = query.getJsonObject("request-headers").containsKey(authorizationKey);
                if (hasAuthorization) {
                    String authorization = query.getJsonObject("request-headers").getString(authorizationKey);
                    if (expected.equals(authorization)) {
                        status = HttpURLConnection.HTTP_OK;
                    }
                }

                exchange.sendResponseHeaders(status, 0);
            }
        });
    }

    private static void redirectConfiguringHost(HttpServer server) {
        server.createContext(HTTP_REDIRECT_CONFIGURE_HOST, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
                String to = params.get("to");

                exchange.getResponseHeaders().add("Location", to);

                exchange.sendResponseHeaders(HttpURLConnection.HTTP_MOVED_PERM, 0);
                OutputStream os = exchange.getResponseBody();
                os.close();
            }
        });
    }

    private static void redirectSameURL(HttpServer server) {
        server.createContext(HTTP_REDIRECT, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
                String nbRedirect = params.get("nb_redirect");
                String dec = params.get("dec");

                log.debug(String.format("Redirect endpoint with n=%s", nbRedirect));

                int status = HttpURLConnection.HTTP_BAD_REQUEST;
                if (nbRedirect != null) {
                    int currentNbRedirect = Integer.parseInt(nbRedirect);

                    int newNbRedirect = currentNbRedirect;
                    if (dec == null || "true".equals(dec)) {
                        newNbRedirect = currentNbRedirect - 1;
                    }

                    if (newNbRedirect > 0) {
                        status = HttpURLConnection.HTTP_MOVED_PERM;
                        String replace = exchange.getRequestURI()
                                .toString()
                                .replace("nb_redirect=" + currentNbRedirect, "nb_redirect=" + newNbRedirect);
                        log.debug(String.format("Redirection: %s", replace));
                        exchange.getResponseHeaders().add("Location", replace);
                    } else {
                        status = HttpURLConnection.HTTP_OK;
                    }
                }

                exchange.sendResponseHeaders(status, 0);
                OutputStream os = exchange.getResponseBody();
                os.close();
            }
        });
    }

    private static void echoContext(HttpServer server) {
        server.createContext(HTTP_ECHO, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {

                JsonObject json = exchange2Json(exchange);

                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                OutputStream os = exchange.getResponseBody();
                os.write(json.toString().getBytes(StandardCharsets.UTF_8));
                os.close();
            }
        });
    }

    /**
     * This endpoint allows to test timeout.
     *
     * @param server
     */
    private static void delayContext(HttpServer server) {
        server.createContext(HTTP_DELAYED_RESPONSE, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
                String delay = params.get("delay");
                log.debug(String.format("%s with delay=%s", HTTP_DELAYED_RESPONSE, delay));

                try {
                    TimeUnit.MILLISECONDS.sleep(Long.parseLong(delay));
                } catch (InterruptedException e) {
                    log.warn(String.format("Exception while HTTP server delay: %s", e.getMessage()));
                    throw new RuntimeException(e);
                }

                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                OutputStream os = exchange.getResponseBody();
                os.write("OK".getBytes(StandardCharsets.UTF_8));
                os.close();
            }
        });
    }

    private static void simpleContext(HttpServer server) {
        server.createContext(HTTP_SIMPLE, new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String content = ResourcesUtils.loadResource("/responses/simple.json");

                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                OutputStream os = exchange.getResponseBody();
                os.write(content.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
        });
    }

    public static JsonObject exchange2Json(final HttpExchange exchange) {
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

        // I force headers name lowercase since I sent Content-Type in the cxf WebClient
        // And the query headers here was Content-type without any explanation.
        Map<String, String> headersFlat = exchange.getRequestHeaders()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), e -> {
                    return e.getValue().stream().collect(Collectors.joining(", "));
                }));
        JsonObjectBuilder jsonHeaders = Json.createObjectBuilder();
        headersFlat.entrySet().stream().forEach(h -> {
            jsonHeaders.add(h.getKey(), h.getValue());
        });
        jsonBuilder.add("request-headers", jsonHeaders.build());

        String requestBody = ResourcesUtils.getString(exchange.getRequestBody());
        jsonBuilder.add("request-body", requestBody);

        return jsonBuilder.build();
    }

    public static Map<String, String> queryToMap(String query) {
        if (query == null) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (String param : query.split("&")) {
            String[] entry = param.split("=");
            if (entry.length > 1) {
                String decoded = null;
                try {
                    decoded = java.net.URLDecoder.decode(entry[1], StandardCharsets.UTF_8.name());
                } catch (UnsupportedEncodingException e) {
                    decoded = "Fail to decode";
                }
                result.put(entry[0], decoded);
            } else {
                result.put(entry[0], "");
            }
        }
        return result;
    }

    public static Map<String, String> formToMap(final HttpExchange exchange) {
        String requestBody = ResourcesUtils.getString(exchange.getRequestBody());
        return queryToMap(requestBody);
    }

    @AllArgsConstructor
    public static class JVMTestHTTPServer implements TestHTTPServer<HttpServer> {

        private HttpServer httpServer;

        private int port;

        @Override
        public void start() {
            this.httpServer.start();
            log.debug(String.format("TestHTTPServer started on port '%s'", port));
        }

        @Override
        public void stop() {
            this.httpServer.stop(0);
            log.debug("TestHTTPServer stopped.");
        }

        @Override
        public HttpServer getHttpServer() {
            return this.httpServer;
        }

        @Override
        public int getPort() {
            return this.port;
        }
    }

}
