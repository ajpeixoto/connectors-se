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
package org.talend.components.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public abstract class AbstractProcessorTest {

    protected final static int NB_RECORDS = 10;

    private HttpServer server;

    private int port;

    @Injected
    private ComponentsHandler handler;

    private RequestConfig config;

    @AfterEach
    void after() {
        // stop server
        server.stop(0);
    }

    protected void setServerContextAndStart(HttpHandler handler) {
        server.createContext("/", handler);
        server.start();
    }

    @BeforeEach
    void beforeEach() throws IOException {
        // Inject needed services
        // handler.injectServices(this);

        // start server
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();

        this.config = this.buildConfig();

    }

    protected RequestConfig getConfig() {
        return this.config;
    }

    protected abstract RequestConfig buildConfig();

    protected HttpServer getServer() {
        return this.server;
    }

    protected int getPort() {
        return this.port;
    }

    protected ComponentsHandler getHandler() {
        return this.handler;
    }

    protected List<Record> createData(int n) {
        RecordBuilderFactory factory = this.getHandler().findService(RecordBuilderFactory.class);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            records
                    .add(factory
                            .newRecordBuilder()
                            .withInt("id", i)
                            .withRecord("pagination",
                                    factory
                                            .newRecordBuilder()
                                            .withInt("page", 10 + i)
                                            .withInt("total", 100 + i)
                                            .build())
                            .withString("module", "module_" + i)
                            .withString("user_name", "<user> user_" + i + " /<user>")
                            .withRecord("book", factory
                                    .newRecordBuilder()
                                    .withString("title", "Title_" + i)
                                    .withRecord("market",
                                            factory.newRecordBuilder().withDouble("price", 1.35 * i).build())
                                    .withRecord("identification",
                                            factory
                                                    .newRecordBuilder()
                                                    .withInt("id", i)
                                                    .withString("isbn", "ISBN_" + i)
                                                    .build())
                                    .build())
                            .build());
        }

        return records;

    }

    protected List<Record> createDataWithJson(int n) {
        RecordBuilderFactory factory = this.getHandler().findService(RecordBuilderFactory.class);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {

            records
                    .add(factory
                            .newRecordBuilder()
                            .withInt("id", i)
                            .withString("user",
                                    "{\"name\": \"peter\", \"age\": 30, \"address\": {\"zipcode\": 44300, \"city\": \"Nantes\", \"altitude\": {\"min\": 2, \"max\": 53}}}")
                            .build());

        }

        return records;
    }

    protected List<Record> createOneEmptyRecord() {
        RecordBuilderFactory factory = this.getHandler().findService(RecordBuilderFactory.class);
        Record record = factory
                .newRecordBuilder()
                .build();
        return Collections.singletonList(record);
    }

}
