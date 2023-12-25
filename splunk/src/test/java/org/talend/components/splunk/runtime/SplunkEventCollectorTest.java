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
package org.talend.components.splunk.runtime;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.service.I18n;
import org.talend.components.http.service.RecordBuilderService;
import org.talend.components.http.service.httpClient.HTTPClientService;
import org.talend.components.splunk.dataset.SplunkDataset;
import org.talend.components.splunk.datastore.SplunkDatastore;
import org.talend.components.splunk.service.SplunkMessages;
import org.talend.sdk.component.api.context.RuntimeContext;
import org.talend.sdk.component.api.context.RuntimeContextHolder;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.splunk")
class SplunkEventCollectorTest {

    private HTTPClientService clientService;

    private SplunkEventCollector eventCollector;

    private SplunkEventCollectorProperties properties;

    @Service
    private I18n i18n;

    private SplunkMessages mockedSplunkI18N;

    @Service
    RecordBuilderService recordBuilderService;

    @BeforeEach
    void setUp() {
        mockedSplunkI18N = Mockito.mock();

        SplunkDatastore datastore = new SplunkDatastore();
        datastore.setServerURL("http://someFakeUrl.com:1111");
        datastore.setToken("someToken");
        SplunkDataset dataset = new SplunkDataset();
        dataset.setDatastore(datastore);
        properties = new SplunkEventCollectorProperties();
        properties.setDataset(dataset);

        clientService = Mockito.mock(HTTPClientService.class);
    }

    @Test
    void testOutputThreeRecordsInOneBatch() throws HTTPClientException {
        prepareClientServiceMockReturnOk();
        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N);

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("event", "value")
                .build();

        eventCollector.startBulk();
        for (int i = 0; i < 3; i++) {
            eventCollector.process(record);
        }

        eventCollector.processBulk();
        Mockito.verify(clientService, Mockito.times(1))
                .invoke(Mockito.any(), Mockito.eq(false));
    }

    @Test
    void testOutputOneRecordFailing() throws HTTPClientException {
        prepareClientServiceMockReturnFailure();
        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N);

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("event", "value")
                .build();
        eventCollector.startBulk();
        eventCollector.process(record);
        Assertions.assertThrows(ComponentException.class, () -> eventCollector.processBulk());
    }

    @Test
    void testAfterVariable() throws HTTPClientException {
        prepareClientServiceMockReturnOk();

        RuntimeContextHolder contextHolder = new RuntimeContextHolder("SplunkEventCollector", new HashMap<>());
        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N);
        Arrays.stream(eventCollector.getClass().getDeclaredFields())
                .filter(field -> Arrays.stream(field.getDeclaredAnnotations())
                        .anyMatch(annotation -> annotation.annotationType().equals(RuntimeContext.class)))
                .findFirst()
                .ifPresent(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(eventCollector, contextHolder);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("event", "value")
                .build();

        eventCollector.startBulk();
        // OnNext
        eventCollector.process(record);
        Mockito.verifyNoInteractions(clientService);
        // AfterGroup
        eventCollector.processBulk();
        // AfterVariables
        eventCollector.finish();
        Assertions.assertEquals(1, contextHolder.getMap().size());
        Assertions.assertEquals(999, contextHolder.getMap().get("SplunkEventCollector_RESPONSE_CODE"));
        Mockito.verify(clientService, Mockito.times(1))
                .invoke(Mockito.any(), Mockito.eq(false));
        Mockito.verify(clientService, Mockito.times(1))
                .convertConfiguration(Mockito.any(), Mockito.any());
    }

    @Test
    void testNullValuesInMetadataIgnored() throws HTTPClientException {
        final AtomicReference<RequestConfig> requestConfigReference = new AtomicReference<>();
        clientService = Mockito.spy(HTTPClientService.class);

        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("event", "some event")
                .withString(SplunkMetadataFields.HOST.getName(), null)
                .withString(SplunkMetadataFields.INDEX.getName(), null)
                .withString(SplunkMetadataFields.SOURCE.getName(), null)
                .withString(SplunkMetadataFields.SOURCE_TYPE.getName(), null)
                .withDateTime(SplunkMetadataFields.TIME.getName(), (ZonedDateTime) null)
                .build();

        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N) {

            @Override
            protected RequestConfig getConfig() {
                requestConfigReference.set(super.getConfig());

                return requestConfigReference.get();
            }
        };

        eventCollector.startBulk();
        eventCollector.process(record);
        eventCollector.processBulk();
        Assertions.assertEquals("{\"event\":{\"event\":\"some event\"}}",
                requestConfigReference.get().getDataset().getBody().getJsonValue().trim());
    }

    @Test
    void testCreateEventFromRecord() throws HTTPClientException {
        ZonedDateTime expectedDateTime =
                ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 0,
                        ZoneId.of("Z"));
        String expectedString = "some value1";
        final AtomicReference<RequestConfig> requestConfigReference = new AtomicReference<>();
        clientService = Mockito.spy(HTTPClientService.class);

        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("col1", expectedString)
                .withString("col2", expectedString)
                .withInt("intCol3", 9999)
                .withLong("longCol4", Long.MAX_VALUE)
                .withDouble("doubleCol5", 0.01d)
                .withFloat("floatCol6", 1f)
                .withBoolean("booleanCol7", true)
                .withBytes("bytesCol8", new byte[] { 1, 2, 3 })
                .withDateTime("dateCol9", expectedDateTime)
                .withString(SplunkMetadataFields.INDEX.getName(), "someIndex")
                .withDateTime(SplunkMetadataFields.TIME.getName(), // test to set time as Date instead of ZonedDateTime
                        new Date(expectedDateTime.toInstant().toEpochMilli()))
                .build();

        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N) {

            @Override
            protected RequestConfig getConfig() {
                requestConfigReference.set(super.getConfig());

                return requestConfigReference.get();
            }
        };

        eventCollector.startBulk();
        eventCollector.process(record);
        eventCollector.processBulk();

        Assertions.assertEquals("{\"event\":{"
                + "\"col1\":\"" + expectedString + "\","
                + "\"col2\":\"" + expectedString + "\","
                + "\"intCol3\":9999,"
                + "\"longCol4\":" + Long.MAX_VALUE + ","
                + "\"doubleCol5\":0.01,"
                + "\"floatCol6\":1.0,"
                + "\"booleanCol7\":true,"
                + "\"bytesCol8\":[1,2,3],"
                + "\"dateCol9\":" + expectedDateTime.toEpochSecond()
                + "},"
                + "\"index\":\"someIndex\","
                + "\"time\":\"" + expectedDateTime.toEpochSecond() + ".000\"}",
                requestConfigReference.get().getDataset().getBody().getJsonValue().trim());
    }

    @Test
    void testTimeAsLong() throws HTTPClientException {
        final long expectedLong = 1628197200000L;
        final AtomicReference<RequestConfig> requestConfigReference = new AtomicReference<>();
        clientService = Mockito.spy(HTTPClientService.class);

        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withLong(SplunkMetadataFields.TIME.getName(), expectedLong)
                .withString("col1", "event1")
                .build();

        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N) {

            @Override
            protected RequestConfig getConfig() {
                requestConfigReference.set(super.getConfig());

                return requestConfigReference.get();
            }
        };

        eventCollector.startBulk();
        eventCollector.process(record);
        eventCollector.processBulk();

        Assertions.assertEquals("{\"event\":{"
                + "\"col1\":\"event1\"},"
                + "\"time\":\"" + expectedLong / 1000 + ".000\"}",
                requestConfigReference.get().getDataset().getBody().getJsonValue().trim());
    }

    @Test
    void testTimeAsBigDecimal() throws HTTPClientException {
        final long expectedTime = 1628197200000L;

        final AtomicReference<RequestConfig> requestConfigReference = new AtomicReference<>();
        clientService = Mockito.spy(HTTPClientService.class);

        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withDecimal(SplunkMetadataFields.TIME.getName(), new BigDecimal(expectedTime))
                .withString("col1", "event1")
                .build();

        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N) {

            @Override
            protected RequestConfig getConfig() {
                requestConfigReference.set(super.getConfig());

                return requestConfigReference.get();
            }
        };

        eventCollector.startBulk();
        eventCollector.process(record);
        eventCollector.processBulk();

        Assertions.assertEquals("{\"event\":{"
                + "\"col1\":\"event1\"},"
                + "\"time\":\"" + expectedTime / 1000 + ".000\"}",
                requestConfigReference.get().getDataset().getBody().getJsonValue().trim());
    }

    @Test
    void testTimeAsString() throws HTTPClientException {
        final long expectedTime = 1628197200000L;

        final AtomicReference<RequestConfig> requestConfigReference = new AtomicReference<>();
        clientService = Mockito.spy(HTTPClientService.class);

        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));

        Record record = recordBuilderService.getRecordBuilderFactory()
                .newRecordBuilder()
                .withString(SplunkMetadataFields.TIME.getName(), String.valueOf(expectedTime))
                .withString("col1", "event1")
                .build();

        eventCollector = new SplunkEventCollector(properties, clientService, i18n, mockedSplunkI18N) {

            @Override
            protected RequestConfig getConfig() {
                requestConfigReference.set(super.getConfig());

                return requestConfigReference.get();
            }
        };

        eventCollector.startBulk();
        eventCollector.process(record);
        eventCollector.processBulk();

        Assertions.assertEquals("{\"event\":{"
                + "\"col1\":\"event1\"},"
                + "\"time\":\"" + expectedTime / 1000 + ".000\"}",
                requestConfigReference.get().getDataset().getBody().getJsonValue().trim());
    }

    private void prepareClientServiceMockReturnOk() throws HTTPClientException {
        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(200);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.getBodyAsString()).thenReturn("{\"text\":\"SomeText\",\"code\":999}");
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));
    }

    private void prepareClientServiceMockReturnFailure() throws HTTPClientException {
        HTTPClient.HTTPResponse mockedResponse = Mockito.mock();
        HTTPClient.Status mockedStatus = Mockito.mock();
        Mockito.when(mockedStatus.getCode()).thenReturn(401);
        Mockito.when(mockedResponse.getStatus()).thenReturn(mockedStatus);
        Mockito.when(mockedResponse.nextPageQueryConfiguration()).thenReturn(Optional.empty());
        Mockito.doReturn(mockedResponse).when(clientService).invoke(Mockito.any(), Mockito.eq(false));
    }
}