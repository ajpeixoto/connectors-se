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
package org.talend.components.http.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.common.httpclient.api.HTTPMethod;
import org.talend.components.http.configuration.Format;
import org.talend.components.http.configuration.OutputContent;
import org.talend.components.http.configuration.Param;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@WithComponents(value = "org.talend.components.http")
public class UIServiceTest {

    private final static List<Param> extractedValues =
            Arrays.asList(new Param("one", "1"),
                    new Param("two", "2"),
                    new Param("three", "3"));

    @Service
    UIService service;

    private Schema incomingSchema;

    @Test
    void testHTTPMethodsSupported() {
        Collection<SuggestionValues.Item> supportedMethods = service.getHTTPMethodList().getItems();
        Assertions.assertEquals(supportedMethods.size(), HTTPMethod.values().length,
                "Supported HTTP method count doesn't match the API");
        supportedMethods.forEach(item -> Assertions.assertDoesNotThrow(() -> HTTPMethod.valueOf(item.getId())));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "false,false,RAW_TEXT,BODY_ONLY",
            "false,false,RAW_TEXT,STATUS_HEADERS_BODY",
            "false,false,JSON,BODY_ONLY",
            "false,false,JSON,STATUS_HEADERS_BODY",
            "false,true,RAW_TEXT,BODY_ONLY",
            "false,true,RAW_TEXT,STATUS_HEADERS_BODY",
            "false,true,JSON,BODY_ONLY",
            "false,true,JSON,STATUS_HEADERS_BODY",
            "true,true,RAW_TEXT,BODY_ONLY",
            "true,true,RAW_TEXT,STATUS_HEADERS_BODY",
            "true,true,JSON,BODY_ONLY",
            "true,true,JSON,STATUS_HEADERS_BODY",
            "true,false,RAW_TEXT,BODY_ONLY",
            "true,false,RAW_TEXT,STATUS_HEADERS_BODY",
            "true,false,JSON,BODY_ONLY",
            "true,false,JSON,STATUS_HEADERS_BODY",
    })
    void simpleDiscoverSchemaExtended(boolean outputKeyValuePairs, boolean forwardInputSchema, final Format bodyFormat,
            final OutputContent returnedContent) {
        final RequestConfig processorConfig = RequestConfigBuilderTest.getEmptyProcessorRequestConfig();
        processorConfig.getDataset().setFormat(bodyFormat);
        processorConfig.getDataset().setReturnedContent(returnedContent);

        processorConfig.getDataset().setOutputKeyValuePairs(outputKeyValuePairs);
        processorConfig.getDataset().setForwardInput(forwardInputSchema);
        processorConfig.getDataset().setKeyValuePairs(extractedValues);

        incomingSchema = service.getRecordBuilderService()
                .getRecordBuilderFactory()
                .newRecordBuilder()
                .withString("A_STRING", "aaa")
                .withInt("A_INTEGER", 1)
                .withBoolean("A_BOOLEAN", true)
                .build()
                .getSchema();

        Supplier<Schema> supplier = () -> service.discoverSchemaExtended(incomingSchema, processorConfig, null);
        _discoverSchemaTest(outputKeyValuePairs, forwardInputSchema, bodyFormat, returnedContent, supplier);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "RAW_TEXT,BODY_ONLY",
            "RAW_TEXT,STATUS_HEADERS_BODY",
            "JSON,BODY_ONLY",
            "JSON,STATUS_HEADERS_BODY",
    })
    void discoverSchema(Format bodyFormat, OutputContent returnedContent) {
        final RequestConfig inputConfig = RequestConfigBuilderTest.getEmptyRequestConfig();
        inputConfig.getDataset().setFormat(bodyFormat);
        inputConfig.getDataset().setReturnedContent(returnedContent);
        Supplier<Schema> supplier = () -> service.discoverSchema(inputConfig.getDataset());
        _discoverSchemaTest(false, false, bodyFormat, returnedContent, supplier);
    }

    private void _discoverSchemaTest(boolean outputKeyValuePairs, boolean forwardInputSchema, final Format bodyFormat,
            final OutputContent returnedContent, Supplier<Schema> discoverSchema) {
        if (outputKeyValuePairs) {
            Schema schema = discoverSchema.get();
            Map<String, Schema.Entry> collect =
                    schema.getEntries().stream().collect(Collectors.toMap(e -> e.getName(), e -> e));

            Map<String, Schema.Entry> expected =
                    extractedValues.stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> service.getRecordBuilderService()
                                    .getRecordBuilderFactory()
                                    .newEntryBuilder()
                                    .withName(e.getKey())
                                    .withType(Schema.Type.STRING)
                                    .withNullable(true)
                                    .build()));

            if (forwardInputSchema) {
                incomingSchema.getEntries().stream().forEach(e -> expected.put(e.getName(), e));
            }
            Assertions.assertEquals(expected.size(), collect.size());
            expected.entrySet().stream().forEach(e -> {
                Schema.Entry entry = collect.remove(e.getValue().getName());
                Assertions.assertNotNull(entry);
                Assertions.assertEquals(e.getValue().getType(), entry.getType());
                Assertions.assertEquals(e.getValue().isNullable(), entry.isNullable());
            });
            Assertions.assertEquals(0, collect.size());
        } else if (bodyFormat == Format.JSON && returnedContent == OutputContent.BODY_ONLY) {
            // Schema can't be infered without HTTP call (and we don't want it) in that case.
            Assertions.assertThrows(ComponentException.class, () -> {
                discoverSchema.get();
            });

        } else {
            Schema schema = discoverSchema.get();
            Assertions.assertNotNull(schema);

            Map<String, Schema.Entry> entries =
                    schema.getEntries().stream().collect(Collectors.toMap(e -> e.getName(), e -> e));

            // Always have body:string attribute
            Assertions.assertEquals(Schema.Type.STRING, entries.get("body").getType());
            Assertions.assertEquals(true, entries.get("body").isNullable());

            // headers & status only if STATUS_HEADERS_BODY
            if (returnedContent == OutputContent.STATUS_HEADERS_BODY) {
                Assertions.assertEquals(3, entries.size());
                Assertions.assertEquals(Schema.Type.STRING, entries.get("headers").getType());
                Assertions.assertEquals(true, entries.get("headers").isNullable());
                Assertions.assertEquals(Schema.Type.INT, entries.get("status").getType());
                Assertions.assertEquals(false, entries.get("status").isNullable());
            }
        }
    }

}
