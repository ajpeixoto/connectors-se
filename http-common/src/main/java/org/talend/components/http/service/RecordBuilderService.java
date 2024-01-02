/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
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

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.json.stream.JsonParserFactory;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.common.httpclient.api.substitutor.Substitutor;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.components.common.stream.format.rawtext.RawTextConfiguration;
import org.talend.components.http.configuration.Format;
import org.talend.components.http.configuration.OutputContent;
import org.talend.components.http.configuration.Param;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.service.provider.DictionaryProvider;
import org.talend.components.http.service.provider.JsonContentProvider;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class RecordBuilderService {

    public final static String CONTEXT_SUBSTITUTOR_INPUT_OPENER = System
            .getProperty("org.talend.components.rest.context_substitutor_input_opener", "{");

    public final static String CONTEXT_SUBSTITUTOR_INPUT_PREFIX_KEY = System
            .getProperty("org.talend.components.rest.context_substitutor_input_prefix_key", ".input");

    public final static String CONTEXT_SUBSTITUTOR_INPUT_CLOSER = System
            .getProperty("org.talend.components.rest.context_substitutor_input_closer", "}");

    public final static String CONTEXT_SUBSTITUTOR_RESULT_OPENER = System
            .getProperty("org.talend.components.rest.context_substitutor_result_opener", "{");

    public final static String CONTEXT_SUBSTITUTOR_RESULT_PREFIX_KEY = System
            .getProperty("org.talend.components.rest.context_substitutor_result_prefix_key", ".response");

    public final static String CONTEXT_SUBSTITUTOR_RESULT_CLOSER = System
            .getProperty("org.talend.components.rest.context_substitutor_result_closer", "}");

    @Service
    private I18n i18n;

    @Service
    private AttachmentService attachmentService;

    @Service
    private RecordIORepository ioRepository;

    @Getter
    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private JsonParserFactory jsonParserFactory;

    public Iterator<Record> buildFixedRecord(final HTTPClient.HTTPResponse response,
            final RequestConfig config) {
        return buildFixedRecord(null, response, config);
    }

    /**
     * This methode generate an iterator that loop over all generated Record.
     * This will generate all the needs for the call to buildRecord(...)
     * 
     * @param input The input record used for substitution if key/value pairs output.
     * @param response The reponse of the current HTTP call.
     * @param config The HTTP call configuration.
     * @return An iterator over generated records.
     */
    public Iterator<Record> buildFixedRecord(final Record input, final HTTPClient.HTTPResponse response,
            final RequestConfig config) {

        final Format format = config.getDataset().getFormat();
        final boolean createContext = config.getDataset().isOutputKeyValuePairs();
        final boolean completeContext = config.getDataset().isForwardInput();
        final boolean isCompletePayload = config.getDataset().getReturnedContent() == OutputContent.STATUS_HEADERS_BODY;

        Map<String, String> headers = response.getHeaders();
        final ContentFormat contentFormat = findFormat(config);
        final RecordReaderSupplier recordReaderSupplier;
        try {
            recordReaderSupplier = this.ioRepository.findReader(contentFormat.getClass());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(i18n.readerNotFound(e.getMessage()), e);
        }
        final RecordReader reader = recordReaderSupplier
                .getReader(recordBuilderFactory, contentFormat,
                        (config.getDataset().getFormat() == Format.RAW_TEXT)
                                ? new ExtendedRawTextConfiguration(response.getEncoding(), isCompletePayload)
                                : null);

        final List<Record> headerRecords = headers
                .entrySet()
                .stream()
                .map(this::convertHeadersToRecords)
                .collect(Collectors.toList());

        final Iterator<Record> readIterator;

        String payload = null;
        try {
            // I read the payload in case of an exception to be able to display it in the message.
            payload = response.getBodyAsString();
            if (config.isDownloadFile()) {
                MediaType mediaType = getNestedResponseMediaType(response);
                if (attachmentService.isMultipart(mediaType)) {
                    String notAttachmentPartOfMultipart = attachmentService.parseMultipartAndDownloadAttachments(
                            mediaType.getType() + "/" + mediaType.getSubtype(), payload, config);

                    payload = notAttachmentPartOfMultipart;
                }
            }
            // Inject null in reader if null or, the ByteArrayInputStream if it contains something.
            readIterator = reader.read(payload == null ? null : new ByteArrayInputStream(payload.getBytes()));
        } catch (RuntimeException | HTTPClientException e) {
            int endSubstring = 100;
            payload = (payload == null || "".equals(payload)) ? i18n.emptyPayload() : payload.trim();
            // Display only first characters of the payload in the message
            endSubstring = endSubstring > payload.length() ? payload.length() : endSubstring;
            String partial = (endSubstring < payload.length()) ? "..." : "";
            String sFormat = "Unknown";
            switch (format) {
            case JSON:
                sFormat = i18n.formatJSON();
                break;
            case RAW_TEXT:
                sFormat = i18n.formatText();
                break;
            }
            throw new IllegalArgumentException(
                    i18n
                            .invalideBodyContent(
                                    sFormat,
                                    response.getStatus().getCodeWithReason(),
                                    payload == null ? "" : payload.substring(0, endSubstring) + partial,
                                    e.getMessage()),
                    e);
        }

        return new IteratorMap<Record, Record>(readIterator,
                r -> this.buildRecord(r, response.getStatus().getCode(), headerRecords, isCompletePayload,
                        createContext,
                        completeContext, format,
                        config.getDataset().getKeyValuePairs(), input),
                true);
    }

    private MediaType getNestedResponseMediaType(
            HTTPClient.HTTPResponse response) {
        return ((Response) response.getNestedResponse()).getMediaType();
    }

    /**
     *
     * @param body The record generated using the body of the HTTP response
     * @param status The HTTP response status.
     * @param headers The HTTP response headers
     * @param isCompletePayload Do we return (status, headers, body) or only (body)
     * @param buildContext Do we build a key/Value pairs with some extractions ?
     * @param completeContext Do we include incoming values to keep an existing incoming context ?
     * @param format If RAW_TEXT the body will be a String not a RECORD
     * @param context key/value pairs configuration
     * @param input The input record for substitution in key/value pair.
     * @return
     */
    private Record buildRecord(final Record body, final int status, final List<Record> headers,
            final boolean isCompletePayload, final boolean buildContext, final boolean completeContext,
            final Format format,
            final List<Param> context,
            final Record input) {

        final Record record = this.buildStandardRecord(body, status, headers, isCompletePayload, format);
        if (buildContext) {
            return this.buildExtractedKeyValuesRecord(completeContext, input, record, context);
        } else {
            return record;
        }
    }

    /**
     * Return a record that represent the content of the response.
     */
    private Record buildStandardRecord(final Record body, final int status, final List<Record> headers,
            final boolean isCompletePayload,
            final Format format) {

        final Record headersRecord = headersToRecord(headers);
        final Schema schema = buildSchema(body, headersRecord, isCompletePayload, format);

        final boolean isRawText = schema
                .getEntries()
                .stream()
                .filter(e -> "body".equals(e.getName()))
                .findFirst()
                .get()
                .getType() == Type.STRING;

        final Builder bodyBuilder;

        if (isRawText) {
            String content = body == null ? null : body.getString("content");
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema).withString("body", content);
            if (isCompletePayload) {
                bodyBuilder.withInt("status", status)
                        .withRecord("headers", headersRecord);
            }
            return bodyBuilder.build();
        } else if (isCompletePayload) {
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema);
            if (body != null) {
                bodyBuilder.withRecord("body", body);
            }
            return bodyBuilder.withInt("status", status)
                    .withRecord("headers", headersRecord)
                    .build();
        } else {
            return body;
        }
    }

    /**
     * Return a record that is a list of key/value pairs.
     */
    private Record buildExtractedKeyValuesRecord(final boolean completeContext, final Record input, final Record record,
            final List<Param> extractedValueList) {
        final Builder builder;

        if (input != null && completeContext) {
            final Schema.Builder schemaBuilder = this.recordBuilderFactory.newSchemaBuilder(input.getSchema());

            extractedValueList.stream()
                    .map(e -> this.recordBuilderFactory.newEntryBuilder()
                            .withType(Type.STRING)
                            .withName(e.getKey())
                            .withNullable(true)
                            .build())
                    .forEach(e -> schemaBuilder.withEntry(e));

            builder = input.withNewSchema(schemaBuilder.build());
        } else {
            builder = this.recordBuilderFactory.newRecordBuilder();
        }

        // Substitution within result of current HTTP call
        final List<Param> updatedOutSubstituted = substituteExtractedValues(extractedValueList, record,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_RESULT_OPENER,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_RESULT_CLOSER,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_RESULT_PREFIX_KEY);

        // Substitution within input record
        final List<Param> updatedInSubstituted = substituteExtractedValues(updatedOutSubstituted, input,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_INPUT_OPENER,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_INPUT_CLOSER,
                RecordBuilderService.CONTEXT_SUBSTITUTOR_INPUT_PREFIX_KEY);
        updatedInSubstituted.stream().forEach(e -> builder.withString(e.getKey(), e.getValue()));

        return builder.build();
    }

    private List<Param> substituteExtractedValues(final List<Param> values, final Record source,
            final String opener, final String closer, final String prefixKey) {
        Substitutor.PlaceholderConfiguration inParameterFinder =
                new Substitutor.PlaceholderConfiguration(opener, closer, prefixKey);
        final UnaryOperator<String> dictionary = ClassLoaderInvokeUtils.invokeInLoader(
                () -> DictionaryProvider.getProvider()
                        .createDictionary(
                                source, this.jsonParserFactory, this.recordBuilderFactory, true),
                this.getClass().getClassLoader());

        final Substitutor substitutor = new Substitutor(inParameterFinder, dictionary);

        return values.stream()
                .map(e -> new Param(e.getKey(), substitutor.replace(e.getValue())))
                .collect(Collectors.toList());
    }

    private ContentFormat findFormat(final RequestConfig config) {
        if (config.getDataset().getFormat() == Format.JSON) {
            return ClassLoaderInvokeUtils.invokeInLoader(
                    () -> JsonContentProvider.getProvider().provideJsonContentFormat(config),
                    this.getClass().getClassLoader());
        }

        return new RawTextConfiguration();
    }

    private Record convertHeadersToRecords(final Map.Entry<String, String> header) {
        return this.recordBuilderFactory
                .newRecordBuilder()
                .withString("key", header.getKey())
                .withString("value", header.getValue())
                .build();
    }

    private Record headersToRecord(final List<Record> headers) {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        headers.forEach(h -> builder.withString(h.getString("key"), h.getString("value")));
        return builder.build();
    }

    private Schema buildSchema(final Record body, final Record headersRecord, final boolean isCompletePayload,
            final Format format) {
        final Schema.Entry headersEntry = this.recordBuilderFactory
                .newEntryBuilder()
                .withName("headers")
                .withType(Type.RECORD)
                .withElementSchema(headersRecord.getSchema())
                .build();

        final Schema.Entry statusEntry = this.recordBuilderFactory
                .newEntryBuilder()
                .withName("status")
                .withType(Type.INT)
                .build();

        final Schema.Entry.Builder bodyBuilder = this.recordBuilderFactory.newEntryBuilder().withName("body");
        // If body is null we always return same schema as a RAW_TEXT
        if (format == Format.RAW_TEXT || body == null) {
            bodyBuilder.withType(Type.STRING).withNullable(true);
        } else {
            final Schema.Builder bodySchema = this.recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            body.getSchema().getEntries().forEach(e -> bodySchema.withEntry(e));
            bodyBuilder.withType(Type.RECORD).withElementSchema(bodySchema.build());
        }
        final Schema.Entry bodyEntry = bodyBuilder.build();

        final Schema.Builder builder = this.recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        if (isCompletePayload) {
            builder.withEntry(statusEntry).withEntry(headersEntry);
        }

        final Schema schema = builder.withEntry(bodyEntry).build();
        return schema;
    }
}
