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

import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.talend.components.common.httpclient.api.BodyFormat;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.http.configuration.Dataset;
import org.talend.components.http.configuration.Datastore;
import org.talend.components.http.configuration.Param;
import org.talend.components.http.configuration.RequestBody;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.output.AbstractHTTPOutput;
import org.talend.components.http.service.I18n;
import org.talend.components.http.service.httpClient.HTTPClientService;
import org.talend.components.splunk.service.SplunkMessages;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.ReturnVariables;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.context.RuntimeContext;
import org.talend.sdk.component.api.context.RuntimeContextHolder;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.SchemaProperty;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.splunk.runtime.SplunkEventCollector.RESPONSE_CODE_AFTER_VARIABLE_KEY;
import static org.talend.sdk.component.api.component.ReturnVariables.ReturnVariable.AVAILABILITY.AFTER;

@Slf4j
@Version
@Processor(name = "EventCollector")
@Icon(value = Icon.IconType.CUSTOM, custom = "splunk-event-collector")
@ReturnVariables.ReturnVariable(value = RESPONSE_CODE_AFTER_VARIABLE_KEY, availability = AFTER,
        description = "Response code", type = Integer.class)
@Documentation("Splunk event collector.")
public class SplunkEventCollector extends AbstractHTTPOutput<SplunkEventCollectorProperties> {

    protected static final String RESPONSE_CODE_AFTER_VARIABLE_KEY = "RESPONSE_CODE";

    private final transient SplunkMessages splunkI18N;

    private Collection<Record> bulk;

    @RuntimeContext
    private transient RuntimeContextHolder context;

    public SplunkEventCollector(@Option("configuration") final SplunkEventCollectorProperties config,
            final HTTPClientService client, final I18n i18n, final SplunkMessages splunkI18N) {
        super(config, client, i18n);
        this.splunkI18N = splunkI18N;
    }

    @Override
    protected RequestConfig translateConfiguration(SplunkEventCollectorProperties config) {
        RequestConfig requestConfig = new RequestConfig();

        Dataset requestConfigDataset = new Dataset();
        Datastore requestConfigDatastore = new Datastore();
        requestConfigDatastore.setBase(config.getDataset().getDatastore().getServerURL());

        requestConfigDataset.setDatastore(requestConfigDatastore);
        requestConfigDataset.setHasHeaders(true);
        requestConfigDataset.setHeaders(Collections.singletonList(new Param("Authorization",
                "Splunk " + config.getDataset().getDatastore().getToken())));
        requestConfigDataset.setMethodType("POST");
        requestConfigDataset.setResource("services/collector");

        requestConfigDataset.setHasBody(true);
        RequestBody requestBody = new RequestBody();
        requestBody.setType(BodyFormat.JSON);

        requestConfigDataset.setBody(requestBody);
        requestConfig.setDieOnError(false);
        requestConfig.setDataset(requestConfigDataset);

        return requestConfig;
    }

    @BeforeGroup
    public void startBulk() {
        bulk = new ArrayList<>();
    }

    @Override
    @ElementListener
    public void process(Record input) {
        bulk.add(input);
    }

    @AfterGroup
    public void processBulk() {
        StringBuilder bodyBuilder = new StringBuilder();
        for (Record input : bulk) {
            JsonObject jsonObject = convertRecordToJsonObjectBody(input);

            bodyBuilder.append(jsonObject).append(System.lineSeparator());

        }

        getConfig().getDataset().getBody().setJsonValue(bodyBuilder.toString());

        // record content never used by HttpClientOutput with our configuration we prepared before, can pass null value
        super.process(null);

        try {
            handleResponse();
        } catch (HTTPClientException e) {
            throw new ComponentException(getI18n().cantReadResponsePayload(e.getMessage()), e);
        }

    }

    private JsonObject convertRecordToJsonObjectBody(Record input) {
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();

        JsonObjectBuilder eventObjectBuilder = Json.createObjectBuilder();
        input.getSchema()
                .getAllEntries()
                .filter(entry -> !SplunkMetadataFields.isMetadataField(entry.getName()))
                .forEach(entry -> putValueToJsonBuilder(eventObjectBuilder, entry.getName(), input));

        JsonObject eventObject = eventObjectBuilder.build();
        if (!eventObject.isEmpty()) {
            jsonObjectBuilder.add("event", eventObjectBuilder.build());
        }

        addMetadataIfPresent(jsonObjectBuilder, input);

        return jsonObjectBuilder.build();
    }

    private void putValueToJsonBuilder(JsonObjectBuilder jsonObjectBuilder,
            String jsonKey, Record input) {
        if (jsonObjectBuilder == null ||
                jsonKey == null ||
                input.get(Object.class, jsonKey) == null) {
            return;
        }

        Schema.Type columnType = input.getSchema().getEntry(jsonKey).getType();

        switch (columnType) {
        case STRING:
            jsonObjectBuilder.add(jsonKey, input.getString(jsonKey));
            break;
        case INT:
            jsonObjectBuilder.add(jsonKey, input.getInt(jsonKey));
            break;
        case LONG:
            jsonObjectBuilder.add(jsonKey, input.getLong(jsonKey));
            break;
        case BOOLEAN:
            jsonObjectBuilder.add(jsonKey, input.getBoolean(jsonKey));
            break;
        case DOUBLE:
        case FLOAT:
            jsonObjectBuilder.add(jsonKey, input.getDouble(jsonKey));
            break;
        case BYTES:
            JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            for (byte b : input.getBytes(jsonKey)) {
                jsonArrayBuilder.add(b);
            }
            jsonObjectBuilder.add(jsonKey, jsonArrayBuilder);
            break;
        case DATETIME:
            ZonedDateTime timeInRecord = input.getDateTime(jsonKey);
            if (timeInRecord != null) {
                long secondsLongValue = timeInRecord.toEpochSecond();
                jsonObjectBuilder.add(jsonKey, secondsLongValue);
            }
            break;
        case DECIMAL:
            jsonObjectBuilder.add(jsonKey, input.getDecimal(jsonKey));
            break;
        }
    }

    private void addMetadataIfPresent(JsonObjectBuilder parentJsonBuilder, Record input) {
        for (Schema.Entry item : input.getSchema()
                .getEntries()
                .stream()
                .filter(entry -> !entry.getName().equals(SplunkMetadataFields.TIME.getName()))
                .filter(entry -> SplunkMetadataFields.isMetadataField(entry.getName()))
                .collect(Collectors.toList())) {
            String metadataValue = input.getString(item.getName());
            if (metadataValue != null) {
                parentJsonBuilder.add(item.getName(), metadataValue);
            }
        }
        if (input.getSchema().getEntry(SplunkMetadataFields.TIME.getName()) != null &&
                input.get(Object.class, SplunkMetadataFields.TIME.getName()) != null) {
            long millisValue;
            if (input.getSchema()
                    .getEntry(SplunkMetadataFields.TIME.getName())
                    .getType() == Schema.Type.DATETIME) {
                millisValue = input.getDateTime(SplunkMetadataFields.TIME.getName()).toInstant().toEpochMilli();
            } else {
                Object timeInRecord = input.get(Object.class, SplunkMetadataFields.TIME.getName());
                String pattern = input.getSchema()
                        .getEntry(SplunkMetadataFields.TIME.getName())
                        .getProps()
                        .get(SchemaProperty.PATTERN);
                if (timeInRecord instanceof String && pattern != null) {
                    String timeInRecordString = timeInRecord.toString();
                    try {
                        // for dynamic time can be string, need to convert
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                        millisValue = simpleDateFormat.parse(timeInRecordString).getTime();
                    } catch (ParseException e) {
                        throw new ComponentException(splunkI18N.cantParseDate(timeInRecordString), e);
                    }
                } else {
                    millisValue = Long.parseLong(String.valueOf(timeInRecord));
                }
            }

            double secondsDoubleValue = millisValue / 1000.0;
            parentJsonBuilder.add(SplunkMetadataFields.TIME.getName(),
                    String.format(Locale.ROOT, "%.3f", secondsDoubleValue));
        }
    }

    private void handleResponse() throws HTTPClientException {

        if (getLastServerResponse().getStatus().getCode() / 100 > 3) {
            throw new ComponentException(getI18n().responseStatusIsNotOK(
                    getLastServerResponse().getStatus().getCodeWithReason()
                            + ": " + getLastServerResponse().getBodyAsString()));
        } else {
            log.debug("Response String:/r/n" + getLastServerResponse().getBodyAsString());
        }
    }

    public Integer getResponseCodeFromLastResponse() {
        try {
            if (getLastServerResponse() != null) {
                String responseBody = getLastServerResponse().getBodyAsString();
                if (responseBody != null) {
                    JsonObject json = Json.createReader(new StringReader(responseBody)).readObject();
                    return json.getInt("code");
                }
            }
        } catch (HTTPClientException e) {
            throw new ComponentException(e);
        }

        return null;
    }

    @PreDestroy
    public void finish() {
        context.set(RESPONSE_CODE_AFTER_VARIABLE_KEY, getResponseCodeFromLastResponse());
    }
}
