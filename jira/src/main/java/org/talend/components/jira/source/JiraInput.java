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
package org.talend.components.jira.source;

import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParsingException;

import org.apache.cxf.common.util.StringUtils;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.http.configuration.Dataset;
import org.talend.components.http.configuration.Datastore;
import org.talend.components.http.configuration.Format;
import org.talend.components.http.configuration.OutputContent;
import org.talend.components.http.configuration.Param;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.configuration.auth.Authentication;
import org.talend.components.http.configuration.auth.Authorization;
import org.talend.components.http.configuration.auth.Basic;
import org.talend.components.http.configuration.pagination.OffsetLimitStrategyConfig;
import org.talend.components.http.configuration.pagination.Pagination;
import org.talend.components.http.input.AbstractHTTPInput;
import org.talend.components.http.service.I18n;
import org.talend.components.http.service.RecordBuilderService;
import org.talend.components.http.service.httpClient.HTTPClientService;
import org.talend.components.http.service.httpClient.HTTPComponentException;
import org.talend.components.jira.dataset.ResourceType;
import org.talend.components.jira.datastore.JiraDatastore;
import org.talend.components.jira.service.JiraI18n;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

@Version(value = 1)
@Icon(value = CUSTOM, custom = "jira-input")
@Emitter(name = "Input")
@Documentation("Jira input connector.")
public class JiraInput extends AbstractHTTPInput<JiraInputConfiguration> {

    private final JiraI18n jiraI18n;

    private final JsonReaderFactory jsonReaderFactory;

    private final JiraInputConfiguration configuration;

    private final Queue<String> jiraJQLSearchResultQueue;

    public JiraInput(HTTPClientService httpClientService, RecordBuilderService recordBuilderService,
            JiraInputConfiguration configuration, I18n i18n, JiraI18n jiraI18n, JsonReaderFactory jsonReaderFactory) {
        super(configuration, httpClientService, recordBuilderService, i18n);
        this.configuration = configuration;
        this.jiraI18n = jiraI18n;
        this.jsonReaderFactory = jsonReaderFactory;
        this.jiraJQLSearchResultQueue = new LinkedList<>();
    }

    @Override
    protected RequestConfig translateConfiguration(JiraInputConfiguration config) {
        RequestConfig requestConfig = new RequestConfig();

        Datastore requestConfigDatastore = new Datastore();
        requestConfigDatastore.setBase(config.getDataset().getDatastore().getJiraURL());

        Authentication authentication = new Authentication();
        if (config.getDataset()
                .getDatastore()
                .getAuthenticationType() == JiraDatastore.AuthenticationType.BASIC) {
            authentication.setType(Authorization.AuthorizationType.Basic);
            Basic basic = new Basic();
            basic.setUsername(config.getDataset().getDatastore().getUser());
            basic.setPassword(config.getDataset().getDatastore().getPass());
            authentication.setBasic(basic);
        } else if (config.getDataset()
                .getDatastore()
                .getAuthenticationType() == JiraDatastore.AuthenticationType.PAT) {
            authentication.setType(Authorization.AuthorizationType.Bearer);
            authentication.setBearerToken(config.getDataset().getDatastore().getPat());
        }
        requestConfigDatastore.setAuthentication(authentication);

        Dataset requestConfigDataset = new Dataset();
        requestConfigDataset.setDatastore(requestConfigDatastore);
        requestConfigDataset.setMethodType("GET");
        requestConfigDataset.setFormat(Format.RAW_TEXT); // stream-json failed to read

        if (config.getDataset().getResourceType() == ResourceType.PROJECT) {
            requestConfigDataset.setResource("rest/api/2/project/" + config.getProjectId());
        } else if (!config.isUseJQL()) {
            requestConfigDataset.setResource("rest/api/2/issue/" + config.getIssueId());
        } else {
            requestConfigDataset.setHasQueryParams(true);
            requestConfigDataset.setQueryParams(Collections.singletonList(new Param("jql", config.getJql())));

            Pagination pagination = new Pagination();
            pagination.setStrategy(Pagination.Strategy.OFFSET_LIMIT);
            OffsetLimitStrategyConfig paginationConfig = new OffsetLimitStrategyConfig();
            paginationConfig.setOffsetParamName("startAt");
            paginationConfig.setOffsetValue("0");
            paginationConfig.setLimitParamName("maxResults");
            paginationConfig.setLimitValue(String.valueOf(config.getBatchSize()));
            pagination.setOffsetLimitStrategyConfig(paginationConfig);
            paginationConfig.setElementsPath(".issues");
            requestConfigDataset.setPagination(pagination);
            requestConfigDataset.setHasPagination(true);

            requestConfigDataset.setResource("rest/api/2/search");
        }

        requestConfigDataset.setReturnedContent(OutputContent.BODY_ONLY);

        requestConfig.setDieOnError(true);
        requestConfig.setDataset(requestConfigDataset);
        return requestConfig;
    }

    @Override
    @Producer
    public Record next() {
        try {
            if (isJQLSearch()) {
                if (jiraJQLSearchResultQueue.isEmpty()) {
                    Record nextPageJQLSearchResult = super.next();
                    processResult(nextPageJQLSearchResult, "issues");

                }
            } else if (isGetAllProjects()) {
                Record allProjectsArray = super.next();
                processResult(allProjectsArray, null);
            } else {
                Record httpResponseRecord = super.next();
                if (httpResponseRecord != null && httpResponseRecord.getString("body") != null) {
                    jiraJQLSearchResultQueue.add(httpResponseRecord.getString("body"));
                }
            }
            String jiraRecordsString = !jiraJQLSearchResultQueue.isEmpty() ? jiraJQLSearchResultQueue.poll() : null;

            if (jiraRecordsString != null) {
                RecordBuilderFactory recordBuilderFactory = getRecordBuilder().getRecordBuilderFactory();
                return recordBuilderFactory.newRecordBuilder()
                        .withString("json", jiraRecordsString)
                        .build();
            } else {
                return null;
            }
        } catch (HTTPComponentException e) {
            throw extractJiraError(e);
        } catch (ComponentException e) {
            throw e;
        }
    }

    private boolean isGetAllProjects() {
        return configuration.getDataset().getResourceType() == ResourceType.PROJECT
                && StringUtils.isEmpty(configuration.getProjectId());
    }

    private boolean isJQLSearch() {
        return configuration.getDataset().getResourceType() == ResourceType.ISSUE && configuration.isUseJQL();
    }

    private void processResult(Record nextPageJQLSearchResult, String fieldName) {
        if (nextPageJQLSearchResult == null || nextPageJQLSearchResult.getString("body") == null) {
            return;
        }

        JsonParser parser = Json.createParser(new StringReader(nextPageJQLSearchResult
                .getString("body")));
        JsonArray records;
        if (fieldName != null) {
            records = parser.getObject().getJsonArray(fieldName);
        } else {
            records = parser.getArray();
        }
        records.forEach(rec -> jiraJQLSearchResultQueue.add(rec.asJsonObject().toString()));
    }

    private ComponentException extractJiraError(HTTPComponentException e) {
        if (e.getResponse() == null) {
            return e;
        }
        try {
            String responseString = e.getResponse().getBodyAsString();
            if (StringUtils.isEmpty(responseString)) {
                return new ComponentException(getI18n()
                        .responseStatusIsNotOK(e.getResponse().getStatus().getCodeWithReason()));
            }
            try {
                JsonReader reader = jsonReaderFactory.createReader(new StringReader(responseString));
                JsonObject jsonObject = reader.readObject();
                JsonArray errorMessages = jsonObject.getJsonArray("errorMessages");
                String errors = errorMessages.stream()
                        .map(JsonValue::toString)
                        .collect(Collectors.joining("\n"));
                return new ComponentException(jiraI18n.jiraErrorsList(errors), e);
            } catch (JsonParsingException parsingException) {
                return new ComponentException(
                        jiraI18n.cantExtractError(parsingException.getMessage(), responseString), parsingException);
            }
        } catch (HTTPClientException httpClientException) {
            return new ComponentException(
                    jiraI18n.cantReadServerResponse(httpClientException.getMessage(), e.getMessage()),
                    httpClientException);
        }
    }
}
