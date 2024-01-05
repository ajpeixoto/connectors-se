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
package org.talend.components.http.input;

import lombok.AccessLevel;
import lombok.Getter;
import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.QueryConfiguration;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.service.I18n;
import org.talend.components.http.service.RecordBuilderService;
import org.talend.components.http.service.httpClient.HTTPClientService;
import org.talend.components.http.service.httpClient.HTTPComponentException;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;

@Getter
@Pollable(name = "Polling", resumeMethod = "resume")
public abstract class AbstractHTTPInput<T> implements Serializable {

    @Getter(AccessLevel.PROTECTED)
    private RequestConfig config;

    @Getter(AccessLevel.PROTECTED)
    private HTTPClientService client;

    @Getter(AccessLevel.PROTECTED)
    private RecordBuilderService recordBuilder;

    private I18n i18n;

    private Iterator<Record> items;

    private boolean done;

    private Optional<QueryConfiguration> queryConfiguration;

    public AbstractHTTPInput(final T config, final HTTPClientService client,
            final RecordBuilderService recordBuilder, final I18n i18n) {
        this.config = translateConfiguration(config);
        this.client = client;
        this.recordBuilder = recordBuilder;
        this.i18n = i18n;
    }

    protected abstract RequestConfig translateConfiguration(T config);

    /**
     * This is the method called to resume polling (because of the @Pollable annotation)
     *
     * @param configuration
     */
    public void resume(Object configuration) {

        if (this.config.getDataset().isHasPagination()) {
            throw new RuntimeException(i18n.paginationNotCompliantWithStreamJob());
        }

        done = false;
        if (!this.queryConfiguration.isPresent()) {
            this.postConstruct();
        }
    }

    @PostConstruct
    public void postConstruct() {
        this.queryConfiguration = Optional.ofNullable(client.convertConfiguration(this.config, null));
    }

    @Producer
    public Record next() {
        if (items == null && !done) {
            done = true;
            try {
                HTTPClient.HTTPResponse response =
                        client.invoke(this.queryConfiguration.get(), this.config.isDieOnError());

                this.queryConfiguration = response.nextPageQueryConfiguration();

                if (response.getLastPageCount() <= 0) {
                    // This getLastPageCount() has been added because DSSL doesn't deal with json empty array.
                    // Since dataset.dssl and dataset.pagination.OffsetLimit.elements dssl segments are contatenated
                    // we can have something like '.element.name' but, on the last pagination call, when an empty array
                    // is retrieved, it generates an exception since .element is empty and we try to access nested
                    // .name.
                    return null;
                }

                items = recordBuilder.buildFixedRecord(response, config);
            } catch (Exception e) {
                HTTPComponentException ce = new HTTPComponentException(ComponentException.ErrorOrigin.BACKEND,
                        i18n.httpClientException(String.valueOf(e.getClass()), e.getMessage()));
                ce.setStackTrace(e.getStackTrace());
                if (e instanceof HTTPComponentException) {
                    ce.setResponse(((HTTPComponentException) e).getResponse());
                }
                throw ce;
            }
        }
        if (items == null) {
            return null;
        }

        Record r = null;
        if (items.hasNext()) {
            r = items.next();
        }

        if (!items.hasNext()) {
            items = null;

            if (this.queryConfiguration.isPresent()) {
                done = false;
            }
        }

        return r;

    }
}
