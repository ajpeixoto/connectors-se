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
package org.talend.components.common.httpclient.pagination;

import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.common.httpclient.api.KeyValuePair;
import org.talend.components.common.httpclient.api.QueryConfiguration;
import org.talend.components.common.httpclient.api.pagination.OffsetLimitPagination;
import org.talend.components.common.httpclient.api.pagination.PaginationParametersLocation;
import org.talend.components.common.httpclient.pagination.location.*;

import javax.json.*;
import java.io.StringReader;
import java.util.List;
import java.util.Optional;

public class OffsetLimitPaginationStrategy implements PaginationStrategy {

    private QueryConfiguration queryConfiguration;

    private PaginationLocation paginationLocation;

    private int lastCount = -1;

    public OffsetLimitPaginationStrategy(final QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
        this.paginationLocation = PaginationLocationFactory
                .getPaginationLocation(queryConfiguration.getOffsetLimitPagination().getLocation());
    }

    @Override
    public QueryConfiguration initiatePagination(QueryConfiguration queryConfiguration) {
        if (queryConfiguration.isInitPaginationDone()) {
            return queryConfiguration; // Pagination already initiated.
        }
        paginationLocation.setPagination(queryConfiguration);
        this.queryConfiguration.setInitPaginationDone(true);

        return this.queryConfiguration;
    }

    @Override
    public Optional<QueryConfiguration> getNextPageConfiguration(HTTPClient.HTTPResponse response)
            throws HTTPClientException {

        int nbReceived = getLastCount(response);
        if (nbReceived <= 0) {
            // If last call didn't receive any data, then stop the pagination.
            return Optional.empty();
        }
        paginationLocation.updatePagination(queryConfiguration, nbReceived);

        return Optional.ofNullable(this.queryConfiguration);
    }

    @Override
    public int getLastCount(HTTPClient.HTTPResponse response) throws HTTPClientException {
        if (lastCount < 0) {
            this.lastCount = computeNbReceivedElement(response);
        }
        return this.lastCount;
    }

    /*
     * The element path must be a simplified DSSL.
     * No predicat { ... }, only simple path .aaa.bbb.ccc and last segment must be a json array.
     * Intermediate segments, must be jsonObject.
     */
    private int computeNbReceivedElement(HTTPClient.HTTPResponse response) throws HTTPClientException {
        String elementsPath = this.queryConfiguration.getOffsetLimitPagination().getElementsPath();

        // Remove trailing '.' characters.
        while (elementsPath.startsWith(".")) {
            elementsPath = elementsPath.substring(1);
        }

        while (elementsPath.endsWith(".")) {
            elementsPath = elementsPath.substring(0, elementsPath.length() - 1);
        }

        // Retrieve all segment
        String[] paths = elementsPath.split("\\.");

        int nbElements = 0;
        JsonReader jsonReader = Json.createReader(new StringReader(response.getBodyAsString()));
        if (paths.length == 0) {
            // If no element path or only '.', the root of the json payload must be an array
            nbElements = jsonReader.readArray().size();
        } else {
            JsonObject current = jsonReader.readObject();
            for (int i = 0; i < paths.length; i++) {
                boolean last = i == (paths.length - 1);
                if (current.getJsonArray(paths[i]) != null) {
                    if (last) {
                        // The last segment must be an array
                        nbElements = current.getJsonArray(paths[i]).size();
                    } else {
                        // Intermediate segment must be an object
                        current = current.getJsonObject(paths[i]);
                    }
                }
            }
        }
        jsonReader.close();

        return nbElements;
    }
}
