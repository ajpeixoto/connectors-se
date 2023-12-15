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
package org.talend.components.common.httpclient.pagination.location;

import org.talend.components.common.httpclient.api.KeyValuePair;
import org.talend.components.common.httpclient.api.QueryConfiguration;

import java.util.List;

public class PaginationLocationQueryParameters implements PaginationLocation {

    private final PaginationLocationService paginationLocationService;

    public PaginationLocationQueryParameters() {
        this.paginationLocationService = new PaginationLocationService();
    }

    @Override
    public void setPagination(QueryConfiguration queryConfiguration) {
        List<KeyValuePair> keyValuePairs = queryConfiguration.getQueryParams();
        List<KeyValuePair> updatedKeyValuePairs = paginationLocationService.initKeyValuePairs(keyValuePairs,
                queryConfiguration.getOffsetLimitPagination());
        queryConfiguration.setQueryParams(updatedKeyValuePairs);
    }

    @Override
    public void updatePagination(QueryConfiguration queryConfiguration, int nbReceived) {
        List<KeyValuePair> keyValuePairs = queryConfiguration.getQueryParams();
        List<KeyValuePair> updatedKeyValuePairs =
                paginationLocationService.updateListKeyValuePair(keyValuePairs,
                        queryConfiguration.getOffsetLimitPagination(), nbReceived);
        queryConfiguration.setQueryParams(updatedKeyValuePairs);
    }
}
