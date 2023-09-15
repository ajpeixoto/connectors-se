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
import org.talend.components.common.httpclient.api.QueryConfiguration;

import java.util.Optional;

public class NoPagination implements PaginationStrategy {

    @Override
    public QueryConfiguration initiatePagination(QueryConfiguration queryConfiguration) {
        return queryConfiguration;
    }

    @Override
    public Optional<QueryConfiguration> getNextPageConfiguration(HTTPClient.HTTPResponse response)
            throws HTTPClientException {
        return Optional.empty();
    }

    @Override
    public int getLastCount(HTTPClient.HTTPResponse response) {
        return 1;
    }

}
