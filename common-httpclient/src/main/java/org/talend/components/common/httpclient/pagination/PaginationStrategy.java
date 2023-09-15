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
import java.util.function.Function;

/**
 * Pagination strategy has to be instanciated with initial QueryConfiguration.
 */
public interface PaginationStrategy {

    /**
     * Must set the QueryConfiguration.initPaginationDone to true.
     *
     * @param queryConfiguration The configuration to update.
     * @return The same configuration after the update.
     */
    QueryConfiguration initiatePagination(QueryConfiguration queryConfiguration);

    /**
     * @param currentConfiguration The last QueryConfiguration.
     * @param response The last HTTP response
     * @return The configuration to retrieve the next page, emtpy if no more page.
     */
    Optional<QueryConfiguration> getNextPageConfiguration(HTTPClient.HTTPResponse response) throws HTTPClientException;

    int getLastCount(HTTPClient.HTTPResponse response) throws HTTPClientException;

}
