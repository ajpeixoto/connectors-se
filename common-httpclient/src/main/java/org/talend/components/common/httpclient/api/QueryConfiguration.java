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
package org.talend.components.common.httpclient.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.talend.components.common.httpclient.api.authentication.AuthenticationType;
import org.talend.components.common.httpclient.api.authentication.LoginPassword;

import lombok.Data;
import org.talend.components.common.httpclient.api.pagination.OffsetLimitPagination;

@Data
/**
 * The configuration of an HTTP query for HTTPClient implementations.
 */
public class QueryConfiguration {

    /**
     * The URL with placeholders.
     * For instance: https://www.myserver.com, or https://api.myserver.com/{version}/{entity}
     * place holders will be replaced using urlPathParam in the HTTPClient.
     */
    private String url;

    /**
     * HTTP Method (GET, PUT, PATCH, ...).
     */
    private String method;

    /**
     * Connection timeout, in millisecond.
     */
    private long connectionTimeout = DefaultConfigurationValues.HTTP_CLIENT_CONNECT_TIMEOUT_VALUE;

    /**
     * Receive data timeout, in millisecond.
     */
    private long receiveTimeout = DefaultConfigurationValues.HTTP_CLIENT_RECEIVE_TIMEOUT_VALUE;

    /**
     * Disabled client certification validation.
     */
    private boolean bypassCertificateValidation = false;

    /**
     * Authentication strategy type.
     */
    private AuthenticationType authenticationType = AuthenticationType.None;

    /**
     * Login/password for basic & digest authentication.
     */
    private LoginPassword loginPassword;

    /**
     * Prefixed token sent to Authorization header.
     */
    private String authorizationToken;

    /**
     * Key/values to replace placeholders in main URL.
     */
    private Map<String, String> urlPathParams = new HashMap<>();

    /**
     * Key/values for query parameters.
     */
    private List<KeyValuePair> queryParams = new ArrayList<>();

    /**
     * Key/values for headers.
     */
    private List<KeyValuePair> headers = new ArrayList<>();

    /**
     * Type of the body sent within the query
     */
    private BodyFormat bodyType;

    /**
     * Key/values for multi-part-form-dara body.
     */
    private List<KeyValuePair> bodyQueryParams = new ArrayList<>();

    /**
     * Plain text body, can be also json, xml etc...
     */
    private String plainTextBody;

    /**
     * Decompress response payload
     */
    private boolean decompressResponsePayload;

    /**
     * OAuth2.0 HTTP call configuration.
     * It is the HTTP call configuration to retrieve the token.
     */
    private QueryConfiguration oauthCall;

    /**
     * The oauth token key for cache
     */
    private Optional<String> oAuthTokenCacheKey = Optional.empty();

    private boolean acceptRedirections =
            DefaultConfigurationValues.HTTP_CLIENT_ACCEPT_REDIRECTIONS_VALUE;

    /**
     * A coma separated list of HTTP verbs accepted for redirection, is null this is the underlying client default one.
     */
    // Need to wait for https://issues.apache.org/jira/browse/CXF-8752
    // private String allowedRedictedVerbs = DefaultConfigurationValues.HTTP_CLIENT_ALLOWED_REDIRECTED_VERBS_VALUE;

    private int maxNumberOfAcceptedRedirectionsOnSameURI =
            DefaultConfigurationValues.HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI_VALUE;

    private boolean acceptOnlySameHostRedirection =
            DefaultConfigurationValues.HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS_VALUE;

    private boolean acceptRelativeURLRedirection =
            DefaultConfigurationValues.HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS_VALUE;

    private String allowedURIRedirection;

    private ResponseFormat responseFormat;

    private ProxyConfiguration proxy;

    private List<Attachment> attachments = new ArrayList<>();

    /**
     * True when the pagination strategy update the configuration to get the 1st page.
     */
    private boolean initPaginationDone = false;

    private OffsetLimitPagination offsetLimitPagination;

}
