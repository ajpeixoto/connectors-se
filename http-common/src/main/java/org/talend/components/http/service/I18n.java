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

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18n {

    String redirect(final int nbRedirect, final String url);

    String request(final String method, final String url, final String authentication);

    String setConnectionTimeout(final int timeout);

    String setReadTimeout(final int timeout);

    String timeout(final String url, final String message);

    String headers();

    String invalideBodyContent(String format, String status, String payload, String cause);

    String formatText();

    String formatJSON();

    String formatXML();

    String httpClientException(String causeType, String message);

    String cantReadResponsePayload(String cause);

    String emptyPayload();

    String notAllowedToExecCallForDiscoverSchema();

    String responseStatusIsNotOK(String codeWithReason);

    String attachmentAlreadyExists(String name);

    String noUploadFileExists(String name);

    String paginationNotCompliantWithStreamJob();

    String readerNotFound(String message);
}
