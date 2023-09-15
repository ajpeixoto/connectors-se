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
package org.talend.components.http.service.httpClient;

import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.sdk.component.api.exception.ComponentException;

public class HTTPComponentException extends ComponentException {

    private HTTPClient.HTTPResponse response;

    public HTTPComponentException(ErrorOrigin errorOrigin, String type, String message, StackTraceElement[] stackTrace,
            Throwable cause) {
        super(errorOrigin, type, message, stackTrace, cause);
    }

    public HTTPComponentException(String type, String message, StackTraceElement[] stackTrace, Throwable cause) {
        super(type, message, stackTrace, cause);
    }

    public HTTPComponentException(ErrorOrigin errorOrigin, String message) {
        super(errorOrigin, message);
    }

    public HTTPComponentException(ErrorOrigin errorOrigin, String message, Throwable cause) {
        super(errorOrigin, message, cause);
    }

    public HTTPComponentException(String message) {
        super(message);
    }

    public HTTPComponentException(String message, Throwable cause) {
        super(message, cause);
    }

    public HTTPComponentException(Throwable cause) {
        super(cause);
    }

    public HTTPClient.HTTPResponse getResponse() {
        return this.response;
    }

    public void setResponse(HTTPClient.HTTPResponse response) {
        this.response = response;
    }
}
