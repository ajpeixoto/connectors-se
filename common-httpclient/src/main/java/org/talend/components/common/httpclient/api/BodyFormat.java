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

import javax.ws.rs.core.MediaType;

public enum BodyFormat {

    TEXT(MediaType.TEXT_PLAIN),
    JSON(MediaType.APPLICATION_JSON),
    XML(MediaType.TEXT_XML),
    FORM_DATA(MediaType.MULTIPART_FORM_DATA),
    X_WWW_FORM_URLENCODED(MediaType.APPLICATION_FORM_URLENCODED);

    private final String contentType;

    BodyFormat(final String contentType) {
        this.contentType = contentType;
    }

    public String getContentType() {
        return this.contentType;
    }
}
