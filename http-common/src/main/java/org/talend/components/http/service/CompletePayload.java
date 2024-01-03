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

import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.json.bind.annotation.JsonbPropertyOrder;
import java.util.Map;

@Data
@RequiredArgsConstructor
@JsonbPropertyOrder({ "status", "headers", "body" })
public class CompletePayload {

    private final int status;

    private final Map<String, String> headers;

    private final Object body;

}
