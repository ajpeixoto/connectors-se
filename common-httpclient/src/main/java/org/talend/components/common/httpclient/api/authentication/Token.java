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
package org.talend.components.common.httpclient.api.authentication;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Token {

    private String accessToken;

    private String tokenType;

    /**
     * Time in seconds, when the token has been delivered.
     */
    private long deliveredTime;

    /**
     * Lifetime in second.
     */
    private long expiresIn;

    /**
     *
     * @return true if the token has expired.
     */
    public boolean isExpired() {
        long expiredTime = deliveredTime + expiresIn;
        return expiredTime - (System.currentTimeMillis() / 1000) < 0;
    }

}
