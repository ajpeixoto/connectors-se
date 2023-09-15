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
package org.talend.components.common.httpclient.impl.cxf;

import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

class BlindTrustManager implements X509TrustManager {

    public void checkClientTrusted(X509Certificate[] chain,
            String authType) throws java.security.cert.CertificateException {
    }

    public void checkServerTrusted(X509Certificate[] chain,
            String authType) throws java.security.cert.CertificateException {
    }

    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}
