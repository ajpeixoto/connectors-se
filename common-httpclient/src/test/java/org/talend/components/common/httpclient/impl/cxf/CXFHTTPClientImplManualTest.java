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

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.HTTPClientException;
import org.talend.components.common.httpclient.api.QueryConfiguration;
import org.talend.components.common.httpclient.api.QueryConfigurationBuilder;
import org.talend.components.common.httpclient.factory.HTTPClientFactory;

/**
 * These are manuel tests I have made but complicated to reproduce as automated unit tests.
 */
@Disabled
public class CXFHTTPClientImplManualTest {

    /**
     * I have followed this tutorial https://linuxize.com/post/how-to-install-and-configure-squid-proxy-on-ubuntu-20-04/
     * /!\ Need this jvm option to run : -Djdk.http.auth.tunneling.disabledSchemes=""
     * to set a local HTTP proxy in my Ubuntu.
     */
    @Test
    public void proxyHTTPTest() throws HTTPClientException {
        // Need to add this jvm option to have basic authent working: -Djdk.http.auth.tunneling.disabledSchemes=""
        // https://ec.europa.eu/digital-building-blocks/wikis/display/CEKB/Proxy+Authentication+failure+on+invoking+HTTPS+end+points
        QueryConfiguration config = QueryConfigurationBuilder.create("https://httpbin.org/get?q=avalue")
                .setMethod("GET")
                .setHTTPProxy("127.0.0.1", 3128, "peter", "azerty")
                .build();
        HTTPClient.HTTPResponse response = HTTPClientFactory.create(config).invoke();
        JsonObject jsonObject = ResourcesUtils.getJsonObject(response.getBodyAsString());
        Assertions.assertEquals(200, response.getStatus().getCode());
        Assertions.assertEquals("avalue", jsonObject.getJsonObject("args").getString("q"));
    }

    /**
     * I've choosen a free socks proxy among http://free-proxy.cz/fr/proxylist/country/all/socks5/ping/all for anonymous
     * proxy (login & password = null)
     * I have also a docker image with dante server : https://github.com/ypiel-talend/docker-dante
     * The test is currently successful with anonymous proxy (login/password = null) but is failing when authentication
     * required
     * whereas curl is successful.
     */
    @Test
    public void proxySOCKSTest() throws HTTPClientException {
        QueryConfiguration config = QueryConfigurationBuilder.create("https://httpbin.org/get?q=avalue")
                .setMethod("GET")
                // .setProxy(ProxyConfiguration.ProxyType.SOCKS, "localhost", 1080, "peter", "aze123_=KLM") // currently
                // failing
                .setSOCKSProxy("127.0.0.1", 1080)
                .build();
        HTTPClient.HTTPResponse response = HTTPClientFactory.create(config).invoke();
        JsonObject jsonObject = ResourcesUtils.getJsonObject(response.getBodyAsString());
        Assertions.assertEquals(200, response.getStatus().getCode());
        Assertions.assertEquals("avalue", jsonObject.getJsonObject("args").getString("q"));
    }

    /**
     * Retrieve json resource from a server with a self-signed certificate.
     * Follow connectors-ee/est-improved/src/main/resources/docker/apache2/README.md
     * to build and start the container. Then execute the test.
     * 
     * @throws HTTPClientException
     */
    @Test
    public void bypassCertificateValidationTest() throws HTTPClientException {
        // TODO : need a HTTPS server with self signed certificate.
        final QueryConfiguration config =
                QueryConfigurationBuilder.create("https://restimprove:45300/digest_authent.json")
                        .setMethod("GET")
                        .bypassCertificateConfiguration(true)
                        .setDigestAuthentication("john", "abcde")
                        .build();

        HTTPClient.HTTPResponse response = HTTPClientFactory.create(config).invoke();
        Assertions.assertEquals(200, response.getStatus().getCode());
    }

    @Test
    public void NTLMAuthent() throws HTTPClientException {
        final QueryConfiguration config =
                QueryConfigurationBuilder
                        .create(System.getProperty("mscrm.onpremise.url.root") + "/accounts?$select=name")
                        .setMethod("GET")
                        .setNTLMAuthentication(System.getProperty("ntlm.user"), System.getProperty("ntlm.password"))
                        .build();

        HTTPClient.HTTPResponse response = HTTPClientFactory.create(config).invoke();

        String payload = response.getBodyAsString();
        Assertions.assertEquals(200, response.getStatus().getCode());
    }

}
