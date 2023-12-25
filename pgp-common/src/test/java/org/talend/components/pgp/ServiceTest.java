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
package org.talend.components.pgp;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

class ServiceTest {

    @Test
    void getPublicKey_FromFile() throws PGPException, IOException {
        PGPKeyConfig conn = new PGPKeyConfig();
        conn.setReadPublicKeyFromFile(true);
        conn.setPublicKeyFilePath(
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("keys/cryptie_pub.asc")
                        .getPath()
                        .toString());
        PGPPublicKey pubKey = PGPCommonService.getPublicKey(conn).getPublicKey();
        Assertions.assertEquals(-821156605394703576L, pubKey.getKeyID(), "Key ID doesn't match!");
    }

    @Test
    void getPublicKey_FromString() throws PGPException, IOException, URISyntaxException {
        PGPKeyConfig conn = new PGPKeyConfig();
        conn.setReadPublicKeyFromFile(false);
        String keyAsString = new String(Files.readAllBytes(
                Paths.get(Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("keys/cryptie_pub.asc")
                        .toURI())));
        conn.setPublicKey(keyAsString);
        PGPPublicKey pubKey = PGPCommonService.getPublicKey(conn).getPublicKey();
        Assertions.assertEquals(-821156605394703576L, pubKey.getKeyID(), "Key ID doesn't match!");
    }

    @Test
    void getPrivateKey_FromFile() throws PGPException, IOException {
        PGPKeyConfig conn = new PGPKeyConfig();
        conn.setReadPrivateKeyFromFile(true);
        conn.setPrivateKeyFilePath(
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("keys/cryptie_sec.asc")
                        .getPath()
                        .toString());
        PGPSecretKey secretKey = PGPCommonService.getPrivateKey(conn).getSecretKey();
        Assertions.assertEquals(-821156605394703576L, secretKey.getKeyID(), "Key ID doesn't match!");
    }

    @Test
    void getPrivateKey_FromString() throws PGPException, IOException, URISyntaxException {
        PGPKeyConfig conn = new PGPKeyConfig();
        conn.setReadPrivateKeyFromFile(false);
        String keyAsString = new String(Files.readAllBytes(
                Paths.get(Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("keys/cryptie_sec.asc")
                        .toURI())));
        conn.setPrivateKey(keyAsString);
        PGPSecretKey secretKey = PGPCommonService.getPrivateKey(conn).getSecretKey();
        Assertions.assertEquals(-821156605394703576L, secretKey.getKeyID(), "Key ID doesn't match!");
    }

}
