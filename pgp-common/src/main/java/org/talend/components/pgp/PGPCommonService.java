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
package org.talend.components.pgp;

import static org.talend.components.pgp.decrypt.PGPDecryptConfig.BehaviorEnum.*;
import static org.talend.components.pgp.encrypt.PGPEncryptConfig.BehaviorEnum.*;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.pgpainless.PGPainless;
import org.pgpainless.algorithm.DocumentSignatureType;
import org.pgpainless.decryption_verification.ConsumerOptions;
import org.pgpainless.encryption_signing.EncryptionOptions;
import org.pgpainless.encryption_signing.ProducerOptions;
import org.pgpainless.encryption_signing.SigningOptions;
import org.pgpainless.key.protection.CachingSecretKeyRingProtector;
import org.pgpainless.util.Passphrase;
import org.talend.components.pgp.decrypt.PGPDecryptConfig;
import org.talend.components.pgp.encrypt.PGPEncryptConfig;
import org.talend.sdk.component.api.service.Service;

@Service
public class PGPCommonService {

    public static PGPPublicKeyRing getPublicKey(PGPKeyConfig config)
            throws IOException, IllegalArgumentException {
        if (config.isReadPublicKeyFromFile()) {
            return PGPainless.readKeyRing().publicKeyRing(new FileInputStream(config.getPublicKeyFilePath()));
        } else {
            if (StringUtils.isEmpty(config.getPublicKey())) {
                throw new IllegalArgumentException("Public key is empty.");
            }
            return PGPainless.readKeyRing().publicKeyRing(config.getPublicKey());
        }
    }

    public static PGPSecretKeyRing getPrivateKey(PGPKeyConfig config)
            throws IOException, IllegalArgumentException {
        if (config.isReadPrivateKeyFromFile()) {
            return PGPainless.readKeyRing()
                    .secretKeyRing(new FileInputStream(config.getPrivateKeyFilePath()));
        } else {
            if (StringUtils.isEmpty(config.getPrivateKey())) {
                throw new IllegalArgumentException("Private key is empty.");
            }
            return PGPainless.readKeyRing().secretKeyRing(config.getPrivateKey());
        }
    }

    public static CachingSecretKeyRingProtector getPrivateKeyRing(PGPKeyConfig config)
            throws IOException {
        CachingSecretKeyRingProtector secretKRing = new CachingSecretKeyRingProtector();
        if (config.getPrivateKeyPassword() != null && config.getPrivateKeyPassword().length() > 0) {
            secretKRing.addPassphrase(getPrivateKey(config),
                    Passphrase.fromPassword(config.getPrivateKeyPassword()));
        }
        return secretKRing;
    }

    public ProducerOptions getProducerOptions(PGPKeyConfig pgpKeyConfig, PGPEncryptConfig encryptConfig,
            String passphrase)
            throws PGPException, IOException {
        ProducerOptions producerOptions = null;
        EncryptionOptions encryptionOptions = new EncryptionOptions();
        SigningOptions signingOptions = new SigningOptions();
        PGPEncryptConfig.BehaviorEnum action = encryptConfig.getAction();
        if (action == Encrypt || action == EncryptAndSign) {
            encryptionOptions = encryptionOptions
                    .addRecipient(getPublicKey(pgpKeyConfig));
            if (passphrase != null
                    && passphrase.length() > 0) {
                encryptionOptions = encryptionOptions
                        .addPassphrase(Passphrase.fromPassword(passphrase));
            }
        }

        if (action == Sign || action == EncryptAndSign) {
            signingOptions = new SigningOptions()
                    .addInlineSignature(
                            getPrivateKeyRing(pgpKeyConfig),
                            getPrivateKey(pgpKeyConfig),
                            DocumentSignatureType.valueOf(encryptConfig.getSignatureType().toString()));
        }

        switch (action) {
        case Encrypt:
            producerOptions = ProducerOptions.encrypt(encryptionOptions);
            break;
        case Sign:
            producerOptions = ProducerOptions.sign(signingOptions);
            break;
        case EncryptAndSign:
            producerOptions = ProducerOptions.signAndEncrypt(encryptionOptions, signingOptions);
            break;
        default:
            throw new RuntimeException("Invalid action: " + action);
        }
        producerOptions.setAsciiArmor(encryptConfig.isArmored());
        if (producerOptions.isAsciiArmor()) {
            producerOptions.setComment(encryptConfig.getComment());
        }

        if (action != Sign && action != Encrypt && action != EncryptAndSign) {
            throw new RuntimeException("Invalid action: " + action);
        }

        return producerOptions;
    }

    public ConsumerOptions getConsumerOptions(PGPKeyConfig pgpKeyConfig, PGPDecryptConfig decryptConfig,
            String passphrase)
            throws IOException {
        PGPDecryptConfig.BehaviorEnum action = null;
        if (decryptConfig != null) {
            action = decryptConfig.getAction();
        }
        if (action != Decrypt && action != Validate && action != DecryptAndValidate) {
            throw new RuntimeException("Invalid action: " + action);
        }
        ConsumerOptions consumerOptions = ConsumerOptions.get();
        if (action == Decrypt || action == DecryptAndValidate) {
            consumerOptions = consumerOptions.addDecryptionKey(
                    getPrivateKey(pgpKeyConfig),
                    getPrivateKeyRing(pgpKeyConfig));

            if (passphrase != null
                    && !passphrase.isEmpty()) {
                consumerOptions = consumerOptions.addDecryptionPassphrase(
                        Passphrase.fromPassword(passphrase));
            }
        }
        if (action == Validate || action == DecryptAndValidate) {
            consumerOptions = consumerOptions
                    .addVerificationCert(getPublicKey(pgpKeyConfig));
        }

        if (action != Decrypt && action != Validate && action != DecryptAndValidate) {
            throw new RuntimeException("Invalid action: " + action);
        }

        return consumerOptions;
    }

}