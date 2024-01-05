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

import java.io.Serializable;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout({
        @GridLayout.Row("readPublicKeyFromFile"),
        @GridLayout.Row("publicKeyFilePath"),
        @GridLayout.Row("publicKey"),
        @GridLayout.Row("readPrivateKeyFromFile"),
        @GridLayout.Row("privateKeyFilePath"),
        @GridLayout.Row("privateKey"),
        @GridLayout.Row("privateKeyPassword")
})
@Documentation("Common key configurations for PGP.")
public class PGPKeyConfig implements Serializable {

    private static final long serialVersionUID = -1189005290726637141L;

    @Option
    @Documentation("Used to load a public key from a file instead of a String.")
    @ActiveIf(target = "../../../actionConfig.action", value = { "Decrypt", "Sign" }, negate = true)
    private boolean readPublicKeyFromFile = false;

    @Option
    @Credential
    @Documentation("Public key in String format.")
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = {
            @ActiveIf(target = "readPublicKeyFromFile", value = "false"),
            @ActiveIf(target = "../../../actionConfig.action", value = { "Decrypt", "Sign" }, negate = true)
    })
    private String publicKey;

    @Option
    @Documentation("Path to the public key.")
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = {
            @ActiveIf(target = "readPublicKeyFromFile", value = "true"),
            @ActiveIf(target = "../../../actionConfig.action", value = { "Decrypt", "Sign" },
                    negate = true)
    })
    private String publicKeyFilePath;

    @Option
    @Documentation("Used to load a private key from a file instead of a String.")
    @ActiveIf(target = "../../../actionConfig.action", value = { "Encrypt", "Validate" }, negate = true)
    private boolean readPrivateKeyFromFile = false;

    @Option
    @Credential
    @Documentation("Private key in String format.")
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = {
            @ActiveIf(target = "readPrivateKeyFromFile", value = "false"),
            @ActiveIf(target = "../../../actionConfig.action", value = { "Encrypt", "Validate" }, negate = true)
    })
    private String privateKey;

    @Option
    @Documentation("Path to the private key.")
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = {
            @ActiveIf(target = "readPrivateKeyFromFile", value = "true"),
            @ActiveIf(target = "../../../actionConfig.action", value = { "Encrypt", "Validate" }, negate = true)
    })
    private String privateKeyFilePath;

    @Option
    @Credential
    @Documentation("Password for the private key.")
    @ActiveIf(target = "../../../actionConfig.action", value = { "Encrypt", "Validate" }, negate = true)
    private String privateKeyPassword;

}