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
package org.talend.components.http.configuration;

import java.io.Serializable;
import java.util.List;
import javax.ws.rs.DefaultValue;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.UIScope;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import lombok.Data;

@Data
@Version(Datastore.VERSION)
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "downloadFile", "directoryToSave" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED,
        value = { @GridLayout.Row({ "dataset" }), @GridLayout.Row("uploadFiles"),
                @GridLayout.Row("uploadFileTable"), @GridLayout.Row("dieOnError") })
public class RequestConfig implements Serializable {

    @Option
    @Documentation("Dataset configuration.")
    private Dataset dataset = new Dataset();

    @Option
    @Documentation("Die on error.")
    private boolean dieOnError;

    @Option
    @DefaultValue("false")
    @Documentation("Download attachments.")
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_SCOPE })
    private boolean downloadFile;

    @Option
    @Documentation("Directory to save attachments.")
    @ActiveIf(target = "downloadFile", value = "true")
    private String directoryToSave;

    @Option
    @Documentation("Upload files.")
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_SCOPE })
    private boolean uploadFiles;

    @Option
    @Documentation("Table to attach files.")
    @ActiveIf(target = "uploadFiles", value = "true")
    private List<UploadFile> uploadFileTable;
}
