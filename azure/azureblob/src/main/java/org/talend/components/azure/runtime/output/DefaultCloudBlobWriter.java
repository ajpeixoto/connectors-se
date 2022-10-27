/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
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
package org.talend.components.azure.runtime.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class DefaultCloudBlobWriter implements CloudBlobWriter {

    private final CloudBlob cloud;

    public DefaultCloudBlobWriter(CloudBlob cloud) {
        this.cloud = cloud;
    }

    @Override
    public void upload(byte[] buffer) throws IOException, StorageException {
        this.cloud.uploadFromByteArray(buffer, 0, buffer.length);
    }

    @Override
    public void append(byte[] contentBytes, OperationContext opContext) throws IOException, StorageException {
        if (!(cloud instanceof CloudAppendBlob)) {
            throw new IllegalStateException("Can't append, not a CloudAppendBlob but " + cloud.getClass().getName());
        }
        ((CloudAppendBlob) this.cloud).appendFromByteArray(contentBytes, 0, contentBytes.length, null, null,
                opContext);
    }

    @Override
    public void upload(InputStream sourceStream) throws StorageException, IOException {
        this.cloud.upload(sourceStream, -1);
    }

    @Override
    public void appendText(String content) throws StorageException, IOException {
        if (!(cloud instanceof CloudAppendBlob)) {
            throw new IllegalStateException("Can't append, not a CloudAppendBlob but " + cloud.getClass().getName());
        }
        ((CloudAppendBlob) this.cloud).appendText(content);
    }

    @Override
    public void onOutput(final OutputFunction action) throws IOException, StorageException {
        if (!(cloud instanceof CloudBlockBlob)) {
            throw new IllegalStateException(
                    "Can't open outputstream, not CloudBlockBlob but " + cloud.getClass().getName());
        }

        try (OutputStream blobOutputStream = ((CloudBlockBlob) this.cloud).openOutputStream()) {
            action.onOutputStream(blobOutputStream);
            blobOutputStream.flush();
        }
    }
}
