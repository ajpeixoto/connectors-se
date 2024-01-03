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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.http.TestUtil;
import org.talend.components.http.configuration.Dataset;
import org.talend.components.http.configuration.Datastore;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.http")
class AttachmentServiceTest {

    @Service
    private AttachmentService attachmentService;

    private RequestConfig config;

    String directoryToSaveAttachments = "src/test/resources/attachments";

    String expectedNotAttachmentPartOfMultipartResponse;

    @BeforeEach
    public void setUp() {
        cleanUp();

        File directoryForAttachments = new File(directoryToSaveAttachments);
        if (!directoryForAttachments.exists()) {
            directoryForAttachments.mkdir();
        }

        expectedNotAttachmentPartOfMultipartResponse = TestUtil
                .loadResource("/org/talend/components/http/service/notAttachmentPartOfMultipartBody.txt");

        Datastore datastore = new Datastore();
        datastore.setBase("https://someurl.org");
        Dataset dataset = new Dataset();
        dataset.setResource("resourceParent/someResource");
        dataset.setDatastore(datastore);
        config = new RequestConfig();
        config.setDataset(dataset);
        config.setDirectoryToSave("src/test/resources/attachments");
        config.setDownloadFile(true);
    }

    @Test
    void testIsMultipartMediaTypesString() {
        String multipartMediaTypeString = "multipart/mixed";
        String notMultipartMediaTypeString = "text/xml";

        Assertions.assertTrue(attachmentService.isMultipart(multipartMediaTypeString));
        Assertions.assertFalse(attachmentService.isMultipart(notMultipartMediaTypeString));
    }

    @Test
    void testIsMultipartMediaType() {
        MediaType multipartMediaType = new MediaType("multipart", "mixed");
        MediaType notMultipartMediaType = new MediaType("text", "xml");

        Assertions.assertTrue(attachmentService.isMultipart(multipartMediaType));
        Assertions.assertFalse(attachmentService.isMultipart(notMultipartMediaType));
    }

    @Test
    void testProcessMultipartResponseWithOneAttachment() {
        String multipartBodyWithOneAttachment = TestUtil
                .loadResource("/org/talend/components/http/service/multipartSoapBodyWithOneAttachment.txt");
        String expectedAttachmentName = "1.txt";
        String expectedAttachmentContent = "123";

        String notAttachmentPartOfMultipartBody = attachmentService
                .parseMultipartAndDownloadAttachments("multipart/related", multipartBodyWithOneAttachment,
                        config);

        Assertions.assertEquals(expectedNotAttachmentPartOfMultipartResponse, notAttachmentPartOfMultipartBody);
        File directoryWithAttachments = new File(directoryToSaveAttachments);
        File[] attachments = Objects.requireNonNull(directoryWithAttachments.listFiles());
        Assertions.assertEquals(1, attachments.length);
        File attachment = attachments[0];
        Assertions.assertEquals(expectedAttachmentName, attachment.getName());
        Assertions.assertEquals(expectedAttachmentContent.length(), attachment.length());
    }

    @Test
    void testProcessMultipartResponseWithOneAttachmentWithoutContentDispositionHeader() {
        String multipartBodyWithOneAttachment = TestUtil
                .loadResource("/org/talend/components/http/service/"
                        + "multipartSoapBodyWithOneAttachmentNoCDHeader.txt");
        String expectedAttachmentName = "someResource_1"; // part of dataset.getResource() after the last slash + '_1'
        String expectedAttachmentContent = "123";

        String notAttachmentPartOfMultipartBody = attachmentService
                .parseMultipartAndDownloadAttachments("multipart/related", multipartBodyWithOneAttachment,
                        config);

        Assertions.assertEquals(expectedNotAttachmentPartOfMultipartResponse, notAttachmentPartOfMultipartBody);
        File directoryWithAttachments = new File(directoryToSaveAttachments);
        File[] attachments = Objects.requireNonNull(directoryWithAttachments.listFiles());
        Assertions.assertEquals(1, attachments.length);
        File attachment = attachments[0];
        Assertions.assertEquals(expectedAttachmentName, attachment.getName());
        Assertions.assertEquals(expectedAttachmentContent.length(), attachment.length());
    }

    @Test
    void testProcessMultipartResponseWithTwoAttachments() {
        String multipartBodyWithOneAttachment = TestUtil
                .loadResource("/org/talend/components/http/service/multipartSoapBodyWithTwoAttachments.txt");
        String expectedAttachmentName1 = "1.txt";
        String expectedAttachmentName2 = "2.txt";
        String expectedAttachmentContent1 = "123";
        String expectedAttachmentContent2 = "4567";

        String notAttachmentPartOfMultipartBody = attachmentService
                .parseMultipartAndDownloadAttachments("multipart/related", multipartBodyWithOneAttachment,
                        config);

        Assertions.assertEquals(expectedNotAttachmentPartOfMultipartResponse, notAttachmentPartOfMultipartBody);
        File directoryWithAttachments = new File(directoryToSaveAttachments);
        File[] attachments = Objects.requireNonNull(directoryWithAttachments.listFiles());

        Assertions.assertEquals(2, attachments.length);

        Map<String, File> attachmentsFileMap = new HashMap<>();

        Arrays.stream(attachments).forEach(file -> attachmentsFileMap.put(file.getName(), file));

        Assertions.assertEquals(2, attachmentsFileMap.size());

        Assertions.assertTrue(attachmentsFileMap.containsKey(expectedAttachmentName1));
        Assertions.assertTrue(attachmentsFileMap.containsKey(expectedAttachmentName2));

        Assertions.assertEquals(expectedAttachmentContent1.length(),
                attachmentsFileMap.get(expectedAttachmentName1).length());
        Assertions.assertEquals(expectedAttachmentContent2.length(),
                attachmentsFileMap.get(expectedAttachmentName2).length());

    }

    @Test
    void tesNotOverwriteWhenFileAlreadyExist() throws IOException {
        String multipartBodyWithOneAttachment = TestUtil
                .loadResource("/org/talend/components/http/service/multipartSoapBodyWithOneAttachment.txt");
        String expectedAttachmentName = "1.txt";

        File alreadyExistingFile = new File(directoryToSaveAttachments + "/" + expectedAttachmentName);
        Assertions.assertFalse(alreadyExistingFile.exists());
        alreadyExistingFile.createNewFile();
        Assertions.assertTrue(alreadyExistingFile.exists());
        Assertions.assertEquals(0, alreadyExistingFile.length());

        attachmentService
                .parseMultipartAndDownloadAttachments("multipart/related", multipartBodyWithOneAttachment,
                        config);

        Assertions.assertEquals(0, alreadyExistingFile.length());

    }

    @AfterEach
    public void cleanUp() {
        File directoryWithAttachments = new File(directoryToSaveAttachments);
        if (directoryWithAttachments.exists() && directoryWithAttachments.isDirectory()) {
            for (File attachment : Objects.requireNonNull(directoryWithAttachments.listFiles())) {
                boolean fileDeleted = attachment.delete();
                Assertions.assertTrue(fileDeleted,
                        "Test file " + attachment.getName() + " has not been deleted");
            }
        }
    }
}