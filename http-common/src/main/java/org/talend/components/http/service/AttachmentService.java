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
package org.talend.components.http.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.Part;
import javax.mail.internet.ContentDisposition;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.ws.rs.core.MediaType;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AttachmentService {

    @Service
    private I18n i18n;

    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition";

    @Service
    RecordBuilderService recordBuilderService;

    public boolean isMultipart(MediaType mediaType) {
        return "multipart".equals(mediaType.getType());
    }

    public boolean isMultipart(String mediaType) {
        return mediaType != null && mediaType.toLowerCase().startsWith("multipart");
    }

    public String parseMultipartAndDownloadAttachments(String mediaType, String multipartBody,
            final RequestConfig config) {
        String notAttachmentBody = null;
        try {
            MimeMultipart multipart = new MimeMultipart(new ByteArrayDataSource(multipartBody, mediaType));

            boolean isMainPartParsed = false;
            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bp = multipart.getBodyPart(i);
                if (isMultipart(bp.getContentType())) {
                    String nestedMultipartBody = convertInputStreamToString(bp.getInputStream(),
                            "UTF-8", true);

                    parseMultipartAndDownloadAttachments(bp.getContentType(), nestedMultipartBody, config);
                } else if (isBodyPartAnAttachment(bp) || isMainPartParsed) {
                    saveAttachmentFromResponsePart(bp, config, i);
                } else {
                    notAttachmentBody = convertInputStreamToString(bp.getInputStream(),
                            "UTF-8", true);

                    isMainPartParsed = true;
                }
            }

        } catch (MessagingException | IOException e) {
            throw new ComponentException(e);
        }

        return notAttachmentBody;
    }

    private void saveAttachmentFromResponsePart(BodyPart bp, RequestConfig config, int multipartIndex)
            throws MessagingException {
        String attachmentName = getAttachmentName(bp, config.getDataset().getResource(), multipartIndex);

        String resultFileName = getResultAttachmentFilePath(config.getDirectoryToSave(), attachmentName);
        File fileToWrite = new File(resultFileName);
        if (!fileToWrite.getParentFile().exists()) {
            fileToWrite.getParentFile().mkdirs();
        }
        if (fileToWrite.exists()) {
            log.warn(i18n.attachmentAlreadyExists(attachmentName));
        } else {
            try (FileOutputStream fos = new FileOutputStream(fileToWrite);
                    InputStream is = bp.getInputStream()) {
                int charInt;
                while ((charInt = is.read()) != -1) {
                    fos.write(charInt);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String getResultAttachmentFilePath(String directoryToSave, String attachmentName) {
        if (!(directoryToSave.endsWith("/") || directoryToSave.endsWith("\\"))) {
            directoryToSave = directoryToSave + File.separator;
        }
        return directoryToSave + attachmentName;
    }

    private String getAttachmentName(BodyPart bp, String resourceName, int index) throws MessagingException {
        String attachmentName = bp.getFileName();

        if (attachmentName == null && bp.getHeader(CONTENT_DISPOSITION_HEADER) != null) {
            String header = bp.getHeader(CONTENT_DISPOSITION_HEADER)[0];
            ContentDisposition cd = new ContentDisposition(header);
            attachmentName = cd.getParameter("name");
        }
        if (attachmentName == null) {
            attachmentName = getDefaultAttachmentName(resourceName, index);
        }

        return attachmentName;
    }

    private String getDefaultAttachmentName(String resourceName, int number) {
        if (!resourceName.endsWith("/")) {
            return resourceName.substring(resourceName.lastIndexOf('/') + 1) + "_" + number;
        } else {
            String resourceNameWithoutEndingSlash = resourceName.substring(0, resourceName.length() - 1);
            if (resourceNameWithoutEndingSlash.contains("/")) {
                return resourceNameWithoutEndingSlash.substring(resourceNameWithoutEndingSlash.lastIndexOf("/" + 1));
            } else {
                return resourceNameWithoutEndingSlash;
            }
        }
    }

    private boolean isBodyPartAnAttachment(BodyPart bp) {
        try {
            return bp.getDisposition() != null && bp.getDisposition().equalsIgnoreCase(Part.ATTACHMENT);
        } catch (MessagingException e) {
            log.warn("Messaging exception", e);
            return false;
        }
    }

    /**
     *
     * @param is InputStream from HTTP response
     * @param charset Charset to decode payload input stream
     * @param null2Empty if true, return empty string if input stream is null
     * @return The String representation of the input stream
     * @throws IOException
     */
    private String convertInputStreamToString(InputStream is, String charset, boolean null2Empty) throws IOException {
        if (is == null) {
            if (!null2Empty) {
                return null;
            } else {
                is = new ByteArrayInputStream(new byte[0]);
            }
        }

        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        is.close();
        String content = result.toString(charset);
        return content;
    }

}
