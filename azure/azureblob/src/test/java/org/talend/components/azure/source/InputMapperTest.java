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
package org.talend.components.azure.source;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.azure.runtime.input.BlobFileReader;

import static org.junit.jupiter.api.Assertions.*;

class InputMapperTest {

    private InputMapper sut;

    private BlobInputProperties properties;

    @BeforeEach
    void setUp() {
        properties = new BlobInputProperties();
        sut = new InputMapper(properties, null, null, null);
    }

    @Test
    void testEstimateSize() {
        Assertions.assertEquals(1L, sut.estimateSize());
    }

    @Test
    void testSplit() {
        List<InputMapper> result = sut.split(Long.MAX_VALUE);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(sut, result.get(0));
    }

    @Test
    void testCreateWorker() throws Exception {
        BlobFileReader fileReaderMock = Mockito.mock();
        try (MockedStatic<BlobFileReader.BlobFileReaderFactory> mockedStatic =
                Mockito.mockStatic(BlobFileReader.BlobFileReaderFactory.class);) {
            mockedStatic.when(() -> BlobFileReader.BlobFileReaderFactory.getReader(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(fileReaderMock);
            BlobSource result = sut.createWorker();
            result.init();
            result.next();
            Mockito.verify(fileReaderMock).readRecord();

        }
    }
}