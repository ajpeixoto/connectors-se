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
package org.talend.components.adlsgen2.input;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.talend.components.adlsgen2.runtime.input.BlobReader;

class InputMapperTest {

    private InputMapper sut;

    private InputConfiguration configuration;

    @BeforeEach
    void setUp() {
        configuration = new InputConfiguration();
        sut = new InputMapper(configuration, null, null, null);
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
        BlobReader readerMock = Mockito.mock();
        try (
                MockedStatic<BlobReader.BlobFileReaderFactory> mockedStatic =
                        Mockito.mockStatic(BlobReader.BlobFileReaderFactory.class);) {
            mockedStatic.when(() -> BlobReader.BlobFileReaderFactory.getReader(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(readerMock);
            AdlsGen2Input result = sut.createWorker();
            result.init();
            result.next();
            Mockito.verify(readerMock).readRecord();

        }
    }

}
