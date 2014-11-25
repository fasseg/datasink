/* 
 * Copyright 2014 Frank Asseg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package org.datasink.test.fixtures;

import org.apache.commons.lang3.RandomStringUtils;
import org.datasink.Dataset;
import org.datasink.DatasetVersion;

import java.io.IOException;

/**
 * @author Frank Asseg
 */
public final class Fixtures {
    public static final DatasetVersion randomDataset() throws IOException {
        final DatasetVersion version = new DatasetVersion();
        version.setDatasetId("ds_" + RandomStringUtils.randomAlphabetic(16));
        version.setVersionId("version_" + RandomStringUtils.randomAlphabetic(16));
        return version;
    }
}
