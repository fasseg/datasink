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
package org.datasink.integration;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.datasink.test.fixtures.Fixtures;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Frank Asseg
 */
public class DatasetIT extends AbstractDatasinkIT {
    @Test
    public void testIngest() throws Exception {
        final HttpResponse resp =  this.postDataset(Fixtures.randomDataset());
        assertEquals(HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());
    }
}