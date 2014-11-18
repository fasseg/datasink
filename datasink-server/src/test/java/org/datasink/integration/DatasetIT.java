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

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.datasink.Dataset;
import org.datasink.test.fixtures.Fixtures;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Frank Asseg
 */
public class DatasetIT extends AbstractDatasinkIT {

    @Autowired
    private ObjectMapper mapper;

    @Test
    public void testIngest() throws Exception {
        final HttpResponse resp =  this.postDataset(Fixtures.randomDataset());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());
    }

    @Test
    public void testRetrieve() throws Exception {
        final Dataset ds = Fixtures.randomDataset();

        HttpResponse resp =  this.postDataset(ds);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        resp = this.retrieveDataset(ds.getId());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());

        final Dataset fetched = this.mapper.readValue(resp.getEntity().getContent(), Dataset.class);
        assertNotNull(fetched);
        assertEquals(ds.getId(), fetched.getId());
        assertEquals(ds.getLabel(), fetched.getLabel());
        assertEquals(ds.getVersion(), fetched.getVersion());
    }

    @Test
    public void testDelete() throws Exception {
        final Dataset ds = Fixtures.randomDataset();
        HttpResponse resp =  this.postDataset(ds);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        resp = this.deleteDataset(ds.getId());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
    }
}
