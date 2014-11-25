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

import junit.framework.Assert;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.datasink.Dataset;
import org.datasink.DatasetVersion;
import org.datasink.test.fixtures.Fixtures;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

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
        final DatasetVersion version = Fixtures.randomDataset();

        HttpResponse resp =  this.postDataset(version);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        resp = this.retrieveLatestDatasetVersion(version.getDatasetId());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());

        final DatasetVersion fetched = this.mapper.readValue(resp.getEntity().getContent(), DatasetVersion.class);
        assertNotNull(fetched);
        assertEquals(version.getVersionId(), fetched.getVersionId());
        assertEquals(version.getLabel(), fetched.getLabel());
        assertEquals(version.getDatasetId(), fetched.getDatasetId());
    }

    @Test
    public void testDelete() throws Exception {
        final DatasetVersion version = Fixtures.randomDataset();
        HttpResponse resp =  this.postDataset(version);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        resp = this.deleteDataset(version.getDatasetId());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
    }

    @Test
    public void testUpdate() throws IOException {
        final DatasetVersion ver1 = Fixtures.randomDataset();
        HttpResponse resp = this.postDataset(ver1);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        ver1.setLabel("Updated label");
        ver1.setVersionId("version_" + RandomStringUtils.randomAlphabetic(16));
        resp = this.postDataset(ver1);
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());

        resp = this.retrieveDataset(ver1.getDatasetId());
        assertEquals(EntityUtils.toString(resp.getEntity()), HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());

        final Dataset ds = this.mapper.readValue(resp.getEntity().getContent(), Dataset.class);
        assertEquals(2, ds.getVersionIds().size());
        assertNotNull(ds.getVersionIds().get(1));
        assertNotNull(ds.getVersionIds().get(2));
    }
}
