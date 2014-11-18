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
package org.datasink.server.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.datasink.DataSetVersion;
import org.datasink.DataSet;
import org.datasink.server.service.IndexService;
import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * IndexService implementation for ElasticSearch
 * @author Frank Asseg
 */
@Service
public class ElasticSearchIndexService extends AbstractElasticSearchService  implements IndexService {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchIndexService.class);

    public static final String INDEX_NAME="datasets";
    public static final String INDEX_TYPE_DATASET = "dataset";
    public static final String INDEX_TYPE_VERSION = "version";


    @PostConstruct
    public void init() {
        if (!this.indexExists(INDEX_NAME)) {
            this.createIndex(INDEX_NAME);
            this.waitForIndex(INDEX_NAME);
        }
    }

    @Override
    public void saveOrUpdate(final DataSetVersion ds) throws IOException {

        // find the current version to know which version we are dealing with
        final int versionNumber = this.lastVersionOf(ds.getId()) + 1;
        final DataSetVersion v = ds.newVersion(versionNumber);
        this.client.prepareIndex(INDEX_NAME, INDEX_TYPE_VERSION, ds.getId() + ":" + versionNumber)
                .setSource(this.mapper.writeValueAsBytes(v))
                .execute()
                .actionGet();

        // for versioning a hierarchical parent must be created for each dataset
        final DataSet vds;
        if (this.exists(v.getId())) {
            log.debug("Updating existing DataSet record " + v.getId());
            vds = this.retrieveDataset(v.getId());
        } else {
            log.debug("Creating new DataSet record " + v.getId());
            final HashMap<Integer, DataSetVersion> versions = new HashMap<>(1);
            versions.put(1, v);
            vds = new DataSet.Builder()
                    .id(ds.getId())
                    .versions(versions)
                    .build();
        }

        // store the serialization in ElasticSearch
        this.client.prepareIndex(INDEX_NAME, INDEX_TYPE_DATASET, vds.getId())
                .setSource(this.mapper.writeValueAsBytes(vds))
                .execute()
                .actionGet();

        // refresh the index so the new record is available
        this.refreshIndex(INDEX_NAME);
    }

    @Override
    public void delete(final String id) throws IOException {
        this.client.prepareDelete(INDEX_NAME, INDEX_TYPE_DATASET, id)
            .execute()
            .actionGet();
    }

    @Override
    public boolean versionExists(final String id, int version) throws IOException {
        return false;
    }

    @Override
    public DataSetVersion retrieve(final String id) throws IOException {
        // find the last version and return the latest
        final String versionId = id + ":" +  this.lastVersionOf(id);
        final GetResponse get = this.client.prepareGet(INDEX_NAME, INDEX_TYPE_VERSION, versionId)
                .execute()
                .actionGet();
        if (!get.isExists()) {
            throw new FileNotFoundException("Dataset " + versionId + " can not be found");
        }
        return this.mapper.readValue(get.getSourceAsBytes(), DataSetVersion.class);
    }

    @Override
    public DataSet retrieveDataset(String id) throws IOException {
        final GetResponse get = this.client.prepareGet(INDEX_NAME, INDEX_TYPE_DATASET, id)
                .execute()
                .actionGet();

        if (!get.isExists()) {
            return null;
        }

        return this.mapper.readValue(get.getSourceAsBytes(), DataSet.class);
    }

    @Override
    public boolean exists(final String id) {
        return this.client.prepareGet(INDEX_NAME, INDEX_TYPE_DATASET, id)
                .execute()
                .actionGet()
                .isExists();
    }

    @Override
    public int lastVersionOf(final String id) throws IOException{
        final DataSet vds = this.retrieveDataset(id);
        return vds == null ? 0 : vds.getVersions().size();
    }
}
