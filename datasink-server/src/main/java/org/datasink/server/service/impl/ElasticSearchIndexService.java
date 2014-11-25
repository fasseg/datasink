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

import org.datasink.Dataset;
import org.datasink.DatasetVersion;
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

    public static final String INDEX_DATASET_NAME ="datasets";
    public static final String INDEX_DATASET_TYPE ="dataset";
    public static final String INDEX_DATASETVERSION_TYPE ="datasetversion";


    @PostConstruct
    public void init() {
        if (!this.indexExists(INDEX_DATASET_NAME)) {
            this.createIndex(INDEX_DATASET_NAME);
            this.waitForIndex(INDEX_DATASET_NAME);
        }
    }

    @Override
    public void saveOrUpdate(final DatasetVersion version) throws IOException {

        // store the new version in elasticsearch so it can be referenced
        this.client.prepareIndex(INDEX_DATASET_NAME, INDEX_DATASETVERSION_TYPE, version.getVersionId())
                .setSource(this.mapper.writeValueAsBytes(version))
                .execute()
                .actionGet();

        final String datasetId = version.getDatasetId();
        final Dataset ds;
        if (this.exists(datasetId)) {
            log.debug("Updating existing dataset record " + datasetId);
            ds = this.retrieveDataset(datasetId);
            ds.getVersionIds().put(ds.getVersionIds().size() + 1, version.getVersionId());
        } else {
            log.debug("Creating new dataset record " + datasetId);
            ds = new Dataset();
            ds.setId(datasetId);
            ds.setVersionIds(new HashMap<>(1));
            ds.getVersionIds().put(1, version.getVersionId());
        }

        // store the serialization in ElasticSearch
        this.client.prepareIndex(INDEX_DATASET_NAME, INDEX_DATASET_TYPE, ds.getId())
                .setSource(this.mapper.writeValueAsBytes(ds))
                .execute()
                .actionGet();

        // refresh the index so the new record is available
        this.refreshIndex(INDEX_DATASET_NAME);
    }

    @Override
    public void delete(final String id) throws IOException {
        final Dataset fetched = this.retrieveDataset(id);
        // iterate over all the version of the dataset to remove everything
        for (final String versionId : fetched.getVersionIds().values()) {
            this.client.prepareDelete(INDEX_DATASET_NAME, INDEX_DATASETVERSION_TYPE, versionId)
                    .execute()
                    .actionGet();
        }
        // and finally we can remove the dataset object itself
        this.client.prepareDelete(INDEX_DATASET_NAME, INDEX_DATASET_TYPE, id)
            .execute()
            .actionGet();
    }

    @Override
    public boolean versionExists(final String id, int version) throws IOException {
        return false;
    }

    @Override
    public DatasetVersion retrieveDatasetVersion(final String versionId) throws IOException {
        final GetResponse get = this.client.prepareGet(INDEX_DATASET_NAME, INDEX_DATASETVERSION_TYPE, versionId)
                .execute()
                .actionGet();

        if (!get.isExists()) {
            throw new FileNotFoundException("DatasetVersion " + versionId + " can not be found");
        }

        return this.mapper.readValue(get.getSourceAsBytes(), DatasetVersion.class);
    }

    @Override
    public Dataset retrieveDataset(final String id) throws IOException {
        final GetResponse get = this.client.prepareGet(INDEX_DATASET_NAME, INDEX_DATASET_TYPE, id)
                .execute()
                .actionGet();

        if (!get.isExists()) {
            throw new FileNotFoundException("Dataset " + id + " can not be found");
        }

        return this.mapper.readValue(get.getSourceAsBytes(), Dataset.class);
    }

    @Override
    public boolean exists(final String id) {
        return this.client.prepareGet(INDEX_DATASET_NAME, INDEX_DATASET_TYPE, id)
                .execute()
                .actionGet()
                .isExists();
    }

    @Override
    public DatasetVersion retrieveLatestDatasetVersion(final String datasetId) throws IOException {
        final Dataset fetched = this.retrieveDataset(datasetId);
        return this.retrieveDatasetVersion(fetched.getVersionIds().get(fetched.getVersionIds().size()));
    }

}
