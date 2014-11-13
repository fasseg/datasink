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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.datasink.Dataset;
import org.datasink.server.service.IndexService;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * IndexService implementation for ElasticSearch
 * @author Frank Asseg
 */
@Service
public class ElasticSearchIndexService implements IndexService {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchIndexService.class);

    public static final String INDEX_NAME="datasets";
    public static final String INDEX_TYPE="dataset";

    @Autowired
    private Client client;

    @Autowired
    private ObjectMapper mapper;

    @Override
    public void saveOrUpdate(Dataset ds) throws IOException {

        if (this.exists(ds.getId())) {
            log.debug("Updating existing dataset record " + ds.getId());
        } else {
            log.debug("Creating new dataset record " + ds.getId());
        }

        // store the serialization in ElasticSearch
        this.client.prepareIndex(INDEX_NAME, INDEX_TYPE, ds.getId())
                .setSource(this.mapper.writeValueAsBytes(ds))
                .execute()
                .actionGet();
    }

    @Override
    public void delete(final String id, int version) throws IOException {
        if (!this.versionExists(id, version)) {
            throw new FileNotFoundException("Dataset " + id + " with version " + version + " can not be found");
        }
        this.client.prepareDelete(INDEX_NAME, INDEX_TYPE, id);
    }

    @Override
    public boolean versionExists(final String id, int version) throws IOException {
        return false;
    }

    @Override
    public Dataset retrieve(final String id, final int version) throws IOException {
        final GetResponse get = this.client.prepareGet(INDEX_NAME, INDEX_TYPE, id + "::" + version)
                .execute()
                .actionGet();

        if (!get.isExists()) {
            throw new FileNotFoundException("Dataset " + id + " can not be found");
        }

        return this.mapper.readValue(get.getSourceAsBytes(), Dataset.class);
    }

    @Override
    public boolean exists(final String id) {
        return this.client.prepareGet(INDEX_NAME, INDEX_TYPE, id)
                .execute()
                .actionGet()
                .isExists();
    }


}
