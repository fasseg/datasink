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

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Frank Asseg
 */
public abstract class AbstractElasticSearchService {
    @Autowired
    protected Client client;

    @Autowired
    protected ObjectMapper mapper;

    protected void createIndex(final String indexName) {
        try {

            client.admin()
                    .indices()
                    .create(new CreateIndexRequest(indexName))
                    .actionGet();

        } catch (final ElasticsearchException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected boolean indexExists(final String indexName) {
        try {

            return client.admin()
                    .indices()
                    .exists(new IndicesExistsRequest(indexName))
                    .actionGet()
                    .isExists();

        } catch (final ElasticsearchException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void refreshIndex(String... indices) throws IOException {
        try {
            client.admin()
                    .indices()
                    .refresh(new RefreshRequest(indices))
                    .actionGet();
        } catch (ElasticsearchException ex) {
            throw new IOException(ex.getMostSpecificCause().getMessage());
        }
    }

    protected void waitForIndex(String indexName){
        try {
            this.client.admin().cluster().prepareHealth(indexName).setWaitForYellowStatus().execute().actionGet();
        } catch (ElasticsearchException ex) {
            throw new RuntimeException(ex);
        }
    }
}
