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
package org.datasink.server;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Frank Asseg
 */
public class ElasticSearchNode {

    private Node node;

    @Autowired
    private Environment env;

    @PostConstruct
    public void startNode() {
        node =
                NodeBuilder.nodeBuilder()
                        .clusterName(env.getProperty("elasticsearch.cluster.name"))
                        .settings(
                            ImmutableSettings.settingsBuilder()
                            .put("path.logs", env.getProperty("elasticsearch.path.logs"))
                            .put("path.data", env.getProperty("elasticsearch.path.data"))
                            .put("bootstrap.mlockall", env.getProperty("elasticsearch.bootstrap.mlockall"))
                            .put("network.bind.host", env.getProperty("elasticsearch.network.bind.host"))
                            .put("network.host", env.getProperty("elasticsearch.network.host", ""))
                            .put("gateway.expected_nodes", env.getProperty("elasticsearch.gateway.expected_nodes"))
                            .put("http.port", env.getProperty("elasticsearch.http.port"))
                            .put("http.enabled", env.getProperty("elasticsearch.http.enabled"))
                            .put("transport.tcp.port", env.getProperty("elasticsearch.transport.tcp.port", ""))
                            .put("network.publish_host", env.getProperty("elasticsearch.network.publish_host", ""))
                            .put("gateway.type",env.getProperty("elasticsearch.gateway.type"))
                        )
                        .node();

        // wait for the cluster to become active
        this.node.client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForYellowStatus()
                .execute()
                .actionGet();
    }

    @PreDestroy
    public void stopNode() {
        if (node != null) {
            node.stop();
        }
    }

    public Client getClient() {
        return node.client();
    }

    public boolean isAlive() {
        return node != null && !node.isClosed();
    }}
