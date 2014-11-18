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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

/**
 * @author Frank Asseg
 */
@Configuration
@EnableAutoConfiguration
@EnableGlobalMethodSecurity
@EnableWebSecurity
@ComponentScan(basePackages = "org.datasink")
public class ServerConfiguration {

    @Autowired
    private Environment env;

    @Bean
    public ElasticSearchNode elasticSearchNode() {
        return new ElasticSearchNode();
    }

    @Bean
    public Client elasticSearchClient() {
        return elasticSearchNode().getClient();
    }

    @Bean
    public SeaweedFsMaster weedFsMaster() {
        return new SeaweedFsMaster();
    }

    @Bean
    public SeaweedFsVolume weedFsVolume() {
        return new SeaweedFsVolume();
    }

}
