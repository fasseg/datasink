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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.datasink.Dataset;
import org.datasink.server.SecurityConfiguration;
import org.datasink.server.ServerConfiguration;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;

/**
 * @author Frank Asseg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {ServerConfiguration.class, SecurityConfiguration.class})
@IntegrationTest
@WebAppConfiguration
public class AbstractDatasinkIT {
    private final String hostName = "localhost";
    private final String contextPath = "/datasink";
    private final int port = 8080;
    private final String serverUrl = "http://" + hostName + ":" + port + contextPath;
    private final String username = "admin";
    private final String password = username;
    private final Executor executor;

    @Autowired
    private ObjectMapper mapper;

    public AbstractDatasinkIT() {
        final HttpHost local = new HttpHost("localhost");
        this.executor = Executor.newInstance()
                .auth(local, username, password)
                .authPreemptive(local);
    }

    protected HttpResponse postDataset(final Dataset ds) throws IOException {
        return this.executor.execute(Request.Post(serverUrl + "/dataset")
                    .bodyString(this.mapper.writeValueAsString(ds), ContentType.APPLICATION_JSON))
                .returnResponse();
    }

    public HttpResponse retrieveDataset(final String id) throws IOException {
        return this.executor.execute(Request.Get(serverUrl + "/dataset/" + id))
                .returnResponse();
    }
}
