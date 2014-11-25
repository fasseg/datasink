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
package org.datasink.server.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.datasink.Dataset;
import org.datasink.DatasetVersion;
import org.datasink.server.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Frank Asseg
 */
@Controller
public class DatasetController {

    @Autowired
    private IndexService indexService;

    @Autowired
    private ObjectMapper mapper;

    @RequestMapping(value = "/version/{id}", produces = "application/json", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public DatasetVersion retrieveDatasetVersion(@PathVariable("id") final String id) throws IOException {
        return indexService.retrieveLatestDatasetVersion(id);
    }

    @RequestMapping(value="/version", method = RequestMethod.POST, produces = "application/json")
    @ResponseStatus(HttpStatus.CREATED)
    public void createDatasetVersion(final InputStream src) throws IOException {
        final DatasetVersion version = this.mapper.readValue(src, DatasetVersion.class);
        this.indexService.saveOrUpdate(version);
    }

    @RequestMapping(value="/dataset/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public Dataset retrieveDataset(@PathVariable("id") final String datasetId) throws IOException {
        return this.indexService.retrieveDataset(datasetId);
    }

    @RequestMapping(value = "/dataset/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    public void deleteDataset(@PathVariable("id") final String id) throws IOException {
        this.indexService.delete(id);
    }
}
