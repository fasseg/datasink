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
import org.datasink.server.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Frank Asseg
 */
@Controller
@RequestMapping("/dataset")
public class DatasetController {

    @Autowired
    private IndexService indexService;

    @Autowired
    private ObjectMapper mapper;

    @RequestMapping(value = "/{id}/{version}", produces = "application/json", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Dataset retrieveDataset(@PathVariable("id") final String id, @PathVariable("version") final int version) throws IOException {
        return indexService.retrieve(id, version);
    }

    @RequestMapping(method = RequestMethod.POST, produces = "application/json")
    @ResponseStatus(HttpStatus.CREATED)
    public void createDataset(final InputStream src) throws IOException {
        final Dataset ds = this.mapper.readValue(src, Dataset.class);
        if (indexService.exists(ds.getId())) {
            throw new IOException("Dataset already exists");
        }
        this.indexService.saveOrUpdate(ds);
    }
}