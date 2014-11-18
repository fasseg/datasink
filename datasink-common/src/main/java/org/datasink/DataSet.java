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
package org.datasink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

/**
 * @author Frank Asseg
 */
@JsonDeserialize(builder = DataSet.Builder.class)
public class DataSet {
    private final String id;
    private final Map<Integer, DataSetVersion> versions;

    private DataSet(Builder b) {
        this.versions = b.versions;
        this.id = b.id;
    }

    public Map<Integer, DataSetVersion> getVersions() {
        return versions;
    }

    public String getId() {
        return id;
    }

    public static class Builder {
        private Map<Integer, DataSetVersion> versions;

        private String id;

        @JsonProperty("id")
        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        @JsonProperty("versions")
        public Builder versions(final Map<Integer, DataSetVersion> versions) {
            this.versions = versions;
            return this;
        }

        public DataSet build() {
            if (id == null || id.isEmpty()) {
                throw new IllegalArgumentException("Dataset id can not be empty");
            }
            if (versions == null || versions.size() == 0) {
                throw new IllegalArgumentException("Dataset versions can not be empty");
            }
            return new DataSet(this);
        }
    }
}
