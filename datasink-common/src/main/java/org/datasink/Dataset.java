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

/**
 * The basic unit of storage in Datasink
 * @author Frank Asseg
 */
public class Dataset {

    private final String id;
    private final String label;
    private final int version;

    private Dataset(Builder builder) {
        this.id = builder.id;
        this.label = builder.label;
        this.version = builder.version;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public int getVersion() {
        return version;
    }

    public static class Builder {

        private final String id;
        private String label;
        private int version;


        public Builder(final String id) {
            this.id = id;
        }

        public Builder label(final String label) {
            this.label = label;
            return this;
        }

        public Builder version(final int version) {
            this.version = version;
            return this;
        }

        public Dataset build() {
            return new Dataset(this);
        }
    }
}
