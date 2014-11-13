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
 * A Collection aggregates multiple Datasets
 * @author Frank Asseg
 */
public class Collection {
    private final String id;
    private final String label;

    private Collection(Builder builder) {
        this.id = builder.id;
        this.label = builder.label;
    }

    public String getLabel() {
        return label;
    }

    public String getId() {
        return id;
    }

    public static class Builder {
        private final String id;
        private String label;

        public Builder(final String id) {
            this.id = id;
        }

        public Builder label(final String label) {
            this.label = label;
            return this;
        }

        public Collection build() {
            return new Collection(this);
        }
    }
}
