/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.compatible.debezium.json;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DbzBinlog {
    private Schema schema;
    private Payload payload;

    @Data
    public static class Schema {
        private String type;
        private List<Field> fields;
        private Boolean optional;
        private String name;

        @Data
        public static class Field {
            private String type;
            private Boolean optional;
            private String name;
            private String field;
            private Integer version;

            @JsonAlias({"default"})
            private Object defaultValue;

            private List<Field> fields;
        }
    }

    @Data
    public static class Payload {
        private Map<String, Object> before;
        private Map<String, Object> after;
        private Map<String, Object> source;
        private String op;
        private Long ts_ms;
        private Object transaction;
    }
}
