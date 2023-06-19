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

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class BinLogMessage {
    private Map<String, Object> data;
    private Map<String, Object> old;
    private String database;
    private Long es;
    private Boolean isDdl;
    private List<String> pkNames;
    private Map<String, Object> sqlType;
    private String table;
    private Long ts;
    private operationType type;

    public enum operationType {
        /** Insert row. */
        INSERT("I"),

        /** Update row. */
        UPDATE("U"),

        /** Delete row. */
        DELETE("D"),

        /** Create table. */
        CREATE("C"),

        /** Alter table. */
        ALTER("A"),

        /** Erase table. */
        ERASE("E"),

        /** Query. */
        QUERY("Q"),

        /** Truncate. */
        TRUNCATE("T"),

        /** rename. */
        RENAME("R"),

        /** create index. */
        CINDEX("CI"),

        /** drop index. */
        DINDEX("DI");

        private String name;

        operationType(final String name) {
            this.name = name;
        }
    }
}
