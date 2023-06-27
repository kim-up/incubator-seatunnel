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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinlogJsonConverter implements Converter, Serializable {

    private ObjectMapper objectMapper;

    public DbzKeyBinlog deserializeKey(String key) {
        tryInit();
        DbzKeyBinlog dbzBinlog = null;
        try {
            dbzBinlog = objectMapper.readValue(key, DbzKeyBinlog.class);
        } catch (JsonProcessingException e) {
            log.error("deserialize error", e);
        }
        return dbzBinlog;
    }

    public DbzBinlog deserializeValue(String value) {
        tryInit();
        DbzBinlog dbzBinlog = null;
        try {
            dbzBinlog = objectMapper.readValue(value, DbzBinlog.class);
        } catch (JsonProcessingException e) {
            log.error("deserialize error", e);
        }
        return dbzBinlog;
    }

    @Override
    public String convert(Object key, Object value) throws JsonProcessingException {
        DbzKeyBinlog dbzKeyBinlog = deserializeKey((String) key);
        DbzBinlog dbzBinlog = deserializeValue((String) value);
        tryInit();
        BinLogMessage binLogMessage = new BinLogMessage();
        setOthers(binLogMessage, dbzBinlog);
        setData(binLogMessage, dbzBinlog);
        setOld(binLogMessage, dbzBinlog);
        setPkNames(binLogMessage, dbzKeyBinlog);
        setSqlType(binLogMessage, dbzBinlog);
        return objectMapper.writeValueAsString(binLogMessage);
    }

    private void setOthers(final BinLogMessage binLogMessage, final DbzBinlog dbzBinlog) {
        binLogMessage.setDatabase(String.valueOf(dbzBinlog.getPayload().getSource().get("db")));
        binLogMessage.setEs(dbzBinlog.getPayload().getTs_ms());
        binLogMessage.setIsDdl(false);
        binLogMessage.setTable(String.valueOf(dbzBinlog.getPayload().getSource().get("table")));
        binLogMessage.setTs(dbzBinlog.getPayload().getTs_ms());
        binLogMessage.setType(operationType(dbzBinlog.getPayload().getOp()));
    }

    private BinLogMessage.operationType operationType(final String op) {
        switch (op) {
            case "u":
                return BinLogMessage.operationType.UPDATE;
            case "c":
                return BinLogMessage.operationType.INSERT;
            case "d":
                return BinLogMessage.operationType.DELETE;
            case "r":
                return BinLogMessage.operationType.QUERY;
            default:
                // TODO
                return null;
        }
    }

    private void setSqlType(final BinLogMessage binLogMessage, final DbzBinlog dbzBinlog) {
        if (binLogMessage.getType() == BinLogMessage.operationType.INSERT) {
            Map<String, Object> sqlType = new HashMap<>();
            dbzBinlog.getSchema().getFields().stream()
                     .filter(f -> "after".equals(f.getField()))
                     .findFirst()
                     .ifPresent(
                         f -> {
                             f.getFields()
                              .forEach(
                                  field -> {
                                      sqlType.put(
                                          field.getField(),
                                          dbzType2SqlType(
                                              field.getName(),
                                              field.getType()
                                          )
                                      );
                                  });
                         });
            binLogMessage.setSqlType(sqlType);
        }
    }

    private Object dbzType2SqlType(String name, final String type) {

        switch (type) {
            case "int8":
                return Types.TINYINT;
            case "int16":
                return Types.SMALLINT;
            case "int32":
                return Types.INTEGER;
            case "int64":
                if ("io.debezium.time.Timestamp".equals(name)) {
                    return Types.TIMESTAMP;
                }
                return Types.BIGINT;
            case "float32":
                return Types.FLOAT;
            case "float64":
                return Types.DOUBLE;
            case "boolean":
                return Types.BOOLEAN;
            case "string":
                return Types.VARCHAR;
            case "bytes":
                return Types.BINARY;
            case "array":
                return Types.ARRAY;
            case "map":
            case "struct":
                return Types.STRUCT;
            default:
                return null;
        }
    }

    private void setPkNames(final BinLogMessage binLogMessage, final DbzKeyBinlog dbzKeyBinlog) {
        List<String> pkNames = new ArrayList<>();
        dbzKeyBinlog.getSchema().getFields().forEach(f -> pkNames.add(f.getField()));
        binLogMessage.setPkNames(pkNames);
    }

    private void setData(final BinLogMessage binLogMessage, final DbzBinlog dbzBinlog) {
        binLogMessage.setData(dbzBinlog.getPayload().getAfter());
    }

    private void setOld(final BinLogMessage binLogMessage, final DbzBinlog dbzBinlog) {
        binLogMessage.setOld(dbzBinlog.getPayload().getBefore());
    }

    private void tryInit() {
        if (objectMapper == null) {
            synchronized (this) {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper();
                    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
                }
            }
        }
    }

}
