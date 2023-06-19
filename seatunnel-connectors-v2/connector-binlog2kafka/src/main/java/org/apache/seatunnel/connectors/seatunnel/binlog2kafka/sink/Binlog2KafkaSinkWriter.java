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

package org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.convert.BinlogJsonConverter;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.state.KafkaSinkState;
import org.apache.seatunnel.format.compatible.debezium.json.DebeziumJsonConverter;

import static org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.Config.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.Config.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.Config.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.Config.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.binlog2kafka.config.Config.TRANSACTION_PREFIX;

@Slf4j
public class Binlog2KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SeaTunnelRowType seaTunnelRowType;
    public final AtomicLong rowCounter = new AtomicLong(0);
    public Context context;
    private final DebeziumJsonConverter debeziumJsonConverter;
    private final BinlogJsonConverter jsonConverter;
    private ReadonlyConfig pluginConfig;

//    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    private String transactionPrefix;
    private long lastCheckpointId = 0;

    private static final int PREFIX_RANGE = 10000;

    public Binlog2KafkaSinkWriter(SeaTunnelRowType seaTunnelRowType, Context context,
                                  ReadonlyConfig pluginConfig,
                                  List<KafkaSinkState> kafkaStates) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        this.pluginConfig = pluginConfig;
        debeziumJsonConverter = new DebeziumJsonConverter(true, true);
        jsonConverter = new BinlogJsonConverter();
        if (pluginConfig.get(TRANSACTION_PREFIX) != null) {
            this.transactionPrefix = pluginConfig.get(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }
        restoreState(kafkaStates);
        this.seaTunnelRowSerializer = getSerializer(pluginConfig, seaTunnelRowType);
        log.info("output rowType: {}", fieldsInfo(seaTunnelRowType));
    }

    @Override
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public void write(SeaTunnelRow element) {
        //        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        //        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer = getSerializer(pluginConfig, seaTunnelRowType);
        Object[] fields = element.getFields();
        try {
            String binlogMessage = jsonConverter.convert(fields[1], fields[2]);
            System.out.println(binlogMessage);
        } catch (JsonProcessingException e) {
            log.error("", e);
        }
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() {
        // nothing
    }

    private String fieldsInfo(SeaTunnelRowType seaTunnelRowType) {
        String[] fieldsInfo = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                String.format(
                    "%s<%s>",
                    seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i)
                );
        }
        return StringUtils.join(fieldsInfo, ", ");
    }

    private SeaTunnelRowSerializer<byte[], byte[]> getSerializer(
        ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        MessageFormat messageFormat = pluginConfig.get(FORMAT);
        String delimiter = DEFAULT_FIELD_DELIMITER;

        if (pluginConfig.get(FIELD_DELIMITER) != null) {
            delimiter = pluginConfig.get(FIELD_DELIMITER);
        }

        String topic = pluginConfig.get(TOPIC);
        // By default, all partitions are sent randomly
        return DefaultSeaTunnelRowSerializer.create(
            topic, Arrays.asList(), seaTunnelRowType, messageFormat, delimiter);
    }

    private void restoreState(List<KafkaSinkState> states) {
        if (!states.isEmpty()) {
            this.transactionPrefix = states.get(0).getTransactionIdPrefix();
            this.lastCheckpointId = states.get(0).getCheckpointId();
        }
    }
}
