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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.state.KafkaAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.binlog2kafka.sink.state.KafkaSinkState;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@NoArgsConstructor
@AutoService(SeaTunnelSink.class)
public class Binlog2KafkaSink
        implements SeaTunnelSink<
                SeaTunnelRow, KafkaSinkState, KafkaCommitInfo, KafkaAggregatedCommitInfo> {

    private SeaTunnelRowType seaTunnelRowType;
    private ReadonlyConfig pluginConfig;

    public Binlog2KafkaSink(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> createWriter(
            final SinkWriter.Context context) throws IOException {
        return new Binlog2KafkaSinkWriter(
                seaTunnelRowType, context, pluginConfig, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> restoreWriter(
            final SinkWriter.Context context, final List<KafkaSinkState> states)
            throws IOException {
        return new Binlog2KafkaSinkWriter(seaTunnelRowType, context, pluginConfig, states);
    }

    @Override
    public Optional<Serializer<KafkaSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<KafkaCommitInfo>> createCommitter() {
        return Optional.of(new KafkaSinkCommitter(pluginConfig));
    }

    @Override
    public Optional<Serializer<KafkaCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return "Binlog2Kafka";
    }

    @Override
    public void prepare(Config pluginConfig) {}
}
