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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.Data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CheckpointSerializeTest2 {

    @Test
    @Disabled
    public void testPipelineStateDeserialize() throws IOException {
        File file = new File("/Users/zhujinming/Downloads/1691144849114-738-1-485.ser");
        FileInputStream fileInputStream = null;
        byte[] bFile = new byte[(int) file.length()];
        // convert file into array of bytes
        fileInputStream = new FileInputStream(file);
        fileInputStream.read(bFile);
        fileInputStream.close();
        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        PipelineState pipelineState = protoStuffSerializer.deserialize(bFile, PipelineState.class);
        CompletedCheckpoint latestCompletedCheckpoint =
                protoStuffSerializer.deserialize(
                        pipelineState.getStates(), CompletedCheckpoint.class);
        Map<ActionStateKey, ActionState> taskStates = latestCompletedCheckpoint.getTaskStates();
        ActionStateKey actionStateKey =
                new ActionStateKey("ActionStateKey - pipeline-1 [Source[0]-MySQL-CDC-binlog]");
        ActionState actionState = taskStates.get(actionStateKey);

        List<ActionSubtaskState> subtaskStates = actionState.getSubtaskStates();
        List<byte[]> coordinatorBytes = actionState.getCoordinatorState().getState();
        DefaultSerializer<HybridPendingSplitsState> fakeSourceSerializer =
                new DefaultSerializer<HybridPendingSplitsState>();
        HybridPendingSplitsState hybridPendingSplitsState =
                fakeSourceSerializer.deserialize(coordinatorBytes.get(0));

        for (ActionSubtaskState state : subtaskStates) {
            List<byte[]> bList = state.getState();
            for (int i = 0; i < bList.size(); i++) {
                byte[] bytes = bList.get(i);
                DefaultSerializer<SourceSplitBase> defaultSerializer =
                        new DefaultSerializer<SourceSplitBase>();
                SourceSplitBase split = defaultSerializer.deserialize(bytes);
                System.out.println(split.splitId());
            }
        }

        actionState = taskStates.get(2L);
        List<byte[]> sinkCommitStateSeri = actionState.getCoordinatorState().getState();
        DefaultSerializer<FileAggregatedCommitInfo> fileSinkStateDefaultSerializer =
                new DefaultSerializer<FileAggregatedCommitInfo>();
        FileAggregatedCommitInfo fileAggregatedCommitInfo =
                fileSinkStateDefaultSerializer.deserialize(sinkCommitStateSeri.get(0));
        subtaskStates = actionState.getSubtaskStates();
        for (ActionSubtaskState state : subtaskStates) {
            List<byte[]> bList = state.getState();
            for (int i = 0; i < bList.size(); i++) {
                byte[] bytes = bList.get(i);
                DefaultSerializer<FileSinkState> defaultSerializer =
                        new DefaultSerializer<FileSinkState>();
                FileSinkState fileSinkState = defaultSerializer.deserialize(bytes);
                System.out.println(fileSinkState.getTransactionDir());
            }
        }
    }

    @Data
    public class SourceState implements Serializable {
        private final Set<SourceSplitBase> assignedSplits;
    }
}
