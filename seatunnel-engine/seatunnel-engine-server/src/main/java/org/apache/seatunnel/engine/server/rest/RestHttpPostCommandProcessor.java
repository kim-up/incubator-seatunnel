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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.log.Log4j2HttpPostCommandProcessor;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.spi.impl.NodeEngine;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOB_URL;

public class RestHttpPostCommandProcessor extends HttpCommandProcessor<HttpPostCommand> {

    private final Log4j2HttpPostCommandProcessor original;

    private static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";

    private static final String SINK_WRITE_COUNT = "SinkWriteCount";

    private NodeEngine nodeEngine;

    public RestHttpPostCommandProcessor(TextCommandService textCommandService) {
        this(textCommandService, new Log4j2HttpPostCommandProcessor(textCommandService));
    }

    public RestHttpPostCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpPostCommandProcessor log4j2HttpPostCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = log4j2HttpPostCommandProcessor;
    }

    @Override
    public void handle(HttpPostCommand request) {
        String uri = request.getURI();
        try {
            if (uri.startsWith(SUBMIT_JOB_URL)) {
                handleSubmitJobsInfo(request);
            } else {
                original.handle(request);
            }
        } catch (IndexOutOfBoundsException e) {
            request.send400();
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + request, e);
            prepareResponse(SC_500, request, exceptionResponse(e));
        }

        this.textCommandService.sendResponse(request);
    }

    @Override
    public void handleRejection(HttpPostCommand request) {
        handle(request);
    }

    private void handleSubmitJobsInfo(final HttpPostCommand request) {
        System.out.println("");
    }
}
