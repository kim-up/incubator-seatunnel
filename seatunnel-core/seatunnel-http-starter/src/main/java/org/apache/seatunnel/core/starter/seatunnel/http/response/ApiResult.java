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

package org.apache.seatunnel.core.starter.seatunnel.http.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ApiResult<T> implements Serializable {

    private static final long serialVersionUID = -1L;

    private static final Logger logger = LoggerFactory.getLogger(ApiResult.class);

    private int code = 0;
    private String message;
    private T data;

    private ApiResult() {
        this.data = null;
    }

    public ApiResult(final ErrorCode errorCode, Object... messages) {
        this.code = errorCode.getCode();
        if (messages != null && messages.length > 0) {
            this.message =
                    MessageFormatter.arrayFormat(errorCode.getMessage(), messages).getMessage();
        } else {
            this.message = errorCode.getMessage();
        }
    }

    public static <T> ApiResult<T> success(T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setData(data);
        return result;
    }

    public static <T> ApiResult<T> failure(String msg) {
        return failure(ErrorCode.BUSINESS_ERROR, msg);
    }

    public static <T> ApiResult<T> failure(ErrorCode errorCode, Object... messages) {
        return new ApiResult<T>(errorCode, messages);
    }

    public int getCode() {
        return code;
    }

    public void setCode(final int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(final String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(final T data) {
        this.data = data;
    }
}
