/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.hbql.impl;

public class QueueElement<T> {
    private final T element;
    private final boolean completeToken;

    QueueElement(final T element, boolean completeToken) {
        this.element = element;
        this.completeToken = completeToken;
    }

    public static <T> QueueElement<T> newResult(final T result) {
        return new QueueElement<T>(result, false);
    }

    public static <T> QueueElement<T> newComplete() {
        return new QueueElement<T>(null, true);
    }

    public T getElement() {
        return this.element;
    }

    public boolean isCompleteToken() {
        return this.completeToken;
    }
}
