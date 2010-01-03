/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionQueue<T> {

    public static class Element<R> {

        private R value = null;
        private boolean completionToken = false;

        private Element(final R value, boolean completionToken) {
            this.value = value;
            this.completionToken = completionToken;
        }

        public static <S> Element<S> getElement(final Element<S> element, final S value) {
            element.value = value;
            element.completionToken = false;
            return element;
        }

        public static <S> Element<S> newEmptyToken() {
            return new Element<S>(null, false);
        }

        public static <S> Element<S> newCompletionToken() {
            return new Element<S>(null, true);
        }

        public R getValue() {
            return this.value;
        }

        public boolean isCompletionToken() {
            return this.completionToken;
        }
    }

    private final Element<T> completionToken = Element.newCompletionToken();
    private final AtomicInteger completionCounter = new AtomicInteger(0);

    private final BlockingQueue<Element<T>> elementQueue;
    private final BlockingQueue<Element<T>> reusablesQueue;

    public CompletionQueue(final int size) throws HBqlException {

        this.elementQueue = new ArrayBlockingQueue<Element<T>>(size, true);

        // Reusable queue avoids creating objects for every item put in queue.
        this.reusablesQueue = new ArrayBlockingQueue<Element<T>>(size);

        try {
            for (int i = 0; i < size + 1; i++) {
                final Element<T> emptyItem = Element.newEmptyToken();
                this.getReusablesQueue().put(emptyItem);
            }
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
    }

    private BlockingQueue<Element<T>> getElementQueue() {
        return this.elementQueue;
    }

    private BlockingQueue<Element<T>> getReusablesQueue() {
        return this.reusablesQueue;
    }

    private AtomicInteger getCompletionCounter() {
        return this.completionCounter;
    }

    private Element<T> getCompletionToken() {
        return this.completionToken;
    }

    public int getCompletionCount() {
        return this.getCompletionCounter().get();
    }

    public void putCompletionToken() {
        try {
            this.getElementQueue().put(this.getCompletionToken());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void putElement(final T val) throws HBqlException {
        try {
            final Element<T> element = Element.getElement(this.getReusablesQueue().take(), val);
            this.getElementQueue().put(element);
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
    }

    public Element<T> takeElement() throws HBqlException {
        try {
            final Element<T> element = this.getElementQueue().take();

            // Completion tokens do not go back to reusable queue
            if (element.isCompletionToken())
                this.getCompletionCounter().incrementAndGet();
            else
                this.getReusablesQueue().put(element);

            return element;
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
    }

    public void reset() {
        this.getCompletionCounter().set(0);
        this.getElementQueue().clear();
    }
}
