/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.mapping;


import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.Arrays;
import java.util.List;

public class EnvVars {

    private static List<String> packagePath = Lists.newArrayList();

    public static void setPackagePath(final String str) {
        packagePath.clear();
        packagePath.add("");      // Add an entry for the object as defined in statement
        packagePath.addAll(Arrays.asList(str.split(":")));
    }

    public static List<String> getPackagePath() {
        return packagePath;
    }
}
