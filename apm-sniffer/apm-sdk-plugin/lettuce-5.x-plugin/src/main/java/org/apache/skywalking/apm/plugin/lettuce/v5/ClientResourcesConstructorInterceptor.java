/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package org.apache.skywalking.apm.plugin.lettuce.v5;

import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceConstructorInterceptor;

/**
 * ClientResources is the link between RedisChannelWriter and AbstractRedisClient.
 * to enhance ClientResources for bring peer(the cluster configuration information)
 * in AbstractRedisClient to RedisChannelWriter.
 *
 * @author zhaoyuguang
 */
public class ClientResourcesConstructorInterceptor implements InstanceConstructorInterceptor {

    @Override
    public void onConstruct(EnhancedInstance objInst, Object[] allArguments) {
    }
}
