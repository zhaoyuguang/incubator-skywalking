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

package org.apache.skywalking.apm.plugin.spring.cloud.gateways.v2;

import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.web.server.ServerWebExchange;

import java.lang.reflect.Method;

/**
 * @author zhaoyuguang
 */
public class RoutePredicateHandlerMappingInterceptor implements InstanceMethodsAroundInterceptor {

    private Logger logger = LoggerFactory.getLogger(RoutePredicateHandlerMappingInterceptor.class);

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
                             MethodInterceptResult result) throws Throwable {
        logger.info("RoutePredicateHandlerMappingInterceptor::getHandlerInternal" + Thread.currentThread().getId());
        ServerWebExchange exchange = (ServerWebExchange) allArguments[0];
        HttpHeaders headers = exchange.getRequest().getHeaders();
        ContextCarrier contextCarrier = new ContextCarrier();

        CarrierItem next = contextCarrier.items();
        while (next.hasNext()) {
            next = next.next();
            if (!CollectionUtils.isEmpty(headers.get(next.getHeadKey()))) {
                next.setHeadValue(headers.get(next.getHeadKey()).get(0));
            }
        }

        AbstractSpan span = ContextManager.createEntrySpan(exchange.getRequest().getPath().value(), contextCarrier);
        exchange.getRequest();
        Tags.URL.set(span, exchange.getRequest().getURI().toString());
        Tags.HTTP.METHOD.set(span, exchange.getRequest().getPath().value());
        span.setComponent(ComponentsDefine.SPRING_CLOUD_GATEWAYS);
        SpanLayer.asHttp(span);
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                              Class<?>[] argumentsTypes, Object ret) throws Throwable {
//        AbstractSpan span = ContextManager.activeSpan();
//        if (response.getStatus() >= 400) {
//            span.errorOccurred();
//            Tags.STATUS_CODE.set(span, Integer.toString(response.getStatus()));
//        }
        ContextManager.stopSpan();
        return ret;
    }


    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                      Class<?>[] argumentsTypes, Throwable t) {
        AbstractSpan span = ContextManager.activeSpan();
        span.errorOccurred();
        span.log(t);
    }
}
