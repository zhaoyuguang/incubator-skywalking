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

package test.org.apache.skywalking.apm.testcase.spring.kafka.controller;

import com.alibaba.fastjson.JSONObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@Controller
@RequestMapping("/case")
@PropertySource("classpath:application.properties")
public class CaseController {

    private static final String SUCCESS = "Success";

    @Value("${consumer.servers}")
    private String consumer;
    @Value("${consumer.topic}")
    private String consumerTopicName;
    @Value("${provider.servers}")
    private String provider;
    @Value("${provider.topic}")
    private String providerTopicName;
    private KafkaTemplate<String, String> producerKafkaTemplate;
    private KafkaTemplate<String, String> producerKafkaTemplate2;

    private CountDownLatch latch = new CountDownLatch(1);


    private boolean run = false;

    @PostConstruct
    private void setUp() {
        setUpProvider();
        setUpProvider2();

//        setUpConsumer();
//        setUpConsumer2();
    }

    private void setUpProvider() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka04-test.lianjia.com:9092,kafka05-test.lianjia.com:9092,kafka06-test.lianjia.com:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerKafkaTemplate = new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory<>(props));
    }

    private void setUpProvider2() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01-test.lianjia.com:9092,kafka02-test.lianjia.com:9092,kafka03-test.lianjia.com:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerKafkaTemplate2 = new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory<>(props));
    }

    private void setUpConsumer() {
        if(run){
           return;
        }
        run = true;
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01-test.lianjia.com:9092,kafka02-test.lianjia.com:9092,kafka03-test.lianjia.com:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "grop1:" + consumerTopicName);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Deserializer<String> stringDeserializer = new StringDeserializer();
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory(configs, stringDeserializer, stringDeserializer);
        ContainerProperties props = new ContainerProperties("ketrace-test-mysql-cost");
        props.setMessageListener(new BatchAcknowledgingMessageListener<String, String>() {
            @Override
            public void onMessage(List<ConsumerRecord<String, String>> list, Acknowledgment var2) {
                Set<String> ss_id_ = new HashSet<>();
                for (ConsumerRecord<String, String> record : list) {
                    System.out.println(TraceContext.traceId()+"  " +Thread.currentThread().getId()+"record.value()" + record.value());
                    ss_id_.add(record.value());
                    producerKafkaTemplate2.send("apm_test_0", record.value(), record.value());
                }
                ActiveSpan.tag("ss_id_0", String.join(",", ss_id_));
                var2.acknowledge();
            }
        });
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(factory, props);
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        container.start();
    }


    private void setUpConsumer2() {
        if(run){
            return;
        }
        run = true;
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01-test.lianjia.com:9092,kafka02-test.lianjia.com:9092,kafka03-test.lianjia.com:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "grop12:" + consumerTopicName);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Deserializer<String> stringDeserializer = new StringDeserializer();
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory(configs, stringDeserializer, stringDeserializer);
        ContainerProperties props = new ContainerProperties("apm_test_0");
        props.setMessageListener(new BatchAcknowledgingMessageListener<String, String>() {
            @Override
            public void onMessage(List<ConsumerRecord<String, String>> list, Acknowledgment var2) {
                Set<String> ss_id_ = new HashSet<>();
                for (ConsumerRecord<String, String> record : list) {
                    System.out.println(TraceContext.traceId()+"  " +Thread.currentThread().getId()+"record.value()" + record.value());
                    ss_id_.add(record.value());

                }
                ActiveSpan.tag("ss_id_0", String.join(",", ss_id_));
                ActiveSpan.tag("test_id", String.join(",", ss_id_));
                OkHttpClient client = new OkHttpClient.Builder().build();
                Request request = new Request.Builder().url("https://shardingsphere.apache.org/document/current/en/overview").build();
                Response response = null;
                try {
                    response = client.newCall(request).execute();
                } catch (IOException e) {
                }
                response.body().close();
                var2.acknowledge();
            }
        });
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(factory, props);
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        container.start();
    }

    @RequestMapping("/spring-kafka-case")
    @ResponseBody
    public String springKafkaCase(@RequestParam("args1") int args1) throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    ProducerRecord<String, String> record = new ProducerRecord<>("signing-owl-order-0", "key", "id_" + args1);
                    ProducerRecord<String, String> record2 = new ProducerRecord<>("signing-owl-order-0", "key", "id_" + (args1+1));

                    record.headers().add("a","a".getBytes());
                    record2.headers().add("a","a".getBytes());
                    producerKafkaTemplate.send(record);
                    producerKafkaTemplate.send(record2);
                    producerKafkaTemplate.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.start();
        return SUCCESS;
    }

    @RequestMapping("/spring-kafka-consumer-ping")
    @ResponseBody
    public String springKafkaConsumerPing() {
        return SUCCESS;
    }

    @RequestMapping("/healthCheck")
    @ResponseBody
    public String healthCheck() {
        return SUCCESS;
    }
}

