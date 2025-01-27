/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * Common methods for all tests
 */
public class KafkaTestCommon {

  public static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("zookeeper.session.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("offsets.topic.replication.factor", "1");
    return prop;
  }

  public static KafkaConsumer<String, String> getConsumer(int kafkaPort) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    return new KafkaConsumer<>(props);
  }

  public static KafkaProducer<String, String> getProducer(int kafkaPort) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<>(props);
  }

  public static void sendKafkaMessage(KafkaProducer kafkaProducer,
                                      String topic,
                                      Map<String, String> messages) {
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      kafkaProducer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()));
    }
  }
}
