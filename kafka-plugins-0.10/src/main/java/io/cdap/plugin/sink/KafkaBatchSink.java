/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.batch.source.KafkaBatchConfig;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.KafkaHelpers;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kafka sink to write to Kafka
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(KafkaBatchSink.NAME)
@Description("KafkaSink to write events to kafka")
public class KafkaBatchSink extends ReferenceBatchSink<StructuredRecord, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBatchSink.class);
  public static final String NAME = "Kafka";
  private static final String ASYNC = "async";
  private static final String TOPIC = "topic";

  // Configuration for the plugin.
  private final Config producerConfig;

  private final KafkaOutputFormatProvider kafkaOutputFormatProvider;

  // Static constants for configuring Kafka producer.
  private static final String ACKS_REQUIRED = "acks";

  public KafkaBatchSink(Config producerConfig) {
    super(producerConfig);
    this.producerConfig = producerConfig;
    this.kafkaOutputFormatProvider = new KafkaOutputFormatProvider(producerConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    producerConfig.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().getFailureCollector().getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LineageRecorder lineageRecorder = new LineageRecorder(context, producerConfig.referenceName);
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null) {
      lineageRecorder.createExternalDataset(inputSchema);
      if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
        lineageRecorder.recordWrite("Write", "Wrote to Kafka topic.", inputSchema.getFields().stream().map
          (Schema.Field::getName).collect(Collectors.toList()));
      }
    }
    context.addOutput(Output.of(producerConfig.referenceName, kafkaOutputFormatProvider));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, Text>> emitter)
    throws Exception {
    List<Schema.Field> fields = input.getSchema().getFields();

      String body;
      if (producerConfig.format.equalsIgnoreCase("json")) {
        body = StructuredRecordStringConverter.toJsonString(input);
      } else {
        List<Object> objs = getExtractedValues(input, fields);
        body = StringUtils.join(objs, ",");
      }

      if (Strings.isNullOrEmpty(producerConfig.key)) {
        emitter.emit(new KeyValue<>((Text) null, new Text(body)));
      } else {
        String key = input.get(producerConfig.key);
        emitter.emit(new KeyValue<>(new Text(key), new Text(body)));
      }
  }

  private List<Object> getExtractedValues(StructuredRecord input, List<Schema.Field> fields) {
    // Extract all values from the structured record
    List<Object> objs = Lists.newArrayList();
    for (Schema.Field field : fields) {
      objs.add(input.get(field.getName()));
    }
    return objs;
  }


  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends ReferencePluginConfig {
    
    private static final String KEY = "key";
    private static final String TOPIC = KafkaBatchSink.TOPIC;
    private static final String FORMAT = "format";
    private static final String KAFKA_PROPERTIES = "kafkaProperties";
    private static final String COMPRESSION_TYPE = "compressionType";

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The connection to use.")
    private KafkaSinkConnectorConfig connection;

    @Name(ASYNC)
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    @Macro
    private String async;

    @Name(KEY)
    @Description("Specify the key field to be used in the message. Only String Partitioner is supported.")
    @Macro
    @Nullable
    private String key;

    @Name(TOPIC)
    @Description("Topic to which message needs to be published")
    @Macro
    private String topic;

    @Name(FORMAT)
    @Description("Format a structured record should be converted to")
    @Macro
    private String format;

    @Name(KAFKA_PROPERTIES)
    @Description("Additional kafka producer properties to set")
    @Macro
    @Nullable
    private String kafkaProperties;

    @Description("The kerberos principal used for the source.")
    @Macro
    @Nullable
    private String principal;

    @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String keytabLocation;

    @Name(COMPRESSION_TYPE)
    @Description("Compression type to be applied on message")
    @Macro
    private String compressionType;
    
    public String getBrokers() {
      return connection == null ? null : connection.getBrokers();
    }

    public Config(String brokers, String async, String key, String topic, String format, String kafkaProperties,
                  String compressionType) {
      super(String.format("Kafka_%s", topic));
      this.connection = new KafkaSinkConnectorConfig(brokers);
      this.async = async;
      this.key = key;
      this.topic = topic;
      this.format = format;
      this.kafkaProperties = kafkaProperties;
      this.compressionType = compressionType;
    }

    private void validate(FailureCollector collector) {
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      if (!async.equalsIgnoreCase("true") && !async.equalsIgnoreCase("false")) {
        collector.addFailure("Async flag has to be either TRUE or FALSE.", null)
                .withConfigProperty(ASYNC);
      }
      KafkaHelpers.validateKerberosSetting(principal, keytabLocation, collector);
      if (connection != null && connection.getBrokers() != null) {
        KafkaBatchConfig.parseBrokerMap(connection.getBrokers(), collector);
      }
    }
  }

  private static class KafkaOutputFormatProvider implements OutputFormatProvider {
    public static final String HAS_KEY = "hasKey";
    private final Map<String, String> conf;

    KafkaOutputFormatProvider(Config kafkaSinkConfig) {
      this.conf = new HashMap<>();
      conf.put(TOPIC, kafkaSinkConfig.topic);

      conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBrokers());
      conf.put("compression.type", kafkaSinkConfig.compressionType);
      conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

      KafkaHelpers.setupKerberosLogin(conf, kafkaSinkConfig.principal, kafkaSinkConfig.keytabLocation);
      addKafkaProperties(kafkaSinkConfig.kafkaProperties);

      conf.put(ASYNC, kafkaSinkConfig.async);
      if (kafkaSinkConfig.async.equalsIgnoreCase("true")) {
        conf.put(ACKS_REQUIRED, "1");
      }

      if (!Strings.isNullOrEmpty(kafkaSinkConfig.key)) {
        conf.put(HAS_KEY, kafkaSinkConfig.key);
      }
    }

    private void addKafkaProperties(String kafkaProperties) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      if (!Strings.isNullOrEmpty(kafkaProperties)) {
        for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
          // add prefix to each property
          String key = "additional." + keyVal.getKey();
          String val = keyVal.getValue();
          conf.put(key, val);
        }
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return KafkaOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
