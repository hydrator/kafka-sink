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

package co.cask.hydrator.plugin.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.plugin.common.KafkaHelpers;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import kafka.api.OffsetRequest;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka Streaming source
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Kafka")
@Description("Kafka streaming source.")
public class KafkaStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSource.class);
  private final KafkaConfig conf;

  public KafkaStreamingSource(KafkaConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    conf.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.getSchema());
    if (conf.getMaxRatePerPartition() != null && conf.getMaxRatePerPartition() > 0) {
      Map<String, String> pipelineProperties = new HashMap<>();
      pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition", conf.getMaxRatePerPartition().toString());
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(conf.referenceName);

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBrokers());
    // Spark saves the offsets in checkpoints, no need for Kafka to save them
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    KafkaHelpers.setupOldKerberosLogin(conf.getPrincipal(), conf.getKeytabLocation());
    // Create a unique string for the group.id using the pipeline name and the topic.
    // group.id is a Kafka consumer property that uniquely identifies the group of
    // consumer processes to which this consumer belongs.
    kafkaParams.put("group.id", Joiner.on("-").join(context.getPipelineName().length(), conf.getTopic().length(),
                                                    context.getPipelineName(), conf.getTopic()));
    kafkaParams.putAll(conf.getKafkaProperties());

    Properties properties = new Properties();
    properties.putAll(kafkaParams);
    try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
                                                                 new ByteArrayDeserializer())) {
      Map<TopicPartition, Long> offsets = conf.getInitialPartitionOffsets(getPartitions(consumer));
      // KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
      // so we have to replace them with the actual smallest and latest
      List<TopicPartition> earliestOffsetRequest = new ArrayList<>();
      List<TopicPartition> latestOffsetRequest = new ArrayList<>();
      for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
        TopicPartition topicAndPartition = entry.getKey();
        Long offset = entry.getValue();
        if (offset == OffsetRequest.EarliestTime()) {
          earliestOffsetRequest.add(topicAndPartition);
        } else if (offset == OffsetRequest.LatestTime()) {
          latestOffsetRequest.add(topicAndPartition);
        }
      }

      Set<TopicPartition> allOffsetRequest =
        Sets.newHashSet(Iterables.concat(earliestOffsetRequest, latestOffsetRequest));
      Map<TopicPartition, Long> offsetsFound = new HashMap<>();
      offsetsFound.putAll(KafkaHelpers.getEarliestOffsets(consumer, earliestOffsetRequest));
      offsetsFound.putAll(KafkaHelpers.getLatestOffsets(consumer, latestOffsetRequest));
      for (TopicPartition topicAndPartition : allOffsetRequest) {
        offsets.put(topicAndPartition, offsetsFound.get(topicAndPartition));
      }

      Set<TopicPartition> missingOffsets = Sets.difference(allOffsetRequest, offsetsFound.keySet());
      if (!missingOffsets.isEmpty()) {
        throw new IllegalStateException(String.format(
          "Could not find offsets for %s. Please check all brokers were included in the broker list.", missingOffsets));
      }
      LOG.info("Using initial offsets {}", offsets);

      Map<TopicAndPartition, Long> oldOffsets = convertOffsetsToOldFormat(offsets);
      Map<String, String> oldKafkaParams = convertParamsToOldFormat(kafkaParams);
      return KafkaUtils.createDirectStream(
        context.getSparkStreamingContext(), byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
        MessageAndMetadata.class, oldKafkaParams, oldOffsets,
        new Function<MessageAndMetadata<byte[], byte[]>, MessageAndMetadata>() {
          @Override
          public MessageAndMetadata call(MessageAndMetadata<byte[], byte[]> in) throws Exception {
            return in;
          }
        }).transform(new RecordTransform(conf));
    }
  }

  private Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer) {
    Set<Integer> partitions = conf.getPartitions();
    if (!partitions.isEmpty()) {
      return partitions;
    }

    partitions = new HashSet<>();
    for (PartitionInfo partitionInfo : consumer.partitionsFor(conf.getTopic())) {
      partitions.add(partitionInfo.partition());
    }
    return partitions;
  }

  private Map<TopicAndPartition, Long> convertOffsetsToOldFormat(Map<TopicPartition, Long> offsets) {
    Map<TopicAndPartition, Long> oldFormat = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      oldFormat.put(new TopicAndPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue());
    }
    return oldFormat;
  }

  private Map<String, String> convertParamsToOldFormat(Map<String, Object> kafkaParmas) {
    Map<String, String> oldFormat = new HashMap<>();
    for (Map.Entry<String, Object> entry : kafkaParmas.entrySet()) {
      oldFormat.put(entry.getKey(), entry.getValue().toString());
    }
    return oldFormat;
  }

  /**
   * Applies the format function to each rdd.
   */
  private static class RecordTransform
    implements Function2<JavaRDD<MessageAndMetadata>, Time, JavaRDD<StructuredRecord>> {

    private final KafkaConfig conf;

    RecordTransform(KafkaConfig conf) {
      this.conf = conf;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<MessageAndMetadata> input, Time batchTime) throws Exception {
      Function<MessageAndMetadata, StructuredRecord> recordFunction = conf.getFormat() == null ?
        new BytesFunction(batchTime.milliseconds(), conf) : new FormatFunction(batchTime.milliseconds(), conf);
      return input.map(recordFunction);
    }
  }

  /**
   * Common logic for transforming kafka key, message, partition, and offset into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private abstract static class BaseFunction implements Function<MessageAndMetadata, StructuredRecord> {
    private final long ts;
    protected final KafkaConfig conf;
    private transient String messageField;
    private transient String timeField;
    private transient String keyField;
    private transient String partitionField;
    private transient String offsetField;
    private transient Schema schema;

    BaseFunction(long ts, KafkaConfig conf) {
      this.ts = ts;
      this.conf = conf;
    }

    @Override
    public StructuredRecord call(MessageAndMetadata in) throws Exception {
      // first time this was called, initialize schema and time, key, and message fields.
      if (schema == null) {
        schema = conf.getSchema();
        timeField = conf.getTimeField();
        keyField = conf.getKeyField();
        partitionField = conf.getPartitionField();
        offsetField = conf.getOffsetField();
        for (Schema.Field field : schema.getFields()) {
          String name = field.getName();
          if (!name.equals(timeField) && !name.equals(keyField)) {
            messageField = name;
            break;
          }
        }
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      if (timeField != null) {
        builder.set(timeField, ts);
      }
      if (keyField != null) {
        builder.set(keyField, in.key());
      }
      if (partitionField != null) {
        builder.set(partitionField, in.partition());
      }
      if (offsetField != null) {
        builder.set(offsetField, in.offset());
      }
      addMessage(builder, messageField, (byte[]) in.message());
      return builder.build();
    }

    protected abstract void addMessage(StructuredRecord.Builder builder, String messageField,
                                       byte[] message) throws Exception;
  }

  /**
   * Transforms kafka key and message into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class BytesFunction extends BaseFunction {

    BytesFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) {
      builder.set(messageField, message);
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction extends BaseFunction {
    private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

    FormatFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) throws Exception {
      // first time this was called, initialize record format
      if (recordFormat == null) {
        Schema messageSchema = conf.getMessageSchema();
        FormatSpecification spec =
          new FormatSpecification(conf.getFormat(), messageSchema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(message)));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
  }

}
