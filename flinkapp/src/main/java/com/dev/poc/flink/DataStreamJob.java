/*
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

package com.dev.poc.flink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import com.mongodb.client.model.InsertOneModel;
import com.dev.poc.flink.dto.NumEvent;
import com.dev.poc.flink.serde.NumEventDeserializationSchema;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import java.util.Properties;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
    final ObjectMapper mapper = new ObjectMapper(); 
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<NumEvent> rawNumsStream;
		KafkaSource<NumEvent> source = KafkaSource.<NumEvent>builder()
          .setBootstrapServers("redpanda:9092")
          .setTopics("nums-raw")
          .setGroupId("my-group")
          .setStartingOffsets(OffsetsInitializer.earliest())
          .setValueOnlyDeserializer(new NumEventDeserializationSchema())
          .build();

    rawNumsStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "redpanda-input");
		
    MongoSink<NumEvent> sink = MongoSink.<NumEvent>builder()
        .setUri("mongodb://user:pass@mongodb:27017")
        .setDatabase("nums")
        .setCollection("nums_event_history")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema(
                (event, context) -> {
                  try{
                    return new InsertOneModel<>(BsonDocument.parse(mapper.writeValueAsString(event)));
                  }
                  catch(Exception e){
                    return null;
                  }
                })
                
        .build();

    rawNumsStream.sinkTo(sink).name("mongo-sink");
    env.execute("Flink Red Panda Mongo");
	}
}
