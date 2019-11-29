package com.zleying.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zleying.common.PropertiesUtil;
import com.zleying.common.RedisUtils;
import com.zleying.model.TopicPartitionOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;
import java.io.IOException;
import java.util.*;
public class Sparkstreamingkafka {
    public static Logger logger = Logger.getLogger(Sparkstreamingkafka.class);
    /**
     * 打印配置文件
     * @param properties
     */
    public static void printConfig(Properties properties) {
        Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            logger.info(entry.getKey().toString()+"="+entry.getValue().toString());
        }
    }
    public static Map<TopicPartition,Long> fromOffsets() throws IOException {
        Jedis jedis = RedisUtils.getJedis();
        Map<TopicPartition,Long> fromOffsets=new HashMap<TopicPartition,Long>();
        String offsets = jedis.get("sparkstreaming-kafka-test-offset");
        logger.info("get offsets from reids :"+offsets);
        if (offsets!=null){
            ObjectMapper mapper = new ObjectMapper();
            List<TopicPartitionOffset> topicPartitonOffsetsList  = mapper.readValue(offsets, new TypeReference<List<TopicPartitionOffset>>() { });
            for (TopicPartitionOffset topicPartitonOffset : topicPartitonOffsetsList) {
                String topic=topicPartitonOffset.getTopic();
                int partition = topicPartitonOffset.getPartition();
                long untilOffset = topicPartitonOffset.getUntilOffset();
                fromOffsets.put(new TopicPartition(topic,partition),untilOffset);
            }
        }
        return fromOffsets;
    }

    /**
     * 将offset写入redis
     **/
    public static void saveOffsets(OffsetRange[] offsetRanges) {
        Jedis jedis = RedisUtils.getJedis();
        ObjectMapper mapper = new ObjectMapper();
        List<TopicPartitionOffset> topicPartitonOffsets = new ArrayList<TopicPartitionOffset>();
        for (OffsetRange o : offsetRanges) {
            TopicPartitionOffset topicPartitonOffset = new TopicPartitionOffset();
            logger.info("topic:"+o.topic()+",partition:"+o.partition()+",fromOffset:"+o.fromOffset()+",untilOffset:"+o.untilOffset());
            topicPartitonOffset.setTopic(o.topic());
            topicPartitonOffset.setPartition(o.partition());
            topicPartitonOffset.setFromOffset(o.fromOffset());
            topicPartitonOffset.setUntilOffset(o.untilOffset());
            topicPartitonOffsets.add(topicPartitonOffset);
        }
        try {
            jedis.set("sparkstreaming-kafka-test-offset",mapper.writeValueAsString(topicPartitonOffsets));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            logger.debug("Usage: please input configFile args!");
            System.exit(1);
        }
        //获取配置文件路径
        String configFile = args[0];
        Properties properties = PropertiesUtil.getProperty(configFile);
        printConfig(properties);
        Set<String> topics = new HashSet(Arrays.asList(properties.getProperty("kafka.topics").split(",")));
        String groupId = properties.getProperty("kafka.groupId");
        //获取批次的时间间隔，比如5s
        Long streamingInterval = Long.parseLong(properties.getProperty("streaming.interval"));
        //获取kafka broker列表
        final String metadataBrokerList = properties.getProperty("bootstrap.servers");
        //组合kafka参数
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", metadataBrokerList);
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("group.id",groupId);
        SparkConf sparkConf = new SparkConf().setAppName("sparkstreaming-kafka-test");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100");
        sparkConf.setMaster("local[2]");
        Map<TopicPartition, Long> fromOffsets = fromOffsets();
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =null;
        if (fromOffsets.size()==0){
            kafkaStream=KafkaUtils.createDirectStream(javaStreamingContext,LocationStrategies.PreferConsistent(),ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, fromOffsets));
        }else {
            kafkaStream=KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Assign(fromOffsets().keySet(), kafkaParams, fromOffsets));
        }
        JavaDStream<ConsumerRecord<String, String>> transform = kafkaStream.transform(new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
            public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                for (OffsetRange offsetRange : offsetRanges) {
                    logger.info(
                              "topic=" + offsetRange.topic()
                            + ",partition=" + offsetRange.partition()
                            + ",fromOffset=" + offsetRange.fromOffset()
                            + ",untilOffset=" + offsetRange.untilOffset()
                            + ",rddpartitions=" + rdd.getNumPartitions()
                            + ",isempty=" + rdd.isEmpty()
                            + ",id=" + rdd.id());
                }
                return rdd;
            }
        });
        transform.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                       public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                           while(consumerRecordIterator.hasNext()){
                               ConsumerRecord<String, String> message = consumerRecordIterator.next();
                               logger.info("topic:"+message.topic()+",partition:"+message.partition()+",offset:"+message.offset()+",value:"+message.value());
                           }
                       }
                   });
                logger.info("start save offsets to redis:");
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                saveOffsets(offsetRanges);
            }
        });
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
