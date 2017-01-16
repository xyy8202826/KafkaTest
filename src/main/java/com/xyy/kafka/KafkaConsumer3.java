package com.xyy.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by Administrator on 2016/5/8.
 */
public class KafkaConsumer3 {
    private final ConsumerConnector consumer;

    private KafkaConsumer3() {
        Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", "127.0.0.1:2181");

        //group 代表一个消费组
        props.put("group.id", "jd-group");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    void consume(int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        TopicFilter topicFilter = new Whitelist(KafkaProducer.TOPIC) ;
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        List<KafkaStream<String, String>> streams = consumer.createMessageStreamsByFilter(topicFilter,threads,keyDecoder,valueDecoder);
        System.out.println("streams.size:"+streams.size());
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerMsgTask(stream, threadNumber));
            threadNumber++;
        }
    }

    public static void main(String[] args) {
        new KafkaConsumer3().consume(3);
    }
}
