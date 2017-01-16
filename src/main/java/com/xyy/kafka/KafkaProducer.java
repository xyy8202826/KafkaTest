package com.xyy.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2016/5/8.
 */
public class KafkaProducer {
    private final Producer<String, String> producer;
    public final static String TOPIC = "TEST-TOPIC";

    private KafkaProducer(){
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "127.0.0.1:9092");

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() {
        int n=10;
        final CountDownLatch countDownLatch=new CountDownLatch(n);
        Long start =System.currentTimeMillis();

        final int COUNT = 1000;

        for( int i=0;i<n;i++){
            new Thread(new Runnable() {
                public void run() {
                     int messageNo = 0;
                    while (messageNo < COUNT) {
                        String key = String.valueOf(Thread.currentThread().getName()+":"+messageNo);
                        String data = "hello kafka message:" + key;
                        producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));
                        System.out.println(data);
                        messageNo ++;
                  /*      Random random = new Random();
                        long i = random.nextInt(3);
                        try {
                            TimeUnit.SECONDS.sleep(i);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            countDownLatch.await();
            Long end =System.currentTimeMillis();
            System.out.println(end-start);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args )
    {
        new KafkaProducer().produce();
    }
}
