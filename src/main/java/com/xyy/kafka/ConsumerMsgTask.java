package com.xyy.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * </p>
 * User: xuyuanye Date: 2016/9/2 Project: Kafka
 */
public class ConsumerMsgTask implements Runnable {

    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }
    public void run() {
        System.out.println("Thread " + m_threadNumber);
        ConsumerIterator<String, String> it = m_stream.iterator();
        while (it.hasNext()){
            MessageAndMetadata<String,String> messageAndMetadata = it.next();
            String message = messageAndMetadata.message();
            int partition = messageAndMetadata.partition();
            //String messageAndMetadata
            System.out.println("Thread " + m_threadNumber + ": partition:"+partition+":message"
                    + message +"--start");
            Random random = new Random();
            long i = random.nextInt(1);
            try {
                TimeUnit.SECONDS.sleep(i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Thread " + m_threadNumber + ": "
                    + message +"--end");
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
