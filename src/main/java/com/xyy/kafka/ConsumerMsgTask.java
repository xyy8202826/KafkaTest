package com.xyy.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

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
            System.out.println("Thread " + m_threadNumber + ": "
                    + new String(it.next().message()));
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
