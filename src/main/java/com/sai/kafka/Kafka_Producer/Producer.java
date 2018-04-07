package com.sai.kafka.Kafka_Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Producer {

	private final static String TOPIC ="test-topic";
	private final static String BROKERS ="hdi1rvmk01-inc.tw-test.net:6667,hdi1rvmk02-inc.tw-test.net:6667,hdi1rvmk03-inc.tw-test.net:6667";
	
	private static KafkaProducer<Long,String> createProducer ()
	{
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BROKERS);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
		return new KafkaProducer<>(properties);
	}
	
	private static void sendSynchronousMessages(final int sendMessageCount) throws Exception
	{
		final KafkaProducer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();
		try {
			for (long index=time; index < time + sendMessageCount;index++)
			{
				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,index,"omsairam" + index);
				RecordMetadata metadata = producer.send(record).get();
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
                record.key(), record.value(), metadata.partition(),
                metadata.offset());
			}
		}
		
		finally {
	          producer.flush();
	          producer.close();
	      }
	}
	private static void sendAsynchronousMessages(final int sendMessageCount) throws Exception
	{
		final KafkaProducer<Long, String> producer = createProducer();
	    long time = System.currentTimeMillis();
	    final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

	    try {
	        for (long index = time; index < time + sendMessageCount; index++) {
	            final ProducerRecord<Long, String> record =
	                    new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);
	            producer.send(record, (metadata, exception) -> {
	                long elapsedTime = System.currentTimeMillis() - time;
	                if (metadata != null) {
	                    System.out.printf("sent record(key=%s value=%s) " +
	                                    "meta(partition=%d, offset=%d) time=%d\n",
	                            record.key(), record.value(), metadata.partition(),
	                            metadata.offset(), elapsedTime);
	                } else {
	                    exception.printStackTrace();
	                }
	                countDownLatch.countDown();
	            });
	        }
	        countDownLatch.await(25, TimeUnit.SECONDS);
	    }
	    finally {
	        producer.flush();
	        producer.close();
	    }
	}
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		//sendSynchronousMessages(10);
		sendAsynchronousMessages(10);

	}

}
