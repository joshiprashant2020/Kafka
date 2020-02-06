package com.zekelabs.kafka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.zekelabs.kafka.constants.IKafkaConstants;
import com.zekelabs.kafka.consumer.ConsumerCreator;
import com.zekelabs.kafka.pojo.CustomObject;
import com.zekelabs.kafka.producer.ProducerCreator;

public class App {
	public static void main(String[] args) {
       // runProducer();
		//runConsumer();
		String csvFile = "/home/edyoda/TestData/OnlineRetail.csv";
        BufferedReader br = null;
       // String line = "";
        //String cvsSplitBy = ",";
        String inputData = "";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((inputData = br.readLine()) != null) {

                // use comma as separator
                
                System.out.println("record= " + inputData );
                runProducer(inputData);
            	}
            	//runProducer(inputData);
            	
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

	
		
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				//System.out.println("Record value " + record.value().getName());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	
	static void runProducer(String inputData) {
		Producer<Long,String> producer = ProducerCreator.createProducer();
		String cvsSplitBy = ",";
		//String id ="record";
		//for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			//CustomObject c = new CustomObject();
			//c.setId(id);
			//c.setName(inputData);
		String data []= inputData.split(cvsSplitBy);
		String topicName = "";
		System.out.println(data[7]);
		String country = data[7].trim();
		System.out.println("country:"+country);
		if(country.equals("United Kingdom"))
		{
			topicName= IKafkaConstants.TOPIC_NAME;
			System.out.println("topicName");
		}
		if(country.equals("France"))
		{
			topicName= IKafkaConstants.TOPIC_NAME2;
			System.out.println("topicName");
		}
			final ProducerRecord<Long,String> record = new ProducerRecord<Long,String>(topicName,inputData);
		
			try {
				RecordMetadata metadata = producer.send(record).get();
				//producer.send(record, new DemoCallback());
				System.out.println("data");
				System.out.println("Record sent with key " + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		
		}
		
	
	
}
	
