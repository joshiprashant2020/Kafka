import java.util.*;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class AvroProducer {

	public static void main(String[] args) throws Exception{

        
        String msg;
        String csvFile = "/home/edyoda/SchemaData/OnlineRetail.csv";
        BufferedReader br = null;
        String record = "";
        
        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((record = br.readLine()) != null) {

                
                System.out.println("record= " + record );
                runProducer(record);
            	}
            	
            	
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
	static void runProducer(String record)
	{	
		String topicName = "demo";
		String cvsSplitBy = ",";
		String data []= record.split(cvsSplitBy);
		
		System.out.println(data[0]);
		String Description = data[0].trim();
		String Quantity = data[1].trim();
		String UnitPrice = data[2].trim();
		System.out.println("Description:"+Description);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");        
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, ClickRecord> producer = new KafkaProducer <>(props);
        ClickRecord cr = new ClickRecord();
        try{
           // cr.setSessionId("10001");
            cr.setDescription(Description);
            cr.setQuantity(Quantity);
            cr.setUnitPrice(UnitPrice);

            producer.send(new ProducerRecord<String, ClickRecord>(topicName,cr)).get();

            System.out.println("Complete");
        }
        catch(Exception ex){
            ex.printStackTrace(System.out);
        }
        finally{
            producer.close();
        }


   }
}
