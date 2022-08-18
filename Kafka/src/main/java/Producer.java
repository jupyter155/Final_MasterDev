import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

import java.util.concurrent.TimeUnit;
public class Producer {
    static String PATH = "/home/minh/Final_MasterDev/data/preprocess_data.csv";
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"my-producer-threading-demo");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.17.80.23:9092"); // 172.17.80.20:9092
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        File file = new File(PATH);
        String topicName = "minhnx12";
        try(Scanner scanner = new Scanner(file)){
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                log.info(line);
                producer.send(new ProducerRecord<>(topicName,null,line));
                TimeUnit.SECONDS.sleep(1);
                producer.flush();

            }
            log.info("Finished Sending");
        }
        producer.close();
    }


}
