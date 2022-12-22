package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import datamodel.Transaction;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Main {



    private static HazelcastInstance getHazelClient(String hazelcastClusterMemberAddresses)  {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress(hazelcastClusterMemberAddresses);

        //Start the client
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("Connected to Hazelcast Cluster");
        return client;
    }

    private static final String TRANSACTION_FOLDER= "/opt/hazelcast/transaction";
    public static void main(String[] args) throws InterruptedException {

        Map<String, String> env = System.getenv();
        String transactionFileName = args[0];
        String HZ_ONNX = env.get("HZ_ONNX");
        String KAFKA_CLUSTER_KEY = env.get("KAFKA_CLUSTER_KEY");
        String KAFKA_CLUSTER_SECRET = env.get("KAFKA_CLUSTER_SECRET");
        String KAFKA_CLUSTER_ENDPOINT = env.get("KAFKA_ENDPOINT");

        HazelcastInstance client = getHazelClient(HZ_ONNX);
        Thread.sleep(3000);

        if (!transactionFileName.isBlank()) {

            submitTransactionLoaderJob(TRANSACTION_FOLDER,
                    transactionFileName,
                    KAFKA_CLUSTER_ENDPOINT
                    ,client,
                    "transactionLoader",
                    KAFKA_CLUSTER_KEY,
                    KAFKA_CLUSTER_SECRET);

            System.out.println("Transaction Loading Job submitted to Hazelcast");


        } else {
            System.out.println("Unable to submit job. Please check environment variables are set and file name is correct.");
        }

        client.shutdown();


    }
    private static void submitTransactionLoaderJob (String transactionFolder, String transactionFilename, String bootstrapServers,HazelcastInstance hazelcastInstance,String jobName,String clusterKey, String clusterSecret) {
        //Batch Source = is a CSV File
        BatchSource<Transaction> source = FileSources.files(transactionFolder)
                .glob(transactionFilename)
                .format(FileFormat.csv(Transaction.class))
                .build();

        //Create a Pipeline to Read Transactions from CSV Source and push to Kafka Transaction Topic
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .map(transaction -> {
                    String key = transaction.getCc_num().toString();
                    //Converting the Transaction to JSONString
                    ObjectMapper mapper = new ObjectMapper();
                    String jsonString = mapper.writeValueAsString(transaction);
                    return new AbstractMap.SimpleEntry<>(key, jsonString);
                } )
                .writeTo(KafkaSinks.kafka(getKafkaBrokerProperties(bootstrapServers,clusterKey,clusterSecret), "Transactions"));
        //Execute the Job
        submitJob(pipeline,hazelcastInstance,jobName);
    }
    private static Properties getKafkaBrokerProperties (String bootstrapServers, String clusterKey, String clusterSecret) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        //Cloud
        props.setProperty("security.protocol","SASL_SSL");
        props.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='" + clusterKey  + "' password='" + clusterSecret  +"';");
        props.setProperty("sasl.mechanism","PLAIN");
        props.setProperty("session.timeout.ms","45000");
        props.setProperty("client.dns.lookup","use_all_dns_ips");
        props.setProperty("acks","all");

        return props;
    }

    private static void submitJob (Pipeline p, HazelcastInstance hazelcastInstance,String jobName)  {

        JobConfig jobCfg = new JobConfig();
        jobCfg.setName(jobName + UUID.randomUUID());
        jobCfg.addClass(Transaction.class);
        jobCfg.addClass(Main.class);

        hazelcastInstance.getJet().newJob(p, jobCfg);


    }



}