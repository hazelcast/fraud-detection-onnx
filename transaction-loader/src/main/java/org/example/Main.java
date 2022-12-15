package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import datamodel.Transaction;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.AbstractMap;
import java.util.Properties;

public class Main {


    private static HazelcastInstance startHazelcastInstances() {

        Config cfg = new Config();
        cfg.getJetConfig().setEnabled(true);
        HazelcastInstance hazelcastInstance =  Hazelcast.newHazelcastInstance(cfg);
        hazelcastInstance.getJet().getConfig().setResourceUploadEnabled(true);
        return hazelcastInstance;

    }
    public static void main(String[] args) throws InterruptedException {

        HazelcastInstance hazelcastInstance = startHazelcastInstances();
        Thread.sleep(5000);

        if (args!=null && args.length ==3) {
            submitTransactionLoaderJob(args[0],args[1],args[2],hazelcastInstance,"transactionLoader");

            //Sleep while job is running
            while (! hazelcastInstance.getJet().getJob("transactionLoader").getStatus().equals(JobStatus.COMPLETED)) {
                Thread.sleep(3000);
            }
            //hazelcastInstance.shutdown();
        } else {
            System.out.println("Expected 3 command line arguments: <transactionFileFolder> <transactionFileName> <kafkaserver:port>");
        }


    }
    private static void submitTransactionLoaderJob (String transactionFolder, String transactionFilename, String bootstrapServers,HazelcastInstance hazelcastInstance,String jobName) {
        //Batch Source = is a CSV File
        BatchSource<Transaction> source = FileSources.files(transactionFolder)
                .glob(transactionFilename)
                .format(FileFormat.csv(Transaction.class))
                .build();

        //Create a Pipeline to Read Transactions from CSV Source and push to Kafka Transaction Topic
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .map(transaction -> {
                    String key = transaction.getTrans_num();
                    ObjectMapper mapper = new ObjectMapper();
                    //Converting the Transaction to JSONString
                    String jsonString = mapper.writeValueAsString(transaction);
                    return new AbstractMap.SimpleEntry<>(key, jsonString);
                } )
                .writeTo(KafkaSinks.kafka(getKafkaBrokerProperties(bootstrapServers), "Transactions"));
                //.writeTo(Sinks.logger());
        //Execute the Job
        submitJob(pipeline,hazelcastInstance,jobName);
    }
    private static Properties getKafkaBrokerProperties (String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        return props;
    }

    private static void submitJob (Pipeline p, HazelcastInstance hazelcastInstance, String jobName)  {

        JobConfig jobCfg = new JobConfig();
        jobCfg.setName(jobName);
        jobCfg.addClass(Transaction.class);
        jobCfg.addClass(Main.class);
        hazelcastInstance.getJet().newJob(p, jobCfg);
    }



}