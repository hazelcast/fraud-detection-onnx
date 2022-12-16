package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.map.IMap;
import org.example.datamodel.Customer;
import org.example.datamodel.Merchant;
import org.example.util.Util;

import java.util.AbstractMap;
import java.util.Map;

import static org.example.util.Util.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        //arg[0] must be Hazelcast server:port (e.g. "192.168.0.135:5701")
        HazelcastInstance client = Util.getHazelClient(args[0]);
        if (!args[0].isBlank()) {
            if (! isOnlineFeatureDataLoaded(client)) {
                //Load feature dictionaries into Hazelcast Maps
                loadPreProcessingDictionary(client,AGE_GROUP_DICTIONARY_FOLDER,AGE_GROUP_MAP);
                loadPreProcessingDictionary(client,CATEGORY_DICTIONARY_FOLDER,CATEGORY_MAP);
                loadPreProcessingDictionary(client,GENDER_DICTIONARY_FOLDER,GENDER_MAP);
                loadPreProcessingDictionary(client,JOB_DICTIONARY_FOLDER,JOB_MAP);
                loadPreProcessingDictionary(client,SETTING_DICTIONARY_FOLDER,SETTING_MAP);
                loadPreProcessingDictionary(client,TRANSACTION_HOUR_DICTIONARY_FOLDER,TRANSACTION_HOUR_MAP);
                loadPreProcessingDictionary(client,TRANSACTION_MONTH_DICTIONARY_FOLDER,TRANSACTION_MONTH_MAP);
                loadPreProcessingDictionary(client,TRANSACTION_WEEKDAY_DICTIONARY_FOLDER,TRANSACTION_WEEKDAY_MAP);
                loadPreProcessingDictionary(client,ZIP_DICTIONARY_FOLDER,ZIP_MAP);
                // Load Customer and Merchant Features into Hazelcast Maps
                loadCustomerFeatureData(client);
                loadMerchantFeatureData(client);
            }
        }
        Thread.sleep(2000);
        client.shutdown();
        System.out.println("\n******************* All Feature Loading Jobs submitted to Hazelcast! *******************\n");
    }
    private static void loadPreProcessingDictionary (HazelcastInstance client, String folder, String mapName) {
        // Simple pipeline to load a dictionary-like JSON file stored in <folder> into a Hazelcast IMap <mapName>
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.json(folder))
                .flatMap(map -> {
                    Object[] entries = map.entrySet().toArray();
                    return (Traversers.traverseArray(entries));
                })
                .map (o -> (Map.Entry<String, Long>) o)
                .writeTo(Sinks.map(mapName));
        Util.submitJob(pipeline,client,mapName);
    }
    private static void loadCustomerFeatureData (HazelcastInstance client) {

        //Batch Source = Unified File Connector
        BatchSource<Customer> source = FileSources.files(Util.CUSTOMER_DATA_FOLDER)
                .glob(Util.CUSTOMER_FILENAME)
                .format(FileFormat.csv(Customer.class))
                .build();

        //Read Customers from CSV,Read Customer Codes from JSON, Join and save to Customer MAP
        Pipeline pipeline = Pipeline.create();
        BatchStage<Customer> customers = pipeline.readFrom(source);

        BatchStage<Map.Entry<Long, Long>> customerCardCodes = pipeline.readFrom(Sources.json(Util.CUSTOMER_DICTIONARY_FOLDER))
                .flatMap(map -> {
                    Object[] entries = map.entrySet().toArray();
                    return (Traversers.traverseArray(entries));
                })
                .map(o -> {
                    Map.Entry<String, Long> entry = (Map.Entry<String, Long>) o;
                    Long newKey = Long.parseLong(entry.getKey());
                    Long value = ((Number) entry.getValue()).longValue();
                    return new AbstractMap.SimpleEntry<>(newKey, value);

                });
        BatchStage<Customer> joined = customers.hashJoin(customerCardCodes,
                JoinClause.joinMapEntries(Customer::getCc_num),
                (customer, ccCode) -> {
                    customer.setCode((ccCode));
                    return customer;
                });
        SinkStage fullJob = joined.map(customer -> new AbstractMap.SimpleEntry<Long,Customer>(customer.getCc_num(), customer))
                .writeTo(Sinks.map(Util.CUSTOMER_MAP));

        Util.submitJob(pipeline,client,"Customer Features");

    }
    private static void loadMerchantFeatureData (HazelcastInstance client) {

        //Batch Source = Unified File Connector - to Read from CSV file
        BatchSource<Merchant> source = FileSources.files(Util.MERCHANT_DATA_FOLDER)
                .glob(Util.MERCHANT_FILENAME)
                .format(FileFormat.csv(Merchant.class))
                .build();

        //Time to create the pipeline: Read Customers from CSV,Load into Customer Map
        Pipeline pipeline = Pipeline.create();
        BatchStage<Merchant> merchants = pipeline.readFrom(source);

        BatchStage<Map.Entry<String, Long>> merchantCodes = pipeline.readFrom(Sources.json(Util.MERCHANT_DICTIONARY_FOLDER))
                .flatMap(map -> {
                    Object[] entries = map.entrySet().toArray();
                    return (Traversers.traverseArray(entries));
                })
                .map(o -> {
                    Map.Entry<String, Long> entry = (Map.Entry<String, Long>) o;
                    String newKey = entry.getKey();
                    Long value = ((Number) entry.getValue()).longValue();
                    return new AbstractMap.SimpleEntry<>(newKey, value);
                });

        BatchStage<Merchant> joined = merchants.hashJoin(merchantCodes,
                JoinClause.joinMapEntries(Merchant::getMerchant_name),
                (merchant, merchantCode) -> {
                    merchant.setMerchantCode(merchantCode);
                    return merchant;
                });
        SinkStage fullJob = joined.map(merchant -> new AbstractMap.SimpleEntry <String, Merchant>(merchant.getMerchant_name(), merchant))
                .writeTo(Sinks.map(Util.MERCHANT_MAP));

        Util.submitJob(pipeline,client,"Merchant Features");

    }
    private static boolean isOnlineFeatureDataLoaded(HazelcastInstance client) {
        IMap<Long, Customer> customerIMap = client.getMap(Util.CUSTOMER_MAP);
        if (customerIMap!=null) {
            return (customerIMap.size() > 0);
        }
        return false;
    }
}