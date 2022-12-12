package org.example.fraudclient;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.example.datamodel.Customer;
import org.example.datamodel.Merchant;
import org.example.fraudmodel.FraudDetectionRequest;
import org.example.fraudmodel.FraudDetectionResponse;
import org.example.fraudmodel.LightGBMFraudDetectorService;
import org.example.fraudmodel.TransactionScoringRequest;

import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;

import static org.example.fraudclient.Util.*;

public class FraudDetectionClient {
    public static void main(String[] args) throws InterruptedException {

        //Util.BASE_DIR = "/Users/esandoval/Documents/fraud-detection-onnx/hz-onnx-debian";

        HazelcastInstance client = Util.getHazelClient();

        if (! isOnlineFeatureDataLoaded(client)) {

            //Load Model Pre-processing Maps
            loadPreProcessingDictionary(client,Util.AGE_GROUP_DICTIONARY_FOLDER,Util.AGE_GROUP_MAP);
            loadPreProcessingDictionary(client,Util.CATEGORY_DICTIONARY_FOLDER, Util.CATEGORY_MAP);
            loadPreProcessingDictionary(client, Util.GENDER_DICTIONARY_FOLDER, Util.GENDER_MAP);
            loadPreProcessingDictionary(client, Util.JOB_DICTIONARY_FOLDER,Util.JOB_MAP);
            loadPreProcessingDictionary(client,Util.SETTING_DICTIONARY_FOLDER,Util.SETTING_MAP);
            loadPreProcessingDictionary(client,Util.TRANSACTION_HOUR_DICTIONARY_FOLDER,Util.TRANSACTION_HOUR_MAP);
            loadPreProcessingDictionary(client,Util.TRANSACTION_MONTH_DICTIONARY_FOLDER,Util.TRANSACTION_MONTH_MAP);
            loadPreProcessingDictionary(client,Util.TRANSACTION_WEEKDAY_DICTIONARY_FOLDER,Util.TRANSACTION_WEEKDAY_MAP);
            loadPreProcessingDictionary(client,Util.ZIP_DICTIONARY_FOLDER,Util.ZIP_MAP);

            //Load Customer and Merchant Features
            loadCustomerFeatureData(client);
            loadMerchantFeatureData(client);

            // Set up Transaction Processing Job to process fraud detection requests!
            createAndSubmitTransactionProcessingJob (client);
            Thread.sleep(3000);
        }

        //Feed some test transactions and get fraud detection predictions!
        feedTransactions(client);
        client.shutdown();

    }
    private static void createAndSubmitTransactionProcessingJob (HazelcastInstance client) {

        // Set up ONNX Fraud Detection Model as a Service in the transaction processing pipeline
        ServiceFactory<?, LightGBMFraudDetectorService> fraudCheckingService = ServiceFactories
                .sharedService(ctx ->new LightGBMFraudDetectorService(Util.MODEL_NAME))
                .toNonCooperative();

        // Create the pipeline
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Long, TransactionScoringRequest>mapJournal(TRANSACTION_MAP, JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .map(Map.Entry::getValue)
                //retrieve Merchant Features
                .mapUsingIMap(Util.MERCHANT_MAP,
                        tup -> tup.getMerchant(),
                        (tup,merchant)-> Tuple2.tuple2(
                                tup,
                                Util.getMerchantFrom((GenericRecord) merchant)))
                //retrieve Customer Features
                .mapUsingIMap(Util.CUSTOMER_MAP,
                        tup -> tup.f0().getCreditCardNumber(),
                        (tup,customer)-> Tuple3.tuple3(tup.f0(),tup.f1(),
                                Util.getCustomerFrom((GenericRecord) customer)))
                //Calculate Realtime Feature "Distance from Home"
                .map (tup -> {
                    double distanceKms = Util.calculateDistanceKms(
                            tup.f0().getMerchantLatitude(),
                            tup.f0().getMerchantLongitude(),
                            tup.f2().getLatitude().doubleValue(),
                            tup.f2().getLongitude().doubleValue());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),distanceKms);
                })
                //Start Feature Processing -> Create a FraudDetectionRequest
                .map(tup -> {
                    FraudDetectionRequest fdr = Util.createFrom(
                            tup.f0(),
                            tup.f2(),
                            tup.f1(),
                            tup.f3());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),fdr);
                })
                // Pre-processing Categorical Features - Merchant Category
                .mapUsingIMap(Util.MERCHANT_MAP,
                        tup -> tup.f1().getMerchant_name(),
                        (tup , gr)-> {
                            FraudDetectionRequest fdr = tup.f3();
                            GenericRecord merchant = (GenericRecord) gr;
                            fdr.setMerchant((int) merchant.getInt64("merchantCode"));
                            return Tuple3.tuple3(tup.f0(),tup.f2(),fdr);
                        })
                //Pre-processing Categorical Features - Gender
                .mapUsingIMap(Util.GENDER_MAP,
                        tup -> tup.f1().getGender(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setGender((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                .mapUsingIMap(Util.JOB_MAP,
                        tup -> tup.f1().getJob(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setJob((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Age Group
                .mapUsingIMap(Util.AGE_GROUP_MAP,
                        tup -> tup.f1().getAge_group(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setAge_group((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Customer Setting (Rural or Urban)
                .mapUsingIMap(Util.SETTING_MAP,
                        tup -> tup.f1().getSetting(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setSetting((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - ZipCode
                .mapUsingIMap(Util.ZIP_MAP,
                        tup -> tup.f1().getZip(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setZip((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Transaction Hour Code
                .mapUsingIMap(Util.TRANSACTION_HOUR_MAP,
                        tup -> String.format("%02d", tup.f0().getTransactionDate().getHour()),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setTransaction_hour((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Transaction Month Code
                .mapUsingIMap(Util.TRANSACTION_MONTH_MAP,
                        tup -> String.valueOf(tup.f0().getTransactionDate().getMonthValue()),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setTransaction_month((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Transaction Day of Week Code
                .mapUsingIMap(Util.TRANSACTION_WEEKDAY_MAP,
                        tup -> String.valueOf(tup.f0().getTransactionDate().getDayOfWeek().getValue()),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setTransaction_weekday((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Time to Call the Fraud Detection Model and get a prediction!
                .mapUsingService(fraudCheckingService, (service, tup) -> {
                    FraudDetectionResponse prediction =  service.getFraudProbability(tup.f2());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),prediction);
                })
                .setLocalParallelism(10)
                // Keep the Fraud Detection Response
                .map(Tuple4::f3)
                .writeTo((Sinks.logger()));


        Util.submitJob(pipeline,client,"TransactionProcessing");

    }
    private static boolean isOnlineFeatureDataLoaded(HazelcastInstance client) {

        IMap<Long, Customer> customerIMap = client.getMap(CUSTOMER_MAP);
        if (customerIMap!=null) {
            return (customerIMap.size() > 0);
        }
        return false;

    }
    private static void feedTransactions(HazelcastInstance client) {
        //Trigger the pipeline by adding transactions to the map
        IMap<Long, TransactionScoringRequest> transactionMap = client.getMap(Util.TRANSACTION_MAP);

        LocalDateTime transactionTime = LocalDateTime.parse("2022-09-16T18:35:45");
        TransactionScoringRequest t1 = new TransactionScoringRequest(3517182278248964L,
                (float) 327.63, "fraud_Erdman-Schaden", false, transactionTime,
                34.67023, -84.634,"2de6ae16cd114c4d4a7f31f4716b9a07");

        for (int i = 0; i < 10000 ; i++) {
            transactionMap.put(t1.getCreditCardNumber()+i, t1);
        }

        System.out.println("transactions on MAP now");
    }
    private static void loadPreProcessingDictionary (HazelcastInstance client, String folder, String mapName) {

        // Simple pipeline to load a map from a JSON file
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.json(folder))
                .flatMap(map -> {
                    Object[] entries = map.entrySet().toArray();
                    return (Traversers.traverseArray(entries));
                })
                .map (o -> {
                    Map.Entry<String,Long> entry = (Map.Entry<String, Long>) o;
                    return entry;
                })
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
                    Map.Entry<Long, Long> newEntry = new AbstractMap.SimpleEntry<Long, Long>(newKey, value);
                    return newEntry;
                });
        BatchStage<Customer> joined = customers.hashJoin(customerCardCodes,
                JoinClause.joinMapEntries(Customer::getCc_num),
                (customer, ccCode) -> {
                    customer.setCode((ccCode));
                    return customer;
                });
        SinkStage fullJob = joined.map(customer -> Map.entry(customer.getCc_num(), customer))
                .writeTo(Sinks.map(Util.CUSTOMER_MAP));

        Util.submitJob(pipeline,client,"LoadCustomerData");

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
                    String newKey = (String) entry.getKey();
                    Long value = ((Number) entry.getValue()).longValue();
                    Map.Entry<String, Long> newEntry = new AbstractMap.SimpleEntry<String, Long>(newKey, value);
                    return newEntry;
                });

        BatchStage<Merchant> joined = merchants.hashJoin(merchantCodes,
                JoinClause.joinMapEntries(Merchant::getMerchant_name),
                (merchant, merchantCode) -> {
                    merchant.setMerchantCode(merchantCode);
                    return merchant;
                });
        SinkStage fullJob = joined.map(merchant -> Map.entry(merchant.getMerchant_name(), merchant))
                .writeTo(Sinks.map(Util.MERCHANT_MAP));

        Util.submitJob(pipeline,client,"LoadMerchantData");

    }

}
