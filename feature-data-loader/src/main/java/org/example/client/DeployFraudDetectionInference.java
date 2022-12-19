package org.example.client;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.org.json.JSONObject;
import org.example.fraudmodel.FraudDetectionRequest;
import org.example.fraudmodel.FraudDetectionResponse;
import org.example.fraudmodel.LightGBMFraudDetectorService;
import org.example.util.Util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;

import static org.example.util.Util.isOnlineFeatureDataLoaded;

public class DeployFraudDetectionInference {
    private static final String KAFKA_TOPIC = "Transactions";
    private static final String SCORING_JOB_NAME = "ScoringJobPipeline";

    public static void main(String[] args) throws InterruptedException {
        //arg[0] must be Hazelcast server:port (e.g. "192.168.0.135:5701")
        HazelcastInstance client = Util.getHazelClient(args[0]);
        System.out.println("Connecting to Hazelcast at " + args[0]);
        //arg[1] must be kafka broker:port (e.g "localhost:9092")
        String kafkaBroker = args[1];
        System.out.println("Pulling transactions from Kafka broker " + args[1]);
        //arg[2] must be onnxFilename (e.g "fraudmodel-1.0.onnx")
        String onnxModelFileName = args[2];
        System.out.println("Name of the Onnx Model " + args[2]);

        if (isOnlineFeatureDataLoaded(client)) {
            submitFraudDetectionInferencePipeline(client,kafkaBroker,onnxModelFileName);
            Thread.sleep(2000);


        } else {
            System.out.println("Incorrect input parameters. Expected hazelcastIP:port kafkaIP:port");
        }
        client.shutdown();

    }

    private static void submitFraudDetectionInferencePipeline(HazelcastInstance client,String kafkaBroker,String onnxModelFileName) throws InterruptedException {

        // Set up ONNX Fraud Detection Model as a Service in the transaction processing pipeline
        ServiceFactory<?, LightGBMFraudDetectorService> fraudCheckingService = ServiceFactories
                .sharedService(ctx ->new LightGBMFraudDetectorService(onnxModelFileName))
                .toNonCooperative();

        Pipeline p = Pipeline.create();

        p.readFrom(KafkaSources.kafka(
                        Util.kafkaConsumerProps(kafkaBroker),
                        cr -> new AbstractMap.SimpleEntry<>(cr.key().toString(),new JSONObject(cr.value().toString())),
                        KAFKA_TOPIC))
                .withNativeTimestamps(0)
                .setLocalParallelism(5)
                //retrieve Merchant Features
                .mapUsingIMap(Util.MERCHANT_MAP,
                        tup -> tup.getValue().getString("merchant"),
                        (tup,merchant)-> Tuple2.tuple2(
                                tup.getValue(),
                                Util.getMerchantFrom((GenericRecord) merchant)))
                //retrieve Customer Features
                .mapUsingIMap(Util.CUSTOMER_MAP,
                        tup -> tup.f0().getLong("cc_num"),
                        (tup,customer)-> Tuple3.tuple3(tup.f0(),tup.f1(),
                                Util.getCustomerFrom((GenericRecord) customer)))
                //Calculate Realtime Feature "Distance from Home"
                .map (tup -> {
                    double distanceKms = Util.calculateDistanceKms(
                            tup.f0().getDouble("merch_lat"),
                            tup.f0().getDouble("merch_long"),
                            tup.f2().getLatitude().doubleValue(),
                            tup.f2().getLongitude().doubleValue());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),distanceKms);})
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
                        tup -> String.format("%02d", LocalDateTime.parse(tup.f0().getString("transaction_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getHour()),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setTransaction_hour((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Transaction Month Code
                .mapUsingIMap(Util.TRANSACTION_MONTH_MAP,
                        tup -> String.valueOf(LocalDateTime.parse(tup.f0().getString("transaction_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getMonthValue()),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setTransaction_month((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Transaction Day of Week Code
                .mapUsingIMap(Util.TRANSACTION_WEEKDAY_MAP,
                        tup -> String.valueOf(LocalDateTime.parse(tup.f0().getString("transaction_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getDayOfWeek().getValue()-1),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            assert fdr != null;
                            fdr.setTransaction_weekday((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Time to Call the Fraud Detection Model and get a prediction!
                .mapUsingService(fraudCheckingService, (service, tup) -> {
                    FraudDetectionResponse prediction =  service.getFraudProbability(tup.f2());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),prediction);
                })
                .filter (tup -> tup.f3().getFraudProbability() > 0.3)
                .writeTo(Sinks.logger());

        Util.submitJob(p, client, SCORING_JOB_NAME);
        Thread.sleep(2000);

    }



}
