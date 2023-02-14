package org.example.client;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;

import com.hazelcast.internal.json.JsonObject;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.*;

import com.hazelcast.org.json.JSONObject;
import org.example.datamodel.Customer;
import org.example.datamodel.Merchant;
import org.example.fraudmodel.FraudDetectionRequest;
import org.example.fraudmodel.FraudDetectionResponse;
import org.example.fraudmodel.LightGBMFraudDetectorService;
import org.example.util.Util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.Map;

import static org.example.client.LoadOnlineFeatures.loadOnlineFeatures;
import static org.example.util.Util.BASE_DIR;
import static org.example.util.Util.isOnlineFeatureDataLoaded;

public class DeployFraudDetectionInference {
    private static final String KAFKA_TOPIC = "Transactions";
    private static final String SCORING_JOB_NAME = "Fraud Detection Inference Pipeline";

    public static void main(String[] args) throws InterruptedException {

        Map<String, String> env = System.getenv();
        String onnxModelFileName = args[0];
        String HZ_ONNX = env.get("HZ_ONNX");
        String KAFKA_CLUSTER_KEY = env.get("KAFKA_CLUSTER_KEY");
        String KAFKA_CLUSTER_SECRET = env.get("KAFKA_CLUSTER_SECRET");
        String KAFKA_CLUSTER_ENDPOINT = env.get("KAFKA_ENDPOINT");
        // get a client connection to Hazelcast-onnx
        HazelcastInstance client = Util.getHazelClient(HZ_ONNX);
        System.out.println("Connecting to Hazelcast at " + HZ_ONNX);
        System.out.println("Pulling transactions from Kafka  " + KAFKA_CLUSTER_ENDPOINT);
        System.out.println("Name of the Onnx Model " + onnxModelFileName);

        if (!isOnlineFeatureDataLoaded(client)) {
            loadOnlineFeatures(client);
        }

        Thread.sleep(2000);
        submitFraudDetectionInferencePipeline(client,KAFKA_CLUSTER_ENDPOINT,onnxModelFileName,KAFKA_CLUSTER_KEY,KAFKA_CLUSTER_SECRET);
        client.shutdown();
    }

    private static void submitFraudDetectionInferencePipeline(HazelcastInstance client,String kafkaBroker,String onnxModelFileName,String kafkaKey, String kafkaSecret) throws InterruptedException {


        // Set up ONNX Fraud Detection Model as a Service in the transaction processing pipeline
        ServiceFactory<?, LightGBMFraudDetectorService> fraudCheckingService = ServiceFactories
                .sharedService(ctx ->new LightGBMFraudDetectorService(BASE_DIR + onnxModelFileName));
                //.toNonCooperative();

        Pipeline p = Pipeline.create();

        p.readFrom(KafkaSources.kafka(
                        Util.kafkaConsumerProps(kafkaBroker, kafkaKey, kafkaSecret),
                        cr -> new AbstractMap.SimpleEntry<>(cr.key().toString(), new JSONObject(cr.value().toString()).put("start_time_nano", System.nanoTime())),
                        KAFKA_TOPIC))
                .withNativeTimestamps(0)
                .setLocalParallelism(10)
                //retrieve Merchant Features
                .mapUsingIMap(Util.MERCHANT_MAP,
                        tup -> tup.getValue().getString("merchant"),
                        (tup, merchant) -> Tuple2.tuple2(
                                tup.getValue(),
                                merchant))

                //retrieve Customer Features
                .mapUsingIMap(Util.CUSTOMER_MAP,
                        tup -> tup.f0().getLong("cc_num"),
                        (tup, customer) -> Tuple3.tuple3(tup.f0(), tup.f1(), customer))

                .filter(tup -> tup.f2()!=null)
                .map (tup -> {
                    double merchantLat = tup.f0().getDouble("merch_lat");
                    double merchantLong = tup.f0().getDouble("merch_long");
                    double customerLat =  ((Customer) tup.f2()).getLatitude().doubleValue();
                    double customerLong = ((Customer) tup.f2()).getLongitude().doubleValue();

                    double distanceKms = Util.calculateDistanceKms(merchantLat,merchantLong,customerLat, customerLong);
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),distanceKms);
                })
                .map(tup -> {
                    FraudDetectionRequest fdr = Util.createFrom(
                            tup.f0(),
                            (Customer) tup.f2(),
                            (Merchant) tup.f1(),
                            tup.f3());
                    return Tuple4.tuple4(tup.f0(),tup.f1(),tup.f2(),fdr);
                })
                // Pre-processing Categorical Features - Merchant Category
                .mapUsingIMap(Util.MERCHANT_MAP,
                        tup -> ((Merchant) tup.f1()).getMerchant_name(),
                        (tup , gr)-> {
                            FraudDetectionRequest fdr = tup.f3();
                            Merchant merchant = (Merchant) gr;
                            fdr.setMerchant((int) merchant.getMerchantCode().intValue());
                            return Tuple3.tuple3(tup.f0(),tup.f2(),fdr);
                        })
                //Pre-processing Categorical Features - Gender
                .mapUsingIMap(Util.GENDER_MAP,
                        tup -> ((Customer) tup.f1()).getGender(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setGender((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                .mapUsingIMap(Util.JOB_MAP,
                        tup -> ((Customer) tup.f1()).getJob(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setJob((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Age Group
                .mapUsingIMap(Util.AGE_GROUP_MAP,
                        tup -> ((Customer) tup.f1()).getAge_group(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setAge_group((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - Customer Setting (Rural or Urban)
                .mapUsingIMap(Util.SETTING_MAP,
                        tup -> ((Customer) tup.f1()).getSetting(),
                        (tup , code)-> {
                            FraudDetectionRequest fdr = tup.f2();
                            fdr.setSetting((int) code);
                            return Tuple3.tuple3(tup.f0(),tup.f1(),fdr);
                        })
                //Pre-processing Categorical Features - ZipCode
                .mapUsingIMap(Util.ZIP_MAP,
                        tup -> ((Customer) tup.f1()).getZip(),
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

                .map(tup -> {
                    JsonObject jsonObject = new JsonObject()
                            .add("transaction_number", tup.f0().getString("trans_num"))
                            .add("transaction_date", tup.f0().getString("transaction_date"))
                            .add("is_fraud", tup.f0().getInt("is_fraud"))
                            .add("amount", tup.f0().getDouble("amt"))
                            .add("merchant", tup.f0().getString("merchant"))
                            .add("merchant_lat", tup.f0().getDouble("merch_lat"))
                            .add("merchant_lon", tup.f0().getDouble("merch_long"))
                            .add("credit_card_number", tup.f0().getLong("cc_num"))
                            .add("customer_name", ((Customer)tup.f1()).getFirst() + " " + ((Customer)tup.f1()).getLast())
                            .add("customer_city", ((Customer)tup.f1()).getCity())
                            .add("customer_age_group", ((Customer)tup.f1()).getAge_group())
                            .add("customer_gender", ((Customer)tup.f1()).getGender())
                            .add("customer_lat", ((Customer)tup.f1()).getLatitude())
                            .add("customer_lon", ((Customer)tup.f1()).getLongitude())
                            .add("distance_from_home",tup.f2().getDistance_from_home())
                            .add("transaction_weekday_code",tup.f2().getTransaction_weekday())
                            .add("transaction_hour_code",tup.f2().getTransaction_hour())
                            .add("transaction_month_code",tup.f2().getTransaction_month())
                            .add("fraud_probability", tup.f3().getFraudProbability())
                            .add("fraud_model_prediction", tup.f3().getPredictedLabel())
                            .add("inference_time_ns", tup.f3().getExecutionTimeNanoseconds())
                            .add("transaction_processing_total_time" , System.nanoTime() -  tup.f0().getLong("start_time_nano"));

                    return jsonObject;
                })
                .writeTo(Sinks.mapWithUpdating("predictionResult",
                        entry -> entry.getString("transaction_number","unknown"),
                        (oldValue, entry) -> new HazelcastJsonValue(entry.toString())));


        Util.submitJob(p, client, SCORING_JOB_NAME);

    }

}
