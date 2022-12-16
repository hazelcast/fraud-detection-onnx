package org.example.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import org.example.Main;
import org.example.datamodel.Customer;
import org.example.datamodel.Merchant;

public class Util {

    public static String BASE_DIR = "/opt/hazelcast/";
    public static final String TRANSACTION_MAP = "transactionScoringRequestMap";
    public static final String GENDER_MAP = "genders";
    public static final String CATEGORY_MAP = "categories";
    public static final String JOB_MAP = "jobs";
    public static final String SETTING_MAP = "settings";
    public static final String AGE_GROUP_MAP = "ageGroups";
    public static final String ZIP_MAP = "zipCodes";
    public static final String MERCHANT_MAP = "merchantMap";
    public static final String CUSTOMER_MAP = "customerMap";
    public static final String TRANSACTION_HOUR_MAP = "transactionHours";
    public static final String TRANSACTION_MONTH_MAP = "transactionMonths";
    public static final String TRANSACTION_WEEKDAY_MAP = "transactionWeedDays";
    public static final String MODEL_NAME = BASE_DIR + "lightgbm_fraud_detection_onnx";
    public static final String AGE_GROUP_DICTIONARY_FOLDER = BASE_DIR + "age-group";
    public static final String CATEGORY_DICTIONARY_FOLDER =  BASE_DIR + "category";
    public static final String GENDER_DICTIONARY_FOLDER = BASE_DIR + "gender";
    public static final String JOB_DICTIONARY_FOLDER = BASE_DIR + "job";
    public static final String ZIP_DICTIONARY_FOLDER = BASE_DIR + "zip";
    public static final String SETTING_DICTIONARY_FOLDER = BASE_DIR + "setting";
    public static final String TRANSACTION_HOUR_DICTIONARY_FOLDER = BASE_DIR + "transaction_hour";
    public static final String TRANSACTION_MONTH_DICTIONARY_FOLDER = BASE_DIR + "transaction_month";
    public static final String TRANSACTION_WEEKDAY_DICTIONARY_FOLDER = BASE_DIR + "transaction_weekday";
    public static final String CUSTOMER_DICTIONARY_FOLDER = BASE_DIR + "cc_num";
    public static final String CUSTOMER_DATA_FOLDER = BASE_DIR ;
    public static final String CUSTOMER_FILENAME = "customer_data.csv";
    public static final String MERCHANT_DICTIONARY_FOLDER = BASE_DIR + "merchant";
    public static final String MERCHANT_FILENAME = "merchant_data.csv";
    public static final String MERCHANT_DATA_FOLDER = BASE_DIR ;

    public static HazelcastInstance getHazelClient(String hazelcastClusterMemberAddresses) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress(hazelcastClusterMemberAddresses);


        //Start the client
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("Connected to Hazelcast Cluster");
        return client;
    }
    public static Customer getCustomerFrom(GenericRecord genericRecord) {
        /*ssn,cc_num,first,last,gender,street,city,state,zip,latitude,longitude,city_pop,job,dob,acct_num,profile,age,setting,age_group*/
        Customer c = new Customer();
        c.setSsn(genericRecord.getString("ssn"));
        c.setCc_num(genericRecord.getInt64("cc_num"));
        c.setFirst(genericRecord.getString("first"));
        c.setLast(genericRecord.getString("last"));
        c.setGender(genericRecord.getString("gender"));
        c.setStreet(genericRecord.getString("street"));
        c.setCity(genericRecord.getString("city"));
        c.setState(genericRecord.getString("state"));
        c.setZip(genericRecord.getString("zip"));
        c.setLatitude(genericRecord.getFloat32("latitude"));
        c.setLongitude(genericRecord.getFloat32("longitude"));
        c.setCity_pop(genericRecord.getInt32("city_pop"));
        c.setDob(genericRecord.getString("dob"));
        c.setJob(genericRecord.getString("job"));
        c.setAcct_num(genericRecord.getString("acct_num"));
        c.setProfile(genericRecord.getString("profile"));
        c.setAge(genericRecord.getInt32("age"));
        c.setSetting(genericRecord.getString("setting"));
        c.setAge_group(genericRecord.getString("age_group"));
        c.setCode(genericRecord.getInt64("code"));
        return c;
    }
    public static Merchant getMerchantFrom(GenericRecord genericRecord) {
        Merchant merchant = new Merchant();
        merchant.setMerchantCode(genericRecord.getInt64("merchantCode"));
        merchant.setMerchant_name(genericRecord.getString("merchant_name"));
        merchant.setCategory(genericRecord.getString("category"));
        return merchant;
    }
    public static void submitJob (Pipeline p, HazelcastInstance client, String jobName)  {
        JobConfig jobCfg = new JobConfig().setName(jobName);
        jobCfg.addClass(Merchant.class);
        jobCfg.addClass(Customer.class);
        jobCfg.addClass(Util.class);
        jobCfg.addClass(Main.class);

        Job existingJob = client.getJet().getJob(jobName);
        if (existingJob!=null) {
            try {
                existingJob.cancel();
                Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        System.out.println("*************** Loading " + jobName + " to Hazelcast******************\n");
        client.getJet().newJob(p, jobCfg);
    }
}
