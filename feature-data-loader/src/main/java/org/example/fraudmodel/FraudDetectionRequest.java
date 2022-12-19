package org.example.fraudmodel;

import java.io.Serializable;

public class FraudDetectionRequest  {
    private int cc_num;
    private int category;
    private float amt;
    private int merchant;
    private int gender;
    private float city_pop;
    private int job;
    private int age;
    private int transaction_weekday;
    private int transaction_month;
    private int transaction_hour;
    private int zip;

    private int setting;
    private int age_group;
    private float distance_from_home;

    public FraudDetectionRequest() {
    }

    public int getCc_num() {
        return cc_num;
    }

    public void setCc_num(int cc_num) {
        this.cc_num = cc_num;
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public float getAmt() {
        return amt;
    }

    public void setAmt(float amt) {
        this.amt = amt;
    }

    public int getMerchant() {
        return merchant;
    }

    public void setMerchant(int merchant) {
        this.merchant = merchant;
    }

    public int getTransaction_weekday() {
        return transaction_weekday;
    }

    public void setTransaction_weekday(int transaction_weekday) {
        this.transaction_weekday = transaction_weekday;
    }

    public int getTransaction_month() {
        return transaction_month;
    }

    public void setTransaction_month(int transaction_month) {
        this.transaction_month = transaction_month;
    }

    public int getTransaction_hour() {
        return transaction_hour;
    }

    public void setTransaction_hour(int transaction_hour) {
        this.transaction_hour = transaction_hour;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public int getZip() {
        return zip;
    }

    public void setZip(int zip) {
        this.zip = zip;
    }

    public float getCity_pop() {
        return city_pop;
    }

    public void setCity_pop(float city_pop) {
        this.city_pop = city_pop;
    }

    public int getJob() {
        return job;
    }

    public void setJob(int job) {
        this.job = job;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSetting() {
        return setting;
    }

    public void setSetting(int setting) {
        this.setting = setting;
    }

    public int getAge_group() {
        return age_group;
    }

    public void setAge_group(int age_group) {
        this.age_group = age_group;
    }

    public float getDistance_from_home() {
        return distance_from_home;
    }

    public void setDistance_from_home(float distance_from_home) {
        this.distance_from_home = distance_from_home;
    }

    public String toString() {
        String header = "FRAUD DETECTION REQUEST *****************\n";
        String ccCode = "Credit Card Code " + this.cc_num + "\n";
        String amt = "Amount " + this.amt + "\n";
        String merchant = "Merchant Code " + this.merchant + "\n";
        String zip = "Zip (int) " + this.zip + "\n";
        String ageGroup = "Age Group " + this.age_group + "\n";
        String age = "Age " + this.age + "\n";
        String category = "Category Code " + this.category + "\n";
        String distance = "Distance from home (kms) " + this.distance_from_home + "\n";
        String job = "Job Code " + this.job + "\n";
        String gender = "Gender Code " + this.gender + "\n";
        String cityPop = "City Population " + this.city_pop + "\n";
        String ruralOrUrbanSetting = "Customer Setting Code " + this.setting + "\n";
        String transactionHour = "Transaction Hour Code " + this.transaction_hour + "\n";
        String transactionWeekday = "Transaction Weekday Code " + this.transaction_weekday + "\n";
        String transactionMonth = "Transaction Month Code " + this.transaction_month + "\n";

        return (header + ccCode + amt + merchant + zip + ageGroup + age + category + distance +
                job + gender + cityPop + ruralOrUrbanSetting + transactionHour + transactionWeekday + transactionMonth);
    }


}
/*
cc_num,category,amt,merchant,transaction_weekday,transaction_month,transaction_hour,gender,zip,city_pop,job,age,setting,age_group,distance_from_home
 */