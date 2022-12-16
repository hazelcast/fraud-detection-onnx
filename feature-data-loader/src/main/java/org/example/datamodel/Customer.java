package org.example.datamodel;

public class Customer  {
    /*ssn,cc_num,first,last,gender,street,city,state,zip,latitude,longitude,city_pop,job,dob,acct_num,profile,age,setting,age_group*/
    private String ssn;
    private Long cc_num;
    private String first;
    private String last;
    private String gender;
    private String street;
    private String city;
    private String state;
    private String zip;
    private Float latitude;
    private Float longitude;
    private int city_pop;
    private String job;
    private String dob;
    private String acct_num;
    private String profile;
    private int age;
    private String setting;
    private String age_group;

    public Long getCode() {
        return code;
    }

    public void setCode(Long code) {
        this.code = code;
    }

    private Long code;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSetting() {
        return setting;
    }

    public void setSetting(String setting) {
        this.setting = setting;
    }

    public String getAge_group() {
        return age_group;
    }

    public void setAge_group(String age_group) {
        this.age_group = age_group;
    }

    public Customer() {
    }

    @Override
    public String toString() {
        String header = "\n****************CUSTOMER***************** \n";
        String ccnum = "CC Num = " + this.cc_num + "\n";
        String first = "First Name = " + this.first + "\n";
        String last = "Last Name = " + this.last + "\n";
        String gender = "Gender = " + this.gender + "\n";
        String street = "Street = " + this.street + "\n";
        String city = "City = " + this.city + "\n";
        String code = "Customer Code = " + this.code + "\n";
        return (header + ccnum + first + last + gender + street + city + code);
    }

    public String getSsn() {
        return ssn;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public Long getCc_num() {
        return cc_num;
    }

    public void setCc_num(Long cc_num) {
        this.cc_num = cc_num;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getLast() {
        return last;
    }

    public void setLast(String last) {
        this.last = last;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public Float getLatitude() {
        return latitude;
    }

    public void setLatitude(Float latitude) {
        this.latitude = latitude;
    }

    public Float getLongitude() {
        return longitude;
    }

    public void setLongitude(Float longitude) {
        this.longitude = longitude;
    }

    public int getCity_pop() {
        return city_pop;
    }

    public void setCity_pop(int city_pop) {
        this.city_pop = city_pop;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public String getAcct_num() {
        return acct_num;
    }

    public void setAcct_num(String acct_num) {
        this.acct_num = acct_num;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }


}
