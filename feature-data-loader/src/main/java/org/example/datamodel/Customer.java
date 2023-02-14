package org.example.datamodel;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class Customer  implements Portable {
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
        String lat = "Customer Home Lat = " + this.latitude + "\n";
        String longitude = "Customer Home Long = " + this.longitude + "\n";
        return (header + ccnum + first + last + gender + street + city + code + lat + longitude);
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


    @Override
    public int getFactoryId() {
        return 2;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        /*ssn,cc_num,first,last,gender,street,city,state,zip,latitude,longitude,city_pop,job,dob,acct_num,profile,age,setting,age_group*/
        writer.writeString("ssn", ssn);
        if (cc_num!=null)
            writer.writeLong("cc_num", Long.valueOf(cc_num));
        writer.writeString("first", first);
        writer.writeString("last", last);
        writer.writeString("gender", gender);
        writer.writeString("street", street);
        writer.writeString("city", city);
        writer.writeString("state", state);
        writer.writeString("zip", zip);
        if (latitude!=null)
            writer.writeFloat("latitude", latitude.floatValue());
        if (longitude!=null)
            writer.writeFloat("longitude", longitude.floatValue());
        writer.writeInt("city_pop", city_pop);
        writer.writeString("job", job);
        writer.writeString("dob", dob);
        writer.writeString("acct_num", acct_num);
        writer.writeString("profile", profile);
        writer.writeInt("age", age);
        writer.writeString("setting", setting);
        writer.writeString("age_group", age_group);
        if (code!=null) {
            writer.writeLong("code", code);
        }



    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        ssn = reader.readString("ssn");
        cc_num = Long.valueOf(reader.readLong("cc_num"));
        first = reader.readString("first");
        last = reader.readString("last");
        gender = reader.readString("gender");
        street = reader.readString("street");
        city = reader.readString("city");
        state = reader.readString("state");
        zip = reader.readString("zip");
        latitude = Float.valueOf(reader.readFloat("latitude"));
        longitude = Float.valueOf(reader.readFloat("longitude"));
        city_pop = reader.readInt("city_pop");
        job = reader.readString("job");
        dob = reader.readString("dob");
        acct_num = reader.readString("acct_num");
        profile = reader.readString("profile");
        age = reader.readInt("age");
        setting = reader.readString("setting");
        age_group = reader.readString("age_group");
        code = Long.valueOf(reader.readLong("code"));



    }
}
