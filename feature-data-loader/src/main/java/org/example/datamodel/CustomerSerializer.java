package org.example.datamodel;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class CustomerSerializer implements CompactSerializer<Customer> {
    @Override
    public Customer read(CompactReader reader) {
        Customer customer = new Customer();
        customer.setSsn(reader.readString("ssn"));
        customer.setCc_num(reader.readNullableInt64("cc_num"));
        customer.setFirst(reader.readString("first"));
        customer.setLast(reader.readString("last"));
        customer.setGender(reader.readString("gender"));
        customer.setStreet(reader.readString("street"));
        customer.setCity(reader.readString("city"));
        customer.setState(reader.readString("state"));
        customer.setZip(reader.readString("zip"));
        customer.setLatitude(reader.readNullableFloat32("latitude"));
        customer.setLongitude(reader.readNullableFloat32("longitude"));
        customer.setCity_pop(reader.readInt32("city_pop"));
        customer.setJob(reader.readString("job"));
        customer.setDob(reader.readString("dob"));
        customer.setAcct_num(reader.readString("acct_num"));
        customer.setProfile(reader.readString("profile"));
        customer.setAge(reader.readInt32("age"));
        customer.setSetting(reader.readString("setting"));
        customer.setAge_group(reader.readString("age_group"));
        customer.setCode(reader.readNullableInt64("code"));
        return customer;
    }

    @Override
    public void write(CompactWriter writer, Customer customer) {
        writer.writeString("ssn", customer.getSsn());
        writer.writeNullableInt64("cc_num", customer.getCc_num());
        writer.writeString("first", customer.getFirst());
        writer.writeString("last", customer.getLast());
        writer.writeString("gender", customer.getGender());
        writer.writeString("street", customer.getStreet());
        writer.writeString("city", customer.getCity());
        writer.writeString("state", customer.getState());
        writer.writeString("zip", customer.getZip());
        writer.writeNullableFloat32("latitude", customer.getLatitude());
        writer.writeNullableFloat32("longitude", customer.getLongitude());
        writer.writeInt32("city_pop", customer.getCity_pop());
        writer.writeString("job", customer.getJob());
        writer.writeString("dob", customer.getDob());
        writer.writeString("acct_num", customer.getAcct_num());
        writer.writeString("profile", customer.getProfile());
        writer.writeInt32("age", customer.getAge());
        writer.writeString("setting", customer.getSetting());
        writer.writeString("age_group", customer.getAge_group());
        writer.writeNullableInt64("code", customer.getCode());
    }

    @Override
    public String getTypeName() {
        return "customer";
    }

    @Override
    public Class<Customer> getCompactClass() {
        return Customer.class;
    }
}
