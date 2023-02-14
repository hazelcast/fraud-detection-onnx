package org.example.datamodel;


import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class Merchant implements Portable {
    private String category;
    private String merchant_name;
    private Long merchantCode;

    public Long getMerchantCode() {
        return merchantCode;
    }

    public void setMerchantCode(Long merchantCode) {
        this.merchantCode = merchantCode;
    }

    public Merchant() {}

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getMerchant_name() {
        return merchant_name;
    }

    public void setMerchant_name(String merchant_name) {
        this.merchant_name = merchant_name;
    }

    @Override
    public String toString() {
        String header = "\nMERCHANT***************** \n";
        String name = "Merchant = " + this.merchant_name + "\n";
        String category = "Category = " + this.category + "\n";
        String code = "Merchant Code = " + this.merchantCode + "\n";
        return (header + name + category + code);
    }

    final static int CLASS_ID = 1;
    final static int FACTORY_ID = 1;
    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeString("category", category);
        writer.writeString("merchant_name", merchant_name);
        if (merchantCode!=null) {
            writer.writeLong("merchantCode", merchantCode.longValue());
        }
    }
    @Override
    public void readPortable(PortableReader reader) throws IOException {
       category =  reader.readString("category");
       merchant_name = reader.readString("merchant_name");
       merchantCode = Long.valueOf(reader.readLong("merchantCode"));
    }
}
