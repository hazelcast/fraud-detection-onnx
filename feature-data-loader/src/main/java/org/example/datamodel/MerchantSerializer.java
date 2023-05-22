package org.example.datamodel;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class MerchantSerializer implements CompactSerializer<Merchant> {
    @Override
    public Merchant read(CompactReader reader) {
        Merchant merchant = new Merchant();
        merchant.setCategory(reader.readString("category"));
        merchant.setMerchant_name(reader.readString("merchant_name"));
        merchant.setMerchantCode(reader.readNullableInt64("merchantCode"));
        return merchant;
    }

    @Override
    public void write(CompactWriter writer, Merchant merchant) {
        writer.writeString("category", merchant.getCategory());
        writer.writeString("merchant_name", merchant.getMerchant_name());
        writer.writeNullableInt64("merchantCode", merchant.getMerchantCode());
    }

    @Override
    public String getTypeName() {
        return "merchant";
    }

    @Override
    public Class<Merchant> getCompactClass() {
        return Merchant.class;
    }
}
