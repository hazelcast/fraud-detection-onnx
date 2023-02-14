package org.example.datamodel;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class MerchantFactory implements PortableFactory {
    @Override
    public Portable create(int classId) {
        if (classId == 1) {
            return new Merchant();
        }
        return null;
    }
}
