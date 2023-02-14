package org.example.datamodel;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class CustomerFactory implements PortableFactory {
    @Override
    public Portable create(int classId) {
        if (classId == 2) {
            return new Customer();
        }
        return null;
    }
}
