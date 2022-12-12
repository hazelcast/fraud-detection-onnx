package org.example.fraudmodel;

public interface FraudDetectionService {
    FraudDetectionResponse getFraudProbability(FraudDetectionRequest r) throws RuntimeException;
}
