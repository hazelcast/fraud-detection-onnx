package org.example.fraudmodel;

import java.io.Serializable;

public class FraudDetectionResponse  {
    public static final long NO_FRAUD = 0;
    public static final long FRAUD = 1;
    private long predictedLabel;
    private float noFraudProbability;

    private float fraudProbability;
    private long executionTimeNanoseconds;

    public FraudDetectionResponse(long predictedLabel, float noFraudProbability, float fraudProbability, long executionTimeNanoseconds) {
        this.predictedLabel = predictedLabel;
        this.noFraudProbability = noFraudProbability;
        this.fraudProbability = fraudProbability;
        this.executionTimeNanoseconds = executionTimeNanoseconds;
    }

    public long getExecutionTimeNanoseconds() {
        return executionTimeNanoseconds;
    }

    public void setExecutionTimeNanoseconds(long executionTimeNanoseconds) {
        this.executionTimeNanoseconds = executionTimeNanoseconds;
    }


    public FraudDetectionResponse(long predictedLabel, float noFraudProbability, float fraudProbability) {
        this.predictedLabel = predictedLabel;
        this.noFraudProbability = noFraudProbability;
        this.fraudProbability = fraudProbability;
    }

    public long getPredictedLabel() {
        return predictedLabel;
    }


    public void setPredictedLabel(long predictedLabel) {
        this.predictedLabel = predictedLabel;
    }

    public float getNoFraudProbability() {
        return noFraudProbability;
    }

    public void setNoFraudProbability(float noFraudProbability) {
        this.noFraudProbability = noFraudProbability;
    }

    public float getFraudProbability() {
        return fraudProbability;
    }

    public void setFraudProbability(float fraudProbability) {
        this.fraudProbability = fraudProbability;
    }

    public String toString() {
        String header = "\nFRAUD DETECTION RESPONSE *****************\n";
        String predictedLabel = "PREDICTED LABEL = " + (this.predictedLabel == NO_FRAUD ? "NO FRAUD" : "FRAUD") + "\n";
        String noFraudProbability = "NO FRAUD probability = " + this.noFraudProbability + "\n";
        String fraudProbability = "FRAUD probability = " + this.fraudProbability + "\n";
        String executionTimeMs = "Model Inference Execution Time = " + this.executionTimeNanoseconds + " Nanoseconds" + "\n";
        return (header + predictedLabel + noFraudProbability + fraudProbability + executionTimeMs);
    }


}
