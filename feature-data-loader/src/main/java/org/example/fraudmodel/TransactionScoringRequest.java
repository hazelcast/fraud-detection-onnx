package org.example.fraudmodel;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TransactionScoringRequest implements Serializable {
    private long creditCardNumber;
    private float amount;

    private String merchant;
    private boolean isFraud;
    private LocalDateTime transactionDate;

    public double getMerchantLatitude() {
        return merchantLatitude;
    }

    public void setMerchantLatitude(double merchantLatitude) {
        this.merchantLatitude = merchantLatitude;
    }

    public double getMerchantLongitude() {
        return merchantLongitude;
    }

    public void setMerchantLongitude(double merchantLongitude) {
        this.merchantLongitude = merchantLongitude;
    }

    private double merchantLatitude;
    private double merchantLongitude;
    private String transactionId;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public TransactionScoringRequest(long creditCardNumber, float amount, String merchant, boolean isFraud, LocalDateTime transactionDate, double merchantLatitude, double merchantLongitude,String transactionId) {
        this.creditCardNumber = creditCardNumber;
        this.amount = amount;
        this.merchant = merchant;
        this.isFraud = isFraud;
        this.transactionDate = transactionDate;
        this.merchantLatitude = merchantLatitude;
        this.merchantLongitude = merchantLongitude;
        this.transactionId = transactionId;
    }



    public LocalDateTime getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(LocalDateTime transactionDate) {
        this.transactionDate = transactionDate;
    }

    public long getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(long creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public boolean isFraud() {
        return isFraud;
    }

    public void setFraud(boolean fraud) {
        isFraud = fraud;
    }

    public long getTransactionDateToMillis() {
        return (this.transactionDate.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Override
    public String toString() {
        String header = "\n ********INCOMING TRANSACTION SCORING REQUEST*********" + "\n";
        String creditCardNumberString = "Credit Card Number " + String.valueOf(this.creditCardNumber) + "\n";
        String amountString = "Amount = " + String.valueOf(this.amount) + "\n";
        String dateString = "Transaction Date " + this.transactionDate.toString() + "\n";

        return header + creditCardNumberString + amountString + dateString;
    }
}
