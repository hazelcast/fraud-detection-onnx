package datamodel;

public class Transaction {

    private Long cc_num;

    private String trans_num;

    private Float amt;

    private String merchant;

    private String transaction_date;

    private Float merch_lat;

    public Long getCc_num() {
        return cc_num;
    }

    public void setCc_num(Long cc_num) {
        this.cc_num = cc_num;
    }

    public String getTrans_num() {
        return trans_num;
    }

    public void setTrans_num(String trans_num) {
        this.trans_num = trans_num;
    }

    public Float getAmt() {
        return amt;
    }

    public void setAmt(Float amt) {
        this.amt = amt;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getTransaction_date() {
        return transaction_date;
    }

    public void setTransaction_date(String transaction_date) {
        this.transaction_date = transaction_date;
    }

    public Float getMerch_lat() {
        return merch_lat;
    }

    public void setMerch_lat(Float merch_lat) {
        this.merch_lat = merch_lat;
    }

    public Float getMerch_long() {
        return merch_long;
    }

    public void setMerch_long(Float merch_long) {
        this.merch_long = merch_long;
    }

    public Integer getIs_fraud() {
        return is_fraud;
    }

    public void setIs_fraud(Integer is_fraud) {
        this.is_fraud = is_fraud;
    }

    private Float merch_long;

    private Integer is_fraud;

    public Transaction() {}

    public Transaction(Long cc_num, String trans_num, Float amt, String merchant, String transaction_date, Float merch_lat, Float merch_long, Integer is_fraud) {
        this.cc_num = cc_num;
        this.trans_num = trans_num;
        this.amt = amt;
        this.merchant = merchant;
        this.transaction_date = transaction_date;
        this.merch_lat = merch_lat;
        this.merch_long = merch_long;
        this.is_fraud = is_fraud;
    }
}
