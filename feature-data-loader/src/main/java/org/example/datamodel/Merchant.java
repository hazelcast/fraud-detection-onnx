package org.example.datamodel;



public class Merchant  {
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

}
