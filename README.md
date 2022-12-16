# Fraud Detection With Hazelcast and ONNX

In this demo, you will:
* Train a Fraud Detection model using LightGBM and Python 3.8  
    * Convert this model to ONNX
* Deploy a complete Fraud Detection Inference pipeline to Hazelcast featuring:
    * Use of Hazelcast's low-latency data store to hold:
        * Customer and Merchant feature data
        * Feature engineering data needed to produce inputs for the Fraud Detection model
    * Use of Hazelcast's stream processing capability to:
        * process incoming stream of transactions
        * perform feature engineering to turn features into model inputs
        * calculate real-time features (e.g. distance from home, time of day, day of week)
        * run the Fraud Detection Onnx Model inside Hazelcast

# Model Training

To-Do


# Start Kakfa and Hazelcast containers
```
docker-compose up -d
```

# Load 2.8M Transactions into Kafka
```
cd transaction-loader 
hz-cli submit -v -t localhost:5701 -c org.example.Main target/transaction-loader-1.0-SNAPSHOT.jar \
    $(pwd) transaction_data_stream.csv localhost:9092
```

After a few seconds, you should see a "Transaction Loader Job" success message in the output

![Transaction Loading Job Success Message](./images/transaction-loader-msg.png)

# Load Customer and Merchant Feature Data into Hazelcast
You will use hz-cli, a Hazelcast client command tool, to submit a series of feature data loading jobs.
These jobs will simply load the Merchant & customer data from JSON and CSV files into [Hazelcast Maps](https://docs.hazelcast.com/hazelcast/5.2/data-structures/map) 

Before you start, You will need to find your IP address.
For MacOS, run this command
```
ifconfig | grep "inet " | grep -Fv 127.0.0.1 | awk '{print $2}' 
```
The output should be your IP address.
I will use 192.168.0.135 as an example

With your IP address, you can submit the Feature data loading jobs. 
But first, you need to navigate to the feature-data-loader directory
```
cd ../feature-data-loader
```
and run the following command (replacing 192.168.0.135 with your own IP address)
```
hz-cli submit -v -t 192.168.0.135:5701 -c org.example.Main target/feature-data-loader-1.0-SNAPSHOT.jar 192.168.0.135:5701
```
After a few seconds, you should see an output similar to

![Feature Loader Success Message](./images/feature-loader-msg.png)

# Submit Model Inference Pipeline to Hazelcast

WIP

# Basic Model Monitoring in Grafana

WIP

## (Optional) Building Your own Hazelcast-Onnx image
```
docker-compose -f build-hz-onnx-image.yml build
docker tag fraud-detection-onnx-hazelcast-onnx-debian <github-username>/<image-name>
docker push <github-username>/<image-name> 
```
