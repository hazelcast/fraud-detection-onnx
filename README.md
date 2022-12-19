# Fraud Detection With Hazelcast and ONNX

In this demo, you will:
* Train a Fraud Detection model using LightGBM and Python 3.8  
    * Export this model to ONNX so that it can be executed later inside Hazelcast (Java)
* Deploy a complete Fraud Detection Inference pipeline to Hazelcast featuring:
    * Use of Hazelcast's low-latency data store to hold:
        * Customer and Merchant feature data
        * Feature engineering data needed to produce inputs for the Fraud Detection model
    * Use of Hazelcast's stream processing and compute capability to:
        * process a stream of incoming credit card transactions
        * perform feature engineering to turn features into model inputs
        * calculate real-time features (e.g. distance from home, time of day, day of week)
        * run the Fraud Detection Onnx Model inside Hazelcast

# Model Training

To-Do


# Start Kakfa, Hazelcast & Grana Containers
```
docker-compose up -d
```
# Create a "Transactions" Kafka Topic
The "Transactions" topic will have 271 partitions (to match Hazelcast's 271 partitions)
```
docker exec -it broker sh /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic Transactions --replication-factor 1 --partitions 271
```

# Load 2.8M Transactions to a Kafka Topic
Hazelcast can be deployed in two modes: Client/Server and embedded within an application.\
This transaction loader is a good example of running Hazelcast in embedded mode
```
cd transaction-loader 
hz-cli submit -v -t 192.168.0.135:5702 -c org.example.Main target/transaction-loader-1.0-SNAPSHOT.jar $(pwd) transaction_data_stream.csv localhost:9092
cd ..
```
This transaction loader is a good example of running Hazelcast in embedded mode\
Check the source code for details\

After a few seconds, you should see a "Transaction Loader Job" success message in the output

![Transaction Loading Job Success Message](./images/transaction-loader-msg.png)

# Load Customer and Merchant Feature Data into Hazelcast
You will use hz-cli, a Hazelcast client command tool, to submit a series of feature data loading jobs.\
These jobs will simply load the Merchant & customer data from JSON and CSV files into [Hazelcast Maps](https://docs.hazelcast.com/hazelcast/5.2/data-structures/map) 

Before you start, You will need to find your IP address.
For MacOS, run this command
```
ifconfig | grep "inet " | grep -Fv 127.0.0.1 | awk '{print $2}' 
```
The output should be your IP address.
I will use 192.168.0.135 as an example

With your IP address, you can submit the Feature data loading jobs. 
```
cd feature-data-loader
hz-cli submit -v -t 192.168.0.135:5701 -c org.example.client.LoadOnlineFeatures target/feature-data-loader-1.0-SNAPSHOT.jar 192.168.0.135:5701
cd ..
```
After a few seconds, you should see an output similar to

![Feature Loader Success Message](./images/feature-loader-msg.png)

# Start Processing Credit Card Transactions in Hazelcast
You are now ready to deploy your Fraud Detection model as part of a transaction processing pipeline\
At a high-level, the pipeline executes the following steps:
* Take a transaction from Kafka (with minimal information such as Credit Card Number, Merchant, Amount, Transaction date and Geolocation )
* Enrich this transaction with with Customer and Merchant Features (e.g. customer's socio-demographic data, historical purchases at this merchant etc)
* Calculate real-time features such as "distance from home" (distance from transaction Geolocation and the Customer's billing address).
* Perform Feature engineering required to convert Customer and Merchant features into numeric values required by the Fraud Detection model
* Run the Fraud Detection model
* Focus on "Potential Fraud" cases (e.g. filter those with fraud probability higher than 0.5)
* Log these potential fraud cases for further analysis


You can deploy the Fraud Detection pipeline by running:

```
cd feature-data-loader
hz-cli submit -v -t 192.168.0.135:5701 -c org.example.client.DeployFraudDetectionInference target/feature-data-loader-1.0-SNAPSHOT.jar 192.168.0.135:5701 broker:29092 lightgbm_fraud_detection_onnx
cd ..
```

If you check the logs for the hazelcast-onnx container, you should see some of these potential fraud cases
```
docker logs hazelcast-onnx
```
You should see "potential fraud cases" as shown in the image below\

You can see how the original transaction has been processed by the Fraud Detection pipeline

![Potential Fraud Cases image](./images/potential-fraud-case.png)



# QUESTION TIME:question:
* Can you suggest reasons the model might be flagging the above transaction as potentially fraudulent?
* Did you notice the model inference time? 
    * Is that good? (0.4ms) 
    * Suggest ways to improve it!


# Monitoring in Hazelcast
You can start Hazelcast Management Center by navigating to localhost:8080\

Create a connection to <YOUR IP ADDRESS>:5701

Find details of the Fraud Detection Inference Pipeline\
![Management Center showing Fraud Detection Inference Job](./images/mc.png)



# Monitoring in Grafana

WIP

# Stop all Containers

```
docker-compose down
```


## (Optional) Building Your own Hazelcast-Onnx image
```
docker-compose -f build-hz-onnx-image.yml build
docker tag fraud-detection-onnx-hazelcast-onnx-debian <github-username>/<image-name>
docker push <github-username>/<image-name> 
```
