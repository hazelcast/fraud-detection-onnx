# Fraud Detection With Hazelcast and ONNX

In this demo, you will:
* Deploy a complete Fraud Detection Inference pipeline to Hazelcast featuring:
    * Use of Hazelcast's fast data store to hold:
        * Customer and Merchant feature data (e.g customer socio-economic attributes)
        * Feature engineering data needed to tranform features into numerical inputs for a Fraud Detection
            * The fraud detection model was trained with LightGBM framework and exported on ONNX format
    * Use of Hazelcast's stream processing and compute capability to:
        * process a stream of incoming credit card transactions
        * perform feature engineering to turn features into model inputs
        * calculate real-time features (e.g. distance from home, time of day, day of week)
        * run the fraud detection model in Hazelcast 

# Create a Kafka Cluster & Topic in With Confluent Cloud
You will use Kafka as source of credit card transactions coming into your fraud detection inference pipeline.

* A simple way to get Kafka running is to [Create a Kafka Cluster in Confluent Cloud](
https://docs.confluent.io/cloud/current/get-started/index.html#quick-start-for-ccloud)

    * Capture your Kafka Cluster Endpoint information
    [Endpoint screenshot](./images/kafka-endpoint.png)

* Once you create your Kakfa cluster, [create a 'Transactions' topic](https://docs.confluent.io/cloud/current/get-started/index.html#step-2-create-a-ak-topic) within it. \
Make sure you name your Topic "Transactions"

* Obtain API Key & Secret to connect to your cluster. You will need these parameters to connect Hazelcast to Kafka later on



# Deploy a Hazelcast-Onnx Container AWS ECS
At present, ONNX runtime is not supported in ARM/Java/Linux 

We've created a docker image preloaded with:
* Hazelcast 5.2.1 running on Ubuntu 22.04
* ONNX runtime libraries in a supported platform/OS/Programming language (AMD64/Linux/Java)
* Some sample Transaction data (in csv files)

## Deploy Hazelcast-Onnx image to AWS ECS with Docker Compose
* Make sure you have `aws cli` configured to work with your AWS account.
* Make sure your AWS user has [IAM permissions listed here](https://docs.docker.com/cloud/ecs-integration/#run-an-application-on-ecs)
* You have Docker Compose installed on your system

Create a docker "Context" to instruct docker compose to deploy to AWS ECS
```
docker context create myecscontext
docker context use myecscontext
```
You can now deploy with
```
docker compose up
```
This will take 5-10 minutes.

Once the process completes, you can check the hazelcast-onnx server name & port by running

```
docker compose ps
```

# Load some transactions into your Kafka Cluster

You will use hz-cli, a Hazelcast CLI, to submit a data loading job that will upload one of the CSV files already present on the `hazelcast-onnx` container into the "Transactions" topic in your Kafka Cluster.

```
cd transaction-loader 
```

followed by a command like this

```
hz-cli submit -v -t [hazelcast-ip:port] -c org.example.Main target/transaction-loader-1.0-SNAPSHOT.jar [pathtoyourtransactionsonServer] [transactions.csv filename] [kafka cluster:port] [hazelcast-ip:port]
```
For example, assuming
* Kafka cluster IP : port = pkc-ymrq7.us-east-2.aws.confluent.cloud:9092
* Hazelcast IP: port = ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701

You could run the following command to load 100k transactions into Kafka
```
hz-cli submit -v -t ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701 -c org.example.Main target/transaction-loader-1.0-SNAPSHOT.jar /opt/hazelcast/transaction 100k-transactions.csv pkc-ymrq7.us-east-2.aws.confluent.cloud:9092 ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701
```

After a few seconds, you should see a "Transaction Loader Job" success message in the output


# Load Customer and Merchant Feature Data into Hazelcast
You will use hz-cli to submit a series of feature data loading jobs and the inference pipeline into Hazelcast.\

These jobs will simply load the Merchant & customer data from JSON and CSV files into [Hazelcast Maps](https://docs.hazelcast.com/hazelcast/5.2/data-structures/map) 

Once the Customer and Merchant Feature data is loaded, the fraud inference pipeline will also be deployed. This will start running the incoming transactions from Kafka through the Fraud Detection model 

First, go into the feature-data-loader folder
``
cd ../feature-data-loader
```
You will need a command that looks like this 
```
hz-cli submit -v -t [hazelcast:port] -c org.example.client.LoadOnlineFeatures target/feature-data-loader-1.0-SNAPSHOT.jar [hazelcast:port]
```

For example, assuming
* Hazelcast IP: port = ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701 
* Kafka cluster IP : port = pkc-ymrq7.us-east-2.aws.confluent.cloud:9092
* Onnx model = lightgbm_fraud_detection_onnx (preloaded into the container!)

Your command would look like this:
```
hz-cli submit -v -t ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701 -c org.example.client.DeployFraudDetectionInference target/feature-data-loader-1.0-SNAPSHOT.jar ecsde-LoadB-1H1HHMC3E8E6F-7b7a701d547d1f06.elb.us-east-2.amazonaws.com:5701 pkc-ymrq7.us-east-2.aws.confluent.cloud:9092 lightgbm_fraud_detection_onnx
``

After a few seconds, you should see an output similar to

![Feature Loader Success Message](./images/feature-loader-msg.png)

# The Fraud Detection Inference Pipeline 

At a high-level, the Fraud Detection inference pipeline executes the following steps:
* Take a transaction from Kafka (with minimal information such as Credit Card Number, Merchant, Amount, Transaction date and Geolocation )
* Enrich this transaction with with Customer and Merchant Features (e.g. customer's socio-demographic data, historical purchases etc, average spent, spent in last month, etc)
* Calculate real-time features such as "distance from home" (distance from transaction Geolocation and the Customer's billing address).
* Perform Feature engineering required to convert Customer and Merchant features into numeric values required by the Fraud Detection model
* Run the Fraud Detection model to obtain a fraud probability

Finally, and for the purposes of this demo, the pipeline perform two actions:

* All potentially fraudulent transactions, (e.g. filter those with fraud probability higher than 0.5), are logged to the console
* All predictions and incoming transactions are saved in memory (as IMaps) 24 hours for further analysis

## Production Ideas
In a real-world scenario, the end of the inference pipeline could become the starting point to other pipelines. For example, you could create pipelines to:

* Trigger automatic customer validation request for potentially fraudulent transactions
* Alert data scientist about model/data drift (e.g. determine if model re-training is needed)
* Send automatic merchant notifications for "fraudulent transactions"
* Update Customer Features such as "last known coordinates" or "number/value of transactions attempted in the last X minutes"


# Check Logs to see Potentially Fraudulent transasctions flagged by the model
you should see some of these potential fraud cases by inspecting the logs. \

```
docker compose logs hazelcast-onnx
```

You should see "potential fraud cases" as shown in the image below\

You can see how the original transaction has been processed by the Fraud Detection pipeline

![Potential Fraud Cases image](./images/potential-fraud-case.png)

# Monitoring in Hazelcast
You can start Hazelcast Management Center by navigating to localhost:8080

Create a connection to `YOUR IP ADDRESS`:5701

Find details of the Fraud Detection Inference Pipeline\
![Management Center showing Fraud Detection Inference Job](./images/mc.png)

In this case, please note that Total Out / Last Minute Out refer to potential fraud cases only!

# Monitoring in Grafana

WIP

# Stop all Containers

```
docker compose down
```

You may also want to return Docker to your "default" context

```
docker context use default
```

## (Optional) Train the model and convert it to ONNX

To-Do


## (Optional) Building Your own Hazelcast-Onnx image
If you wanted to create your own hazelcast-onnx image and preload it with your data and model, you can check the `Dockerfile` in the `hz-onnx-debian` folder for inspiration

```
docker-compose -f build-hz-onnx-image.yml build
docker tag fraud-detection-onnx-hazelcast-onnx-debian <github-username>/<image-name>
docker push <github-username>/<image-name> 
```
