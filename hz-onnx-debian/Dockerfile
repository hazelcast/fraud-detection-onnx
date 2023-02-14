FROM ubuntu:22.04

#runtime variables
ARG HZ_HOME="/opt/hazelcast"
#Workdir 
WORKDIR ${HZ_HOME}

USER root
RUN apt-get update -y
RUN apt-get install -y apt-transport-https
RUN apt-get install -y gpg-agent
RUN apt-get install -y wget
RUN apt update -y
RUN apt install -y bash-completion

#Install hazelcast 5.2.1
RUN wget -qO - https://repository.hazelcast.com/api/gpg/key/public | gpg --dearmor | tee /usr/share/keyrings/hazelcast-archive-keyring.gpg > /dev/null
RUN echo "deb [signed-by=/usr/share/keyrings/hazelcast-archive-keyring.gpg] https://repository.hazelcast.com/debian stable main" | tee -a /etc/apt/sources.list
RUN apt update -y && apt install -y hazelcast=5.2.1


#Example data folders
RUN mkdir /opt/hazelcast/age-group /opt/hazelcast/category \ 
    /opt/hazelcast/cc_num /opt/hazelcast/gender /opt/hazelcast/job \
    /opt/hazelcast/merchant /opt/hazelcast/setting /opt/hazelcast/transaction_hour \
    /opt/hazelcast/transaction_month /opt/hazelcast/transaction_weekday \
    /opt/hazelcast/zip /opt/hazelcast/transaction

#Example Customer and Merchant Feature and pre-processing data
COPY ./age-group/age_group_dict.json /opt/hazelcast/age-group/
COPY ./category/category_dict.json /opt/hazelcast/category/
COPY ./cc_num/cc_num_dict.json /opt/hazelcast/cc_num/
COPY ./gender/gender_dict.json /opt/hazelcast/gender/
COPY ./job/job_dict.json /opt/hazelcast/job/
COPY ./merchant/merchant_dict.json /opt/hazelcast/merchant/
COPY ./setting/setting_dict.json /opt/hazelcast/setting/
COPY ./transaction_hour/transaction_hour_dict.json /opt/hazelcast/transaction_hour/
COPY ./transaction_weekday/transaction_weekday_dict.json /opt/hazelcast/transaction_weekday/
COPY ./transaction_month/transaction_month_dict.json /opt/hazelcast/transaction_month/
COPY ./transaction_hour/transaction_hour_dict.json /opt/hazelcast/transaction_hour/
COPY ./transaction_weekday/transaction_weekday_dict.json /opt/hazelcast/transaction_weekday/
COPY ./zip/zip_dict.json /opt/hazelcast/zip/

#Customer and Merchant Feature data
COPY ./customer_data.csv /opt/hazelcast/
COPY ./merchant_data.csv /opt/hazelcast/

#Example Fraud Detection Onnx Model
COPY ./lightgbm_fraud_detection_onnx /opt/hazelcast/

#Example transaction data files
COPY ./100k-transactions.csv /opt/hazelcast/transaction/
COPY ./small_transactions.csv /opt/hazelcast/transaction/
COPY ./1k-transactions.csv /opt/hazelcast/transaction/
COPY ./10k-transactions.csv /opt/hazelcast/transaction/

#Place Onnx Libraries in Hazelcast's Classpath
COPY ./onnxruntime-1.13.1.jar /opt/hazelcast/onnxruntime-1.13.1.jar
COPY ./libonnxruntime.so /opt/hazelcast/lib/libonnxruntime.so
COPY ./libonnxruntime.so.1.13.1 /opt/hazelcast/lib/libonnxruntime.so.1.13.1
COPY ./libonnxruntime4j_jni.so /opt/hazelcast/lib/libonnxruntime4j_jni.so

#Hazelcast config file - with config for the Fraud Scoring Example
COPY ./hazelcast.yml /opt/hazelcast/
COPY ./hazelcast-aws.yml /opt/hazelcast/
COPY ./hazelcast-k8.yml /opt/hazelcast/

#place Merchant Portable Serialization classes to member classpath
COPY ./feature-data-loader-1.0-SNAPSHOT.jar /opt/hazelcast/

# Default Runtime variables
ENV HZ_HOME="${HZ_HOME}" \
    CLASSPATH_DEFAULT="${HZ_HOME}/*" \
    JAVA_OPTS_DEFAULT="-Donnxruntime.native.path=/opt/hazelcast/lib -Djava.net.preferIPv4Stack=true -XX:MaxRAMPercentage=80.0 -XX:MaxGCPauseMillis=5" \
    PROMETHEUS_PORT="" \
    PROMETHEUS_CONFIG="${HZ_HOME}/config/jmx_agent_config.yaml" \
    CLASSPATH="" \
    JAVA_OPTS="" \
    HAZELCAST_CONFIG="" \
    LANG=C.UTF-8 \
    PATH=${HZ_HOME}/bin:$PATH

# Expose port
EXPOSE 5701

# User hazelcast
USER hazelcast

CMD ["hz", "start"]