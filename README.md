# fraud-detection-onnx


# Start Kakfa and Hazelcast containers
```
docker-compose up -d
```

# Load 2.8M Transactions into a Kafka Topic (Transactions)
```
cd transaction-loader 
hz-cli submit -v -t localhost:5701 -c org.example.Main target/transaction-loader-1.0-SNAPSHOT.jar \
    $(pwd) transaction_data_stream.csv localhost:9092
```



# (Optional) Building Your own Hazelcast-Onnx image
```
docker-compose -f build-hz-onnx-image.yml build
docker tag fraud-detection-onnx-hazelcast-onnx-debian <github-username>/<image-name>
docker push <github-username>/<image-name> 
```
