# fraud-detection-onnx
Work in progress

# Build the Hazelcast-Onnx image
```
docker-compose -f build-hz-onnx-image.yml build
docker tag fraud-detection-onnx-hazelcast-onnx-debian <github-username>/<image-name>
docker push <github-username>/<image-name> 
```

# Run a local container with the Hazelcat-Onnx image
```
docker-compose -f run-hz-onnx-container.yml up -d
```

# Stop local container with the Hazelcast-Onnx image
```
docker-compose -f run-hz-onnx-container.yml down
```
