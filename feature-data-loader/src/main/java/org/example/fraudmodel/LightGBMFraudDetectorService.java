package org.example.fraudmodel;

import ai.onnxruntime.*;


import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LightGBMFraudDetectorService implements FraudDetectionService {

    private static final long NO_FRAUD = 0;
    private static final long FRAUD = 1;
    private static OrtEnvironment env;
    private static OrtSession session;

    public LightGBMFraudDetectorService(String onnxModelFileName) throws OrtException {
        //Load model from resource.filename
        //URL fullPath = LightGBMFraudDetectorService.class.getClassLoader().getResource(onnxModelFileName);

        if (onnxModelFileName!=null) {
            String model = onnxModelFileName;

            if (model != null) {
                //create onnx environment and session
                if (env == null) {
                    env = OrtEnvironment.getEnvironment();
                }
                if (session == null) {
                    //create onnx session with Basic Optimization options
                    OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
                    opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
                    session = env.createSession(model, opts);
                }
            }
        }
    }

    @Override
    public FraudDetectionResponse getFraudProbability(FraudDetectionRequest r) throws RuntimeException {

        long start = System.nanoTime();

        FraudDetectionResponse response;


        // transform input data to OnnxTensor
        float [][]  inputData = new float [1][15];
        inputData[0]= toFloatArray(r);

        try {
            //load request data into OnnxTensor
            OnnxTensor inputTensor = OnnxTensor.createTensor(LightGBMFraudDetectorService.env, inputData);
            //Perform model inference passing inputTensor
            OrtSession.Result output = session.run(Collections.singletonMap("input", inputTensor));

            //Unpack prediction results from the 2 model outputs: "output_label" & "output_probabilities"
            OnnxValue onnxPredictedLabels= output.get(0); //0 corresponds to "output_label"
            OnnxValue onnxPredictedProbas= output.get(1); //1 corresponds to "output_probabilities"

            //The first element in output_label is either a 0 (No Fraud) or 1 (Fraud)
            long[] predictedLabels = (long[]) onnxPredictedLabels.getValue();


            // output_probabilities is a list of OnnxMaps.
            List<OnnxMap> predictedProbaMaps = (List<OnnxMap>) onnxPredictedProbas.getValue();
            // take the first OnnxMap. Each map is a  <Label, Probability>
            OnnxMap predictedProbas = predictedProbaMaps.get(0);
            Map<Long,Float> probaMap = (Map<Long,Float>)predictedProbas.getValue();
            float noFraudProbability = probaMap.get(NO_FRAUD);
            float fraudProbability = probaMap.get(FRAUD);
            long end = System.nanoTime();
            long totalInferenceTime = end-start;

            response = new FraudDetectionResponse(predictedLabels[0],noFraudProbability,fraudProbability,totalInferenceTime);


        } catch (OrtException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private float[] toFloatArray(FraudDetectionRequest r){
        float [] result = new float[15];
        result[0]= (float) r.getCc_num() ;
        result[1]= (float) r.getCategory();
        result[2]= r.getAmt();
        result[3]= (float) r.getMerchant();
        result[4]= (float) r.getTransaction_weekday();
        result[5]= (float) r.getTransaction_month();
        result[6]= (float) r.getTransaction_hour();
        result[7]= (float) r.getGender();
        result[8]= (float) r.getZip();
        result[9]= r.getCity_pop();
        result[10]= (float) r.getJob();
        result[11]= (float) r.getAge();
        result[12]= (float) r.getSetting();
        result[13]= (float) r.getAge_group();
        result[14]= r.getDistance_from_home();

        return result;

    }
}
