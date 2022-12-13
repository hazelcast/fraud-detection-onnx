package hazelcast.solutions.fraud;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.time.LocalDateTime;

public class AuthRequestSource
{
    public static void main( String[] args )
    {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        Runtime.getRuntime().addShutdownHook(new Thread(hz::shutdown));

        IMap<Long, TransactionScoringRequest> transactionMap = hz.getMap("transactionScoringRequestMap");

        LocalDateTime transactionTime = LocalDateTime.parse("2022-09-16T18:35:45");
        TransactionScoringRequest template = new TransactionScoringRequest(3517182278248964L,
                (float) 327.63, "fraud_Erdman-Schaden", false, transactionTime,
                34.67023, -84.634,"2de6ae16cd114c4d4a7f31f4716b9a07");

        while(! Thread.currentThread().isInterrupted()){
            template.setCreditCardNumber(template.getCreditCardNumber() + 1);
            transactionMap.put(template.getCreditCardNumber(), template);
            try{
                Thread.sleep(499);
            } catch(InterruptedException x){
              Thread.currentThread().interrupt();
            }
        }

    }
}
