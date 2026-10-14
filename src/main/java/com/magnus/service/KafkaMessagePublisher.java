package com.magnus.service;

import com.magnus.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    public static final String TOPIC_NAME="myTopicPr8";

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMessageToTopic(String message){

       CompletableFuture<SendResult<String, Object>> future =

            kafkaTemplate.send(TOPIC_NAME, 1,null,message );


    future.whenComplete((result,exception)->{
         if(exception==null){
             System.out.println("Message Sent [ "+message +" ]"+ " on "+TOPIC_NAME+
             " With Offset=["+result.getRecordMetadata().offset()+"]"
                     );
         }
         else {
             System.out.println("Unable to send message [ "+message +" ]"+ " on "+TOPIC_NAME);
         }
     });

    }

    public void sendCustomerToTopic(Customer customer){

        CompletableFuture<SendResult<String, Object>> future =
              //  Random Partition selections
                     kafkaTemplate.send(TOPIC_NAME,2,null, customer);

                // specify Partition Number and key as null to send data in particular partition

                kafkaTemplate.send(TOPIC_NAME,1,null, customer);


        future.whenComplete((result,exception)->{
            if(exception==null){
                System.out.println("Message Sent [ "+customer +" ]"+ " on "+TOPIC_NAME+
                        " With Offset=["+result.getRecordMetadata().offset()+"]"
                );
            }
            else {
                System.out.println("Unable to send message [ "+customer +" ]"+ " on "+TOPIC_NAME);
            }
        });

    }

    public void sendCustomerToTopicUsingKey(Customer customer){

        CompletableFuture<SendResult<String, Object>> future =
                // Random Partition selections
                // kafkaTemplate.send(TOPIC_NAME,2,null, message);

                // specify Partition Number and key as null to send data in particular partition
                kafkaTemplate.send(TOPIC_NAME,0,"CREATED", customer);

        future.whenComplete((result,exception)->{
            if(exception==null){
                System.out.println("Message Sent [ "+customer +" ]"+ " on "+TOPIC_NAME+
                        " With Offset=["+result.getRecordMetadata().offset()+"]"
                );
            }
            else {
                System.out.println("Unable to send message [ "+customer +" ]"+ " on "+TOPIC_NAME);
            }
        });

    }

    public void sendCustomerToTopicManualCommit(Customer customer){

        CompletableFuture<SendResult<String, Object>> future =
                // Random Partition selections
                kafkaTemplate.send(TOPIC_NAME,customer);

        future.whenComplete((result,exception)->{
            if(exception==null){
                System.out.println("Message Sent [ "+customer +" ]"+ " on "+TOPIC_NAME+
                        " With Offset=["+result.getRecordMetadata().offset()+"]"
                );
            }
            else {
                System.out.println("Unable to send message [ "+customer +" ]"+ " on "+TOPIC_NAME);
            }
        });

    }

}
