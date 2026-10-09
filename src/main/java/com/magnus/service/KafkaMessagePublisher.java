package com.magnus.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    public static final String TOPIC_NAME="myTopicPr4";

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMessageToTopic(String message){

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(TOPIC_NAME, message);

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
}
