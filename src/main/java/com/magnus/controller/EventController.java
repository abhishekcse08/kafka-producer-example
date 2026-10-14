package com.magnus.controller;


import com.magnus.dto.Customer;
import com.magnus.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message){
        try {
            for(int i=0;i<100000;i++) {
                kafkaMessagePublisher.sendMessageToTopic(message + "Message count is "+ i);
            }

            return ResponseEntity.ok("Message Published Successfully");
        }
        catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }

    @PostMapping("/publish/{message}")
    public ResponseEntity<?> publishMessageOnPartition(@PathVariable String message){
        try {
            for(int i=0;i<1000;i++) {
                kafkaMessagePublisher.sendMessageToTopic(message + "Message count is "+ i);
            }

            return ResponseEntity.ok("Message Published Successfully");
        }
        catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }

    @PostMapping("/publish")
    public ResponseEntity<?> publishCustomerData(@RequestBody Customer customer){
        try {

           // kafkaMessagePublisher.sendCustomerToTopic(customer);
            kafkaMessagePublisher.sendCustomerToTopicUsingKey(customer);
            //Manual Commit
           // kafkaMessagePublisher.sendCustomerToTopicManualCommit(customer);


            return ResponseEntity.ok("Message Published Successfully");
        }
        catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }
}
