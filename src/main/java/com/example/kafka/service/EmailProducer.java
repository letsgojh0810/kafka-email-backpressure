package com.example.kafka.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EmailProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public EmailProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendBulkEmails() {
        for (int i = 1; i <= 10000; i++) {
            // 👇 토픽 이름 변경!
            kafkaTemplate.send("daily-email-job", "user_" + i + "@example.com");
        }
        System.out.println("🚀 10,000건의 이메일 작업이 Kafka에 등록되었습니다.");
    }
}