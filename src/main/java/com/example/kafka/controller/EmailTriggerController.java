package com.example.kafka.controller;

import com.example.kafka.service.EmailProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailTriggerController {

    private final EmailProducer emailProducer;

    public EmailTriggerController(EmailProducer emailProducer) {
        this.emailProducer = emailProducer;
    }

    @PostMapping("/api/start")
    public String startBatch() {
        emailProducer.sendBulkEmails();
        return "배치 작업 시작됨! 로그를 확인하세요.";
    }
}