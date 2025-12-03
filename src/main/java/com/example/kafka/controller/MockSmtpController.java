package com.example.kafka.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/mock/smtp")
public class MockSmtpController {

    private static final int MAX_REQUESTS_PER_SECOND = 5; // 제한 속도
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicLong lastResetTime = new AtomicLong(System.currentTimeMillis());

    @PostMapping("/send")
    public ResponseEntity<String> sendEmail() throws InterruptedException {
        // 네트워크 지연 흉내 (10~50ms)
        Thread.sleep(1);

        long currentTime = System.currentTimeMillis();

        // 1초 지났으면 카운트 리셋
        synchronized (this) {
            if (currentTime - lastResetTime.get() > 1000) {
                requestCount.set(0);
                lastResetTime.set(currentTime);
            }
        }

        if (requestCount.incrementAndGet() > MAX_REQUESTS_PER_SECOND) {
            System.out.println("❌ [Mock Server] 429 Error! 속도 위반!");
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Too fast!");
        }

        return ResponseEntity.ok("Sent");
    }
}