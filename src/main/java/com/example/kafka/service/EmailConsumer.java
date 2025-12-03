package com.example.kafka.service;

import com.example.kafka.infra.ExternalMailClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

@Service
public class EmailConsumer {

    private final ExternalMailClient mailClient;
    private final KafkaListenerEndpointRegistry registry;

    public EmailConsumer(ExternalMailClient mailClient, KafkaListenerEndpointRegistry registry) {
        this.mailClient = mailClient;
        this.registry = registry;
    }

    // 👇 id 지정 필수, 토픽 이름 Producer랑 똑같이!
    @KafkaListener(id = "my-listener-id", topics = "daily-email-job")
    public void consume(String email) {
        // 1. 들어오자마자 로그 찍기 (이게 안 찍히면 연결 문제)
        System.out.println("📨 수신: " + email);

        try {
            mailClient.sendEmail(email);
        } catch (HttpClientErrorException.TooManyRequests e) {
            logAndPause();
            throw e; // Kafka가 재시도하도록 예외 던짐
        } catch (Exception e) {
            System.err.println("❌ 에러 발생: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void logAndPause() {
        System.out.println("⛔ [Backpressure] 과부하 감지! 잠시 멈춥니다.");

        MessageListenerContainer container = registry.getListenerContainer("my-listener-id");

        if (container != null) {
            container.pause();
            new Thread(() -> {
                try {
                    Thread.sleep(5000);
                    container.resume();
                    System.out.println("🟢 [Backpressure] 다시 시작합니다.");
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }).start();
        } else {
            System.out.println("⚠️ 컨테이너를 찾을 수 없습니다!");
        }
    }
}