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

    @KafkaListener(id = "my-listener-id", topics = "daily-email-job")
    public void consume(String email) {
        // System.out.println("📨 수신함: " + email);

        try {
            mailClient.sendEmail(email);
        } catch (HttpClientErrorException.TooManyRequests e) {
            // 이건 진짜 429 에러 (아직 서킷 열리기 전)
            System.out.println("⚠️ [429] 너무 빨라요! 서킷 집계 중...");
            throw e;
        } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
            // 👇 [New] 서킷이 열려서 차단된 경우 (스택 트레이스 없이 깔끔하게!)
            System.out.println("⛔ [Circuit Open] 서킷 가동 중... 잠시 대기합니다.");

            // 여기서 잠깐 쉬어주면 로그가 너무 빨리 올라가는 걸 막을 수 있음
            try { Thread.sleep(1000); } catch (InterruptedException ig) {}

            throw e; // Kafka에게 "나중에 다시 할게"라고 알려줌
        } catch (Exception e) {
            System.err.println("❌ 알 수 없는 에러: " + e.getMessage());
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