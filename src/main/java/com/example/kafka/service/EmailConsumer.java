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
    private final KafkaListenerEndpointRegistry registry; // 컨슈머 ON/OFF 스위치

    public EmailConsumer(ExternalMailClient mailClient, KafkaListenerEndpointRegistry registry) {
        this.mailClient = mailClient;
        this.registry = registry;
    }

    // id는 나중에 레지스트리에서 찾을 때 씁니다.
    @KafkaListener(id = "email-listener", topics = "email-send-tasks")
    public void consume(String email) {
        try {
            // Mock 서버로 전송 시도
            mailClient.sendEmail(email);
            // System.out.println("✅ 전송 성공: " + email); // 로그 너무 많으면 주석

        } catch (HttpClientErrorException.TooManyRequests e) {
            logAndPause();
            // 현재 실패한 메시지는 다시 처리해야 하므로 예외를 던져야 함 (여기선 단순화)
            throw e;
        } catch (Exception e) {
            // 그 외 에러 처리
        }
    }

    private void logAndPause() {
        System.out.println("⛔ [Backpressure] 외부 서버 과부하! 컨슈머를 5초간 정지합니다.");

        MessageListenerContainer container = registry.getListenerContainer("email-listener");
        if (container != null) {
            container.pause(); // ⏸️ 일시 정지

            // 5초 뒤에 다시 켜는 스레드 실행
            new Thread(() -> {
                try {
                    Thread.sleep(5000);
                    container.resume(); // ▶️ 재개
                    System.out.println("🟢 [Backpressure] 컨슈머를 다시 가동합니다.");
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }).start();
        }
    }
}