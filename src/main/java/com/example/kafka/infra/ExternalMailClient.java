package com.example.kafka.infra;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
public class ExternalMailClient {

    private final RestTemplate restTemplate = new RestTemplate();

    // WireMock 주소
    private final String MOCK_URL = "http://localhost:8089/email";

    // ⭐ 핵심: 이 메서드에 서킷 브레이커를 장착한다!
    // 에러가 많이 나면 알아서 차단(Open) 되고, fallbackMethod가 실행됨
    @CircuitBreaker(name = "smtpBreaker", fallbackMethod = "fallback")
    public void sendEmail(String emailData) {
        restTemplate.postForEntity(MOCK_URL, emailData, String.class);
    }

    // 차단되었을 때(서킷 Open) 실행될 메서드
    public void fallback(String emailData, CallNotPermittedException e) {
        // 여기가 진짜 Backpressure!
        // 서킷이 열려있으면 아예 요청을 안 보내고 여기서 로그만 찍고 끝냄 (자원 절약)
        System.out.println("⛔ [Circuit Open] 서킷이 열렸습니다. 요청을 보내지 않고 대기합니다: " + emailData);

        // 실무라면 여기서 DLQ(Dead Letter Queue)로 보내거나,
        // Kafka Consumer에게 "잠깐 쉬어"라고 신호를 보낼 수도 있음.
        // 여기서는 예외를 던져서 Kafka가 나중에 다시 시도하게 유도
        throw e;
    }

    // 진짜 429 에러가 났을 때 처리
    public void fallback(String emailData, HttpClientErrorException.TooManyRequests e) {
        System.out.println("❌ [429 Error] 과부하 발생! 서킷 실패율 집계 중...");
        throw e; // 예외를 던져서 서킷 브레이커가 "아 실패했구나"라고 알게 함
    }
}