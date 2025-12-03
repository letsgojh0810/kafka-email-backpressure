package com.example.kafka.infra;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ExternalMailClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String MOCK_URL = "http://localhost:8081/mock/smtp/send";

    public void sendEmail(String emailData) {
        // 여기서 429 에러가 나면 예외가 발생해서 Consumer로 전파됩니다.
        restTemplate.postForEntity(MOCK_URL, emailData, String.class);
    }
}