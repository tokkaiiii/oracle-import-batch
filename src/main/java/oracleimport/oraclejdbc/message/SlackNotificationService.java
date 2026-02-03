package oracleimport.oraclejdbc.message;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Slf4j
@Service
@RequiredArgsConstructor
public class SlackNotificationService implements NotificationService {

    private final RestClient restClient = RestClient.builder()
            .baseUrl("https://hooks.slack.com")
            .messageConverters(converters -> converters.add(new MappingJackson2HttpMessageConverter()))
            .build();

    private final String SLACK_WEBHOOK_PATH = "";

    @Async("slackTaskExecutor")
    @Override
    public void sendNotification(String message) {
        try {
            restClient.post()
                    .uri(SLACK_WEBHOOK_PATH)
                    .contentType(APPLICATION_JSON)
                    .body(Map.of("text", message))
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (request, response) -> {
                        log.error("슬랙 알림 전송 실패: HTTP {}", response.getStatusCode());
                    })
                    .toBodilessEntity(); // 응답 본문이 필요 없을 때 사용

            log.info("슬랙 비동기 알림 전송 완료");
        } catch (Exception e) {
            log.error("슬랙 알림 전송 중 예외 발생: {}", e.getMessage());
        }
    }
}
