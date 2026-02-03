package oracleimport.oraclejdbc.listener.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import oracleimport.oraclejdbc.message.NotificationService;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobCompletionNotificationListener implements JobExecutionListener {

    private final NotificationService notificationService;

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        String status = jobExecution.getStatus().toString();

        // 상세 통계 계산
        long totalRead = jobExecution.getStepExecutions().stream()
                .mapToLong(StepExecution::getReadCount)
                .sum();

        String message = String.format("""
                        🚀 *Job 종료 알림*
                        - Job Name: %s
                        - CategoryId: 82
                        - Status: %s
                        - Total Read: %d""",
                jobName, status, totalRead);

        // 비동기 서비스 호출
        notificationService.sendNotification(message);
    }
}
