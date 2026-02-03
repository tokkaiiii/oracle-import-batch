package oracleimport.oraclejdbc.scheduler;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@EnableScheduling
public class AppScheduler {

    private final JobLauncher jobLauncher;
    private final Job oracleImportJob;

    @Scheduled(cron = "0 0 1 * * *")
    public void run() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("lastRunTime", LocalDateTime.now().minusDays(1)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(oracleImportJob, params);
    }
}
