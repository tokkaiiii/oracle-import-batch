package oracleimport.oraclejdbc.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import oracleimport.oraclejdbc.dto.CategoryDto;
import oracleimport.oraclejdbc.dto.FormDataDto;
import oracleimport.oraclejdbc.dto.FormDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Base64;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OracleImportConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    @Bean
    public Job oracleImportJob(
            Step downloadFilesStep,
            Step importCategoryStep,
            Step importFormStep,
            Step importFormDataStep,
            JobExecutionListener jobCompletionNotificationListener
    ) {
        return new JobBuilder("oracleImportJob", jobRepository)
                .listener(jobCompletionNotificationListener)
                .start(downloadFilesStep)
                .next(importCategoryStep)
                .next(importFormStep)
                .next(importFormDataStep)
                .build();
    }

    @Bean
    public Step downloadFilesStep() {
        return new StepBuilder("downloadFilesStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    String[] files = {"file01.csv", "file02.csv", "file02.csv"};
                    String mssqlUrl = "";

                    for (String f : files) {
                        URL url = URI.create(mssqlUrl + f).toURL();
                        File target = new File("temp/" + f);
                        target.getParentFile().mkdirs();
                        Files.copy(url.openStream(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    }
                    return RepeatStatus.FINISHED;
                }, transactionManager).build();
    }

    // --- STEP 2: CLIP_FORM_CATEGORY 적재 ---
    @Bean
    public Step importCategoryStep() {
        return new StepBuilder("importCategoryStep", jobRepository)
                .<CategoryDto, CategoryDto>chunk(500, transactionManager)
                .reader(commonFileReader("temp/file01.csv",
                        CategoryDto.class,
                        "필드명"))
                .writer(categoryWriter())
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<CategoryDto> categoryWriter() {
        return new JdbcBatchItemWriterBuilder<CategoryDto>()
                .dataSource(dataSource)
                .sql("""
                        merge query
                        """)
                .beanMapped()
                .build();
    }

    // --- STEP 3: CLIP_FORM 적재 ---
    @Bean
    public Step importFormStep() {
        return new StepBuilder("importFormStep", jobRepository)
                .<FormDto, FormDto>chunk(500, transactionManager)
                .reader(commonFileReader("temp/file02.csv",
                        FormDto.class,
                        "필드명"))
                .writer(formWriter())
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<FormDto> formWriter() {
        return new JdbcBatchItemWriterBuilder<FormDto>()
                .dataSource(dataSource)
                .sql("""
                        merge query
                        """)
                .beanMapped()
                .build();
    }

    // --- STEP 4: CLIP_FORM_DATA 적재 (BLOB 처리) ---
    @Bean
    public Step importFormDataStep() {
        return new StepBuilder("importFormDataStep", jobRepository)
                .<FormDataDto, FormDataDto>chunk(20, transactionManager)
                .reader(commonFileReader("temp/file01.csv", FormDataDto.class,
                        "필드명"))
                .processor(item -> {
                    if (item.getBase64FormData() != null) {
                        try {
                            byte[] base64Decoded = Base64.getDecoder().decode(item.getBase64FormData());

                            String decodedXml = new String(base64Decoded, "UTF-16LE");

                            if (decodedXml.contains("encoding=\"UTF-8\"?+")) {
                            }

                            item.setFormData(decodedXml.getBytes("UTF-8"));

                        } catch (Exception e) {
                            log.error("인코딩 변환 중 에러 발생 (ID: {}): {}", item.getFormId(), e.getMessage());
                            item.setFormData(Base64.getDecoder().decode(item.getBase64FormData()));
                        }
                    }
                    item.setCreateDate(safeTrimDate(item.getCreateDate()));
                    item.setUpdateDate(safeTrimDate(item.getUpdateDate()));
                    return item;
                })
                .writer(formDataWriter()) // 커스텀 라이터 호출
                .build();
    }

    /**
     * ORA-17004, ORA-03106 해결을 위한 커스텀 ItemWriter
     * JdbcBatchItemWriter의 일괄 실행 대신, 건별로 타입을 명시하여 실행합니다.
     */
    @Bean
    public ItemWriter<FormDataDto> customFormDataWriter() {
        return chunk -> {
            for (FormDataDto item : chunk) {
                // 1. UPDATE 시도
                int updated = jdbcTemplate.update("""
                                update query
                                """,
                        item.getFormData(),
                        nullIfEmpty(item.getUpdateDate()), item.getUpdateUserId(),
                        item.getFormId()
                );

                // 2. 결과가 0이면 INSERT 실행
                if (updated == 0) {
                    jdbcTemplate.update("""
                                    insert query
                                    """,
                            item.getFormId(), item.getFormData(),
                            nullIfEmpty(item.getCreateDate()), item.getCreateUserId(),
                            nullIfEmpty(item.getUpdateDate()), item.getUpdateUserId()
                    );
                }
            }
        };
    }

    /**
     * ORA-17004 및 Named Parameter 파싱 에러 해결을 위한 커스텀 ItemWriter
     */
    @Bean
    public ItemWriter<FormDataDto> formDataWriter() {
        return chunk -> {
            for (FormDataDto item : chunk) {
                try {
                    // 1. UPDATE 문 (WHERE 조건 포함 총 12개 파라미터)
                    String updateSql = """
                            update query
                            """;

                    int updated = jdbcTemplate.update(updateSql, ps -> {
                        ps.setBytes(1, item.getFormData()); // BLOB은 setBytes가 가장 확실함
                        ps.setString(9, item.getUpdateUserId());
                        ps.setLong(11, item.getFormId());
                    });

                    // 2. 결과가 없으면 INSERT (총 14개 파라미터)
                    if (updated == 0) {
                        String insertSql = """
                                insert query
                                """;

                        jdbcTemplate.update(insertSql, ps -> {
                            ps.setLong(2, item.getFormId());
                            ps.setBytes(4, item.getFormData());
                            ps.setString(12, item.getCreateUserId());
                            ps.setString(13, nullIfEmpty(item.getUpdateDate()));
                            ps.setString(14, item.getUpdateUserId());
                        });
                    }
                } catch (Exception e) {
                    log.error("적재 실패 [PID={}, FID={}]: {}", item.getProductId(), item.getFormId(), e.getMessage());
                    throw e;
                }
            }
        };
    }

    private String nullIfEmpty(String str) {
        return (str == null || str.trim().isEmpty()) ? null : str;
    }

    private <T> FlatFileItemReader<T> commonFileReader(String path, Class<T> type, String... names) {
        return new FlatFileItemReaderBuilder<T>()
                .name(type.getSimpleName() + "Reader")
                .resource(new FileSystemResource(path))
                .delimited().delimiter("|")
                .names(names)
                .targetType(type)
                .build();
    }

    private String safeTrimDate(String date) {
        if (date == null) return null;
        return (date.length() > 19) ? date.substring(0, 19) : date;
    }
}