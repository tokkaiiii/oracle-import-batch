package oracleimport.oraclejdbc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class OracleJdbcImportApplication {

  public static void main(String[] args) {
    SpringApplication.run(OracleJdbcImportApplication.class, args);
  }

}
