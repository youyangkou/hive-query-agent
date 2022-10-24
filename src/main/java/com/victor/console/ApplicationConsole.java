package com.victor.console;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import springfox.documentation.oas.annotations.EnableOpenApi;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@Configuration
@EnableOpenApi
@SpringBootApplication(scanBasePackages = "com.victor.console")
@EnableScheduling
@MapperScan("com.victor.console.dao")
public class ApplicationConsole {
    public static void main(String[] args) {
        SpringApplication.run(ApplicationConsole.class, args);
    }
}
