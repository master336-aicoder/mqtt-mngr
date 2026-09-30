package io.mngr;

import io.mngr.config.MngrProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MngrProperties.class)
@MapperScan("io.mngr.store.h2.mapper")
public class MngrApplication {
    public static void main(String[] args) {
        SpringApplication.run(MngrApplication.class, args);
    }
}
