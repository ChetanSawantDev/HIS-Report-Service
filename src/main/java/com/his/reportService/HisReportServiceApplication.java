package com.his.reportService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableMongoRepositories(basePackages = "com.his.reportService.mongo.repositories")
@ComponentScan(basePackages = {"com.his.reportService", "org.his.core"})
@EnableFeignClients
public class HisReportServiceApplication {
	public static void main(String[] args) {
		SpringApplication.run(HisReportServiceApplication.class, args);
	}
}
