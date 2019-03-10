package com.example.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaDemoApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}
}