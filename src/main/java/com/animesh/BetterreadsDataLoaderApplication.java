package com.animesh;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BetterreadsDataLoaderApplication {

	public static void main(String[] args) {

		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
		//Tries to connect to local cassandra through a port 
	}

}
