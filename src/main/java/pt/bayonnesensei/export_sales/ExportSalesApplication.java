package pt.bayonnesensei.export_sales;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ExportSalesApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExportSalesApplication.class, args);
	}

}
