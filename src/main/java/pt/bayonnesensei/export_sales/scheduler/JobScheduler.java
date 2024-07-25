package pt.bayonnesensei.export_sales.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Date;

@Component
@RequiredArgsConstructor
public class JobScheduler {
    private final Job dbToFileJob;
    private final JobLauncher jobLauncher;

    @Scheduled(cron = "0/30 * * * * *")
    @SneakyThrows
    void trigger() {

        var fileName = LocalDate.now().toString().concat("_sales.csv");

        var jobParameters = new JobParametersBuilder()
                .addString("output.file.name", fileName)
                .addDate("processed", new Date())
                .toJobParameters();

        this.jobLauncher.run(dbToFileJob, jobParameters);
    }
}
