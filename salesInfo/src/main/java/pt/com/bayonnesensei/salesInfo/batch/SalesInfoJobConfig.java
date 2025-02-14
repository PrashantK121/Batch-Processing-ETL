package pt.com.bayonnesensei.salesInfo.batch;


import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import pt.com.bayonnesensei.salesInfo.batch.dto.SalesInfoDTO;
import pt.com.bayonnesensei.salesInfo.batch.faulttolerance.CustomSkipPolicy;
import pt.com.bayonnesensei.salesInfo.batch.listeners.CustomJobExecutionListener;
import pt.com.bayonnesensei.salesInfo.batch.listeners.CustomStepExecutionListener;
import pt.com.bayonnesensei.salesInfo.batch.processor.SalesInfoItemProcessor;
import pt.com.bayonnesensei.salesInfo.domain.SalesInfo;

import javax.persistence.EntityManagerFactory;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@RequiredArgsConstructor
public class SalesInfoJobConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final SalesInfoItemProcessor salesInfoItemProcessor;

    private final CustomSkipPolicy customSkipPolicy;

    private final CustomStepExecutionListener customStepExecutionListener;

    private final CustomJobExecutionListener customJobExecutionListener;

    private final KafkaTemplate<String,SalesInfo> salesInfoKafkaTemplate;


    @Bean
    public Job importSalesInfo(Step fromFileIntoDataBase){
        return jobBuilderFactory.get("importSalesInfo")
                .incrementer(new RunIdIncrementer())
                .start(fromFileIntoDataBase)
                .listener(customJobExecutionListener)
                .build();
    }


    @Bean
    public Step fromFileIntoKafka(ItemReader<SalesInfoDTO> salesInfoDTOItemReader){
        return stepBuilderFactory.get("fromFileIntoDatabase")
                .<SalesInfoDTO, Future<SalesInfo>>chunk(100)
                .reader(salesInfoDTOItemReader)
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .faultTolerant()
                .skipPolicy(customSkipPolicy)
                .listener(customStepExecutionListener)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<SalesInfoDTO> salesInfoFileReader(@Value("#{jobParameters['input.file.name']}") String resource){
        return new FlatFileItemReaderBuilder<SalesInfoDTO>()
                .resource(new FileSystemResource(resource))
                .name("salesInfoFileReader")
                .delimited()
                .delimiter(",")
                .names("product","seller","sellerId","price","city","category")
                .linesToSkip(1)
                .targetType(SalesInfoDTO.class)
                .build();
    }

    @Bean
    public JpaItemWriter<SalesInfo> salesInfoItemWriter(){
        return new JpaItemWriterBuilder<SalesInfo>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(15);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("Thread N-> :");
        return executor;
    }

    @Bean
    public AsyncItemProcessor<SalesInfoDTO,SalesInfo> asyncItemProcessor(){
        var asyncItemProcessor = new AsyncItemProcessor<SalesInfoDTO,SalesInfo>();
        asyncItemProcessor.setDelegate(salesInfoItemProcessor);
        asyncItemProcessor.setTaskExecutor(taskExecutor());
        return asyncItemProcessor;
    }

    @Bean
    public AsyncItemWriter<SalesInfo> asyncItemWriter(){
        var asyncWriter = new AsyncItemWriter<SalesInfo>();
        asyncWriter.setDelegate(salesInfoKafkaItemWriter());
        return asyncWriter;
    }

    @Bean
    @SneakyThrows
    public KafkaItemWriter<String,SalesInfo> salesInfoKafkaItemWriter(){
        var kafkaItemWriter = new KafkaItemWriter<String,SalesInfo>();
        kafkaItemWriter.setKafkaTemplate(salesInfoKafkaTemplate);
        kafkaItemWriter.setItemKeyMapper(salesInfo -> String.valueOf(salesInfo.getSellerId()));
        kafkaItemWriter.setDelete(Boolean.FALSE);
        kafkaItemWriter.afterPropertiesSet();
        return kafkaItemWriter;
    }
}
