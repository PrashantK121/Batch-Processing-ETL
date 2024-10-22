package pt.bayonnesensei.export_sales.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import pt.bayonnesensei.export_sales.dto.SalesDTO;
import pt.bayonnesensei.export_sales.listeners.SalesWriterListener;
import pt.bayonnesensei.export_sales.processor.SalesProcessor;
import pt.bayonnesensei.export_sales.task.UploadFileToS3BucketTasklet;

import javax.sql.DataSource;
import java.util.Collections;

@EnableBatchProcessing
@Configuration
@RequiredArgsConstructor
public class ExportSalesJobConfig {

    public static final String SELECT_CLAUSE = "SELECT sale_id, product_id, customer_id, sale_date, sale_amount, store_location, country FROM sales WHERE processed = false";

    private final DataSource dataSource;
    private final SalesProcessor processor;
    private final JobRepository repository;
    private final PlatformTransactionManager transactionManager;
    private final SalesWriterListener salesWriterListener;
    private final UploadFileToS3BucketTasklet uploadFileToS3BucketTasklet;

    @Bean
    public Job dbToFileJob(Step fromSalesTableToFile) {
        return new JobBuilder("dbToFileJob", repository)
                .incrementer(new RunIdIncrementer())
                .start(fromSalesTableToFile)
                .next(uploadFileToS3())
                .build();
    }

    @Bean
    public Step fromSalesTableToFile(FlatFileItemWriter<SalesDTO> flatFileItemWriter,
                                     JdbcPagingItemReader<SalesDTO> salesJdbcPagingItemReader) {
        return new StepBuilder("from db to File", repository)
                .<SalesDTO, SalesDTO>chunk(2000, transactionManager)
                .reader(salesJdbcPagingItemReader)
                .processor(processor)
                .writer(flatFileItemWriter)
                .listener(salesWriterListener)
                .build();
    }


    //JDBCCursorItemReader
    @Bean
    public JdbcCursorItemReader<SalesDTO> salesJdbcCursorItemReader() {
        return new JdbcCursorItemReaderBuilder<SalesDTO>()
                .name("sales reader")
                .dataSource(dataSource)
                .sql(SELECT_CLAUSE)
                .fetchSize(100)
                .rowMapper(new DataClassRowMapper<>(SalesDTO.class))
                .build();
    }

    //JdbcPagingItemReader
    @Bean
    public JdbcPagingItemReader<SalesDTO> salesJdbcPagingItemReader(PagingQueryProvider queryProvider) {
        return new JdbcPagingItemReaderBuilder<SalesDTO>()
                .name("sales paging reader")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .rowMapper(new DataClassRowMapper<>(SalesDTO.class))
                .pageSize(25)
                .build();
    }

    @Bean
    public SqlPagingQueryProviderFactoryBean queryProvider() {
        var queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setSelectClause("SELECT sale_id, product_id, customer_id, sale_date, sale_amount, store_location, country");
        queryProvider.setFromClause("FROM Sales");
        queryProvider.setWhereClause("WHERE processed = false");
        queryProvider.setDataSource(dataSource);
        queryProvider.setDatabaseType(DatabaseType.POSTGRES.name());
        queryProvider.setSortKeys(Collections.singletonMap("sale_id", Order.ASCENDING));
        return queryProvider;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<SalesDTO> flatFileItemWriter(@Value("#{jobParameters['output.file.name']}") String outputFile) {
        return new FlatFileItemWriterBuilder<SalesDTO>()
                .name("sales file writer")
                .resource(new FileSystemResource(outputFile))
                .headerCallback(writer -> writer.append("Header of File"))
                .delimited()
                .delimiter(";")
                .sourceType(SalesDTO.class)
                .names("productId", "customerId", "saleDate", "saleAmount", "storeLocation", "country")
                .shouldDeleteIfEmpty(Boolean.FALSE)
                .append(Boolean.TRUE)
                .build();
    }

    @Bean
    public Step uploadFileToS3() {
        return new TaskletStepBuilder(new StepBuilder("uploadFileToS3", repository))
                .tasklet(uploadFileToS3BucketTasklet, transactionManager)
                .build();
    }

}
