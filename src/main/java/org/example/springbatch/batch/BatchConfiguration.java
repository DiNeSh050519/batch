package org.example.springbatch.batch;

import org.example.springbatch.model.EmployeeDetail;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {
    @Value("${file.input}")
    String fileInput;
    @Bean
    public FlatFileItemReader<EmployeeDetail> reader() {
        FlatFileItemReaderBuilder faltfileItemReaderBuilder = new FlatFileItemReaderBuilder<>();
        return faltfileItemReaderBuilder.resource(new ClassPathResource(fileInput)).name("reader").delimited().names(new String[]{"id", "name", "designation", "phoneNumber"}).fieldSetMapper(new BeanWrapperFieldSetMapper<>() {
            {
                setTargetType(EmployeeDetail.class);
            }
        }).linesToSkip(1).build();
    }
    @Bean
    public JdbcBatchItemWriter<EmployeeDetail> writer(DataSource dataSource){
        JdbcBatchItemWriterBuilder jdbcBatchItemWriterBuilder = new JdbcBatchItemWriterBuilder();
        return jdbcBatchItemWriterBuilder.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider())
                .sql("INSERT INTO EMPLOYEE1 VALUES(:id,:name,:designation,:phoneNumber)")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public Job job(JobRepository jobRepository,Step step){
            JobBuilder jobBuilder = new JobBuilder("Coding", jobRepository);
            return jobBuilder.start(step).build();

        }
    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, JdbcBatchItemWriter jdbcBatchItemWriter){
        StepBuilder stepBuilder = new StepBuilder("Step", jobRepository);
        return stepBuilder.chunk(2, platformTransactionManager).reader(reader()).writer(jdbcBatchItemWriter).build();
    }
}
