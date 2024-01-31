package com.applory.mybatch.simplejob;

import com.applory.mybatch.domain.Customer;
import com.applory.mybatch.domain.CustomerRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SimpleJobConfiguration {


    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final CustomerRepository customerRepository;

    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job simpleJob() {
        return new JobBuilder("simpleJob", jobRepository)
                .start(simpleChunckOrientedStep())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step simpleTaskletStep() {

        return new StepBuilder("name", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

                        List<Customer> customers = customerRepository.findAll();

                        customers.forEach(customer -> {
                            customer.setCount(customer.getCount() + 1);
                        });
                        log.info("this is my tasklet!!");

                        return RepeatStatus.FINISHED;
                    }
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Step simpleChunckOrientedStep() {
        return new StepBuilder("simpleChunckOrientedStep", jobRepository)
                .<Customer, Customer>chunk(50, platformTransactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public ItemReader<Customer> itemReader() {
        return new JpaPagingItemReaderBuilder<Customer>()
                .name("itemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(50)
                .queryString("select c from Customer c")
                .build();
    }

    @Bean
    public ItemProcessor<Customer, Customer> itemProcessor() {
        return item -> {
            item.setCount(item.getCount() + 1);
            return item;
        };
    }

    @Bean
    public ItemWriter<Customer> itemWriter() {
        return new JpaItemWriterBuilder<Customer>()
                .entityManagerFactory(entityManagerFactory)
                .build();

    }
}
