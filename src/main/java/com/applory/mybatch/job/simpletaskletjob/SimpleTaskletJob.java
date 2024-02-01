package com.applory.mybatch.job.simpletaskletjob;

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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SimpleTaskletJob {


    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final CustomerRepository customerRepository;

    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job mySimpleTaskletJob() {
        return new JobBuilder("mySimpleTaskletJob", jobRepository)
                .start(simpleTaskletStep1())
                .next(simpleTaskletStep2())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step simpleTaskletStep1() {
        return new StepBuilder("mySimpleTaskletJob1", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

                        List<Customer> customers = customerRepository.findAll();

                        customers.forEach(customer -> {
                            customer.setCount(customer.getCount() + 1);
                        });
                        log.info("this is my simpleTaskletStep1!!");

                        return RepeatStatus.FINISHED;
                    }
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Step simpleTaskletStep2() {
        return new StepBuilder("mySimpleTaskletJob2", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        List<Customer> customers = customerRepository.findAll();

                        customers.forEach(customer -> {
                            customer.setCount(customer.getCount() + 1);
                        });
                        log.info("this is simpleTaskletStep2!!");

                        return RepeatStatus.FINISHED;
                    }
                }, platformTransactionManager)
                .build();
    }

}
