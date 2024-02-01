package com.applory.mybatch.job.simpleflowjob;

import com.applory.mybatch.domain.CustomerRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
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

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SimpleFlowJob {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final CustomerRepository customerRepository;

    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job mySimpleFlowJob() {
        return new JobBuilder("mySimpleFlowJob", jobRepository)
                .start(failStep())
                .on(ExitStatus.FAILED.getExitCode()).to(recoveryStep())
                .from(failStep()).on("*").to(nextStep())
                .end()
                .incrementer(new RunIdIncrementer())
                .build();
    }


//    @Bean
//    public Flow flow1() {
//        new FlowBuilder("flow1")
//                .start(failStep())
//                .on(ExitStatus.FAILED.getExitCode())
//                .to(recoveryStep())
//                .end();
//    }

    @Bean
    Step failStep() {
        return new StepBuilder("mySimpleFlowJobStep1", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        log.info("=========== mySimpleFlowJobStep1 ==============");
                        throw new RuntimeException("mySimpleFlowJobStep1 fail");
//                        return null;
                    }
                }, platformTransactionManager)
                .build();
    }

    @Bean
    Step recoveryStep() {
        return new StepBuilder(" ================ mySimpleFlowJobRecoveryStep2 ================ ", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        log.info("mySimpleFlowJobRecoveryStep2");
                        return null;
                    }
                }, platformTransactionManager)
                .build();
    }

    @Bean
    Step nextStep() {
        return new StepBuilder("mySimpleFlowJobNextStep", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        log.info(" ================ mySimpleFlowJobNextStep ================ ");
                        return null;
                    }
                }, platformTransactionManager)
                .build();
    }

}
