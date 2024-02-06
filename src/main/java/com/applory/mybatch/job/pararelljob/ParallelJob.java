package com.applory.mybatch.job.pararelljob;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@RequiredArgsConstructor
@Slf4j
@Configuration
public class ParallelJob {

    private final PlatformTransactionManager platformTransactionManager;

    private final JobRepository jobRepository;

    private final DataSource dataSource;


    @Bean
    public Job myParallelJob() {
        return new JobBuilder("myParallelJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(myParallelJobFlow1())
                .split(myParallelJobStepTaskExecutor()).add(myParallelJobFlow2())
                .end()
                .build();
    }

    @Bean
    public Flow myParallelJobFlow1() {

        TaskletStep step = new StepBuilder("myParallelJobStep1", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                        try {
                            Thread.sleep(1000);
                            log.info("========== myParallelJobFlow1 started ===========");
                        } catch (Exception e) {

                        }
                        return null;
                    }
                }, platformTransactionManager)
                .build();

        return new FlowBuilder<Flow>("myParallelJobFlow1")
                .start(step)
                .build();
    }

    @Bean
    public Flow myParallelJobFlow2() {

        TaskletStep step = new StepBuilder("myParallelJobFlow2", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                        try {
                            Thread.sleep(2000);
                            log.info("========== myParallelJobFlow1 started ===========");
                        } catch (Exception e) {

                        }
                        return null;
                    }
                }, platformTransactionManager)
                .build();

        return new FlowBuilder<Flow>("myParallelJobFlow2")
                .start(step)
                .build();
    }

    @Bean
    public TaskExecutor myParallelJobStepTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(8);
        executor.setThreadNamePrefix("async-thread-");
        return executor;
    }
}
