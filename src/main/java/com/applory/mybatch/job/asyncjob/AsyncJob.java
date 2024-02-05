package com.applory.mybatch.job.asyncjob;

import com.applory.mybatch.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class AsyncJob {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    private final DataSource dataSource;


    @Bean
    public Job myAsyncJob() throws Exception {
        return new JobBuilder("myAsyncJob", jobRepository)
                .listener(new StopWatchJobListener())
                .incrementer(new RunIdIncrementer())
                .start(myAsyncJobStep1())
                .build();
    }

    @Bean
    public Step myAsyncJobStep1() throws Exception {

        return new StepBuilder("myAsyncJobStep1", jobRepository)
                .<Customer, Customer>chunk(100, platformTransactionManager)
                .reader(myAsyncJobReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .listener(new AsyncItemProcessListener())
                .taskExecutor(myAsyncJobTaskExecutor())
                .build();
    }

    @Bean
    public ItemReader<Customer> myAsyncJobReader() {
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(this.dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new RowMapper<>() {
            @Override
            public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new Customer(
                        rs.getInt("id"),
                        rs.getString("firstName"),
                        rs.getString("lastName"),
                        LocalDateTime.now(),
                        rs.getInt("count")
                );
            }
        });

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, birthdate, count");
        queryProvider.setFromClause("from customer");

        Map<String, Order> sortKeys = new HashMap<>(1);

        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);


        return reader;
    }

    @Bean
    public AsyncItemProcessor asyncItemProcessor() throws Exception {
        AsyncItemProcessor asyncItemProcessor = new AsyncItemProcessor<>();

        asyncItemProcessor.setDelegate(customItemProcessor());
        asyncItemProcessor.setTaskExecutor(myAsyncJobTaskExecutor());
//        asyncItemProcessor.afterPropertiesSet();

        return asyncItemProcessor;
    }

    @Bean
    public ItemProcessor customItemProcessor() {
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(Customer item) throws Exception {

                log.info("================ AsyncStepListener thread name: {}", Thread.currentThread().getName());

//                Thread.sleep(1000);

                return new Customer(item.getId(),
                        item.getFirstName().toUpperCase(),
                        item.getLastName().toUpperCase(),
                        item.getBirthdate(),
                        100);
            }
        };
    }

    @Bean
    public AsyncItemWriter asyncItemWriter() throws Exception {
        AsyncItemWriter<Customer> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(customItemWriter());
        asyncItemWriter.afterPropertiesSet();

        return asyncItemWriter;
    }

    @Bean
    public ItemWriter<Customer> customItemWriter() {
        return chunk -> {
            List<? extends Customer> items = chunk.getItems();

            for (Customer item : items) {
                log.info("ItemWriter: {} : {}", Thread.currentThread().getName(), item.getId());
            }

        };
    }


    /**
     * step을 처리하는 스레드풀의 default 크기는 4개
     * 4개의 스레드가 각각 AsyncItemProcess를 실행하는데 이미 4개의 스레드는 모두 점유되어 있음
     * 그런데 스레드의 개수를 maxPoolSize만큼 늘려주지 않음 그렇기 때문에 TaskExecutor의 queue에 계속 쌓이기만 하고 처리가 안됨 -> 데드락에 걸림
     * corePoolSize > 4 설정해줘야 처리 가능
     * 혹은 AsyncItemProcessor에 SimpleAsyncTaskExecutor를 사용
     * @return
     */
    @Bean
    public TaskExecutor myAsyncJobTaskExecutor() {

        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(4);
        threadPoolTaskExecutor.setMaxPoolSize(1000);
        threadPoolTaskExecutor.initialize();
        threadPoolTaskExecutor.setThreadNamePrefix("async-thread");

        return threadPoolTaskExecutor;
    }

}
