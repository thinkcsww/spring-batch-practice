package com.applory.mybatch.job.partitionjob;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class MyPartitioner implements Partitioner {

    private final JdbcOperations jdbcTemplate;

    public MyPartitioner(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Integer min = jdbcTemplate.queryForObject("select min(id) from customer", Integer.class);
        Integer max = jdbcTemplate.queryForObject("select max(id) from customer", Integer.class);

        Map<String, ExecutionContext> executionContextMap = new HashMap<>();
        int targetSize = (max - min) / gridSize + 1;

        int number = 0;
        int start = min;
        int end = targetSize - 1;

        while (start <= max) {

            if (end >= max) {
                end = max;
            }

            ExecutionContext executionContext = new ExecutionContext();
            executionContext.putInt("minValue", start);
            executionContext.putInt("maxValue", end);

            executionContextMap.put("partition" + number, executionContext);

            start += targetSize;
            end += targetSize;
            number++;
        }

        return executionContextMap;
    }
}
