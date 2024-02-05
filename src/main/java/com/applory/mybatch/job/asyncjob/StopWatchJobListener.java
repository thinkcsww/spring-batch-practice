package com.applory.mybatch.job.asyncjob;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class StopWatchJobListener implements JobExecutionListener {

    @Override
    public void afterJob(JobExecution jobExecution) {
        long time = jobExecution.getEndTime().getSecond() - jobExecution.getStartTime().getSecond();
        System.out.println("==========================================");
        System.out.println("총 소요된 시간 : " + time);
        System.out.println("==========================================");
    }
}
