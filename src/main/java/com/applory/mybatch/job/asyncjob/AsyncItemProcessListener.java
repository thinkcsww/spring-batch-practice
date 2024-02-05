package com.applory.mybatch.job.asyncjob;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;

@Slf4j
public class AsyncItemProcessListener implements ItemProcessListener {

    @Override
    public void beforeProcess(Object item) {
        log.info("==================== ItemProcessListener : {}", Thread.currentThread().getName());
    }
}
