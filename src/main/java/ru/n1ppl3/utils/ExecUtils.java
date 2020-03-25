package ru.n1ppl3.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public abstract class ExecUtils {

    public static <T> List<T> executeParallelTasks(List<Callable<T>> callableList) throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(callableList.size());
        try {
            return doWithExecutor(executorService, callableList);
        } finally {
            executorService.shutdown();
        }
    }

    private static <T> List<T> doWithExecutor(ExecutorService executorService, List<Callable<T>> callableList) throws InterruptedException, ExecutionException {
        List<T> result = new ArrayList<>(callableList.size());
        List<Future<T>> futures = executorService.invokeAll(callableList);
        for (Future<T> future : futures) {
            result.add(future.get());
        }
        return result;
    }
}
