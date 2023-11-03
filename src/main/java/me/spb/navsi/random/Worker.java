package me.spb.navsi.random;

import org.jdbi.v3.core.Jdbi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;


public class Worker {
    Jdbi jdbi;
    ExecutorService executor = Executors.newFixedThreadPool(5);
    String INSERT_QUERY = "INSERT INTO %name (random_number) VALUES (?);";

    public Worker(Jdbi jdbi, String tableName) {
        this.jdbi = jdbi;
        this.INSERT_QUERY = INSERT_QUERY.replace("%name", tableName);
    }

    public CompletableFuture<Integer> submit(Integer number, long sleepFactor) {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        executor.submit(() -> {
            if(sleepFactor > 0) {
                try {
                    Thread.sleep(sleepFactor);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            future.complete(jdbi.withHandle(x -> {
                return x.createUpdate(INSERT_QUERY).bind(0, number).execute();
            }));
        });

        return future;
    }
}
