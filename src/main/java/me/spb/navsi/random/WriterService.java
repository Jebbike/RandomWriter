package me.spb.navsi.random;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@Service
@Log4j2()
public class WriterService implements ApplicationListener<ApplicationReadyEvent> {
    @Autowired
    Map<Integer, Worker> workers;
    final int NEEDED_INSERT = 1000;
    final int WATCHER_PERIOD = 1;
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    Map<Integer, Integer> completedQueriesStats = new ConcurrentHashMap<>();
    Map<Integer, Integer> plannedStats = new HashMap<>();
    ScheduledFuture<?> scheduledFuture;

    @Override
    public void onApplicationEvent(final ApplicationReadyEvent event) {
        log.info("start cycle...");

        for (int i = 0; i < NEEDED_INSERT; i++) {
            int dbNumber = Utils.randInt(1, 3);
            int value = Utils.randInt(0, 5000);

            log.debug("Inserting `{}` to `{}`", value, dbNumber);

            Utils.incrementByKey(dbNumber, plannedStats);

            workers.get(dbNumber).submit(value, 0).thenAcceptAsync(result -> {
                Utils.incrementByKey(dbNumber, completedQueriesStats);
            });
        }

        log.info("cycle finished");

        runStatWatcher();
    }

    void runStatWatcher() {
        this.scheduledFuture = executor.scheduleAtFixedRate(() -> {
            if (completedQueriesStats.isEmpty())
                return;

            if(completedQueriesStats.equals(plannedStats)) {
                log.info("");
                printMap(completedQueriesStats);
                log.info("All queries completed!");
                log.info("Total queries: " + completedQueriesStats.values().stream().reduce(Integer::sum).orElse(0));

                scheduledFuture.cancel(false);
                return;
            }

            log.info("");
            printMap(completedQueriesStats);

        }, 0, WATCHER_PERIOD, TimeUnit.SECONDS);
    }

    void printMap(Map<Integer, Integer> db) {
        plannedStats.forEach((k, v) -> {
            Integer completed = completedQueriesStats.getOrDefault(k, 0);
            log.info("db_{} -> {}/{}", k, completed ,v );
        });
    }
}
