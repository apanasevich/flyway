package org.flyway.core.internal.command;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.Test;

import java.util.concurrent.*;

public final class DbMigrateTest {
  private static final int threadNum = 10;


  @SuppressWarnings("InstanceMethodNamingConvention")
  @Test
  public void schemaHistoryTableCreationFailsOnRestoringAfterAnError() throws Throwable {
    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/flyway");
    hikariConfig.setUsername("usr");
    hikariConfig.setPassword("pwd");
    hikariConfig.setAutoCommit(false); //The key statement
    final HikariDataSource dataSource = new HikariDataSource(hikariConfig);

    final CountDownLatch readyLatch = new CountDownLatch(threadNum);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    final CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executorService);

    for (int i = 0; i < threadNum; i++) {
      completionService.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          final Flyway flyway = new Flyway();
          flyway.setDataSource(dataSource);
          readyLatch.countDown();
          startLatch.await();
          flyway.migrate();
          return Boolean.TRUE;
        }
      });
    }
    readyLatch.await();
    startLatch.countDown();
    for (int i = 0; i < threadNum; i++) {
      final Future<Boolean> future = completionService.take();
      try {
        future.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
    executorService.shutdown();
  }
}