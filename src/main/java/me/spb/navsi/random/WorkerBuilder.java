package me.spb.navsi.random;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class WorkerBuilder {
    final String CONNECTION_STRING = "jdbc:postgresql://148.251.121.251:1114";
    final String CREATE_DATABASE_QUERY = "CREATE DATABASE %s";
    final String SCHEMA_TABLES_QUERY = "CREATE TABLE IF NOT EXISTS %name (random_number int)";
    final String DB_PREFIX = "db_";
    final String TABLE_PREFIX = "table_";

    @Bean
    public Jdbi rootConnection() {
        return Jdbi.create(CONNECTION_STRING + "/postgres", defaultUser());
    }

    @Bean
    public Map<Integer, Worker> workers(@Autowired Jdbi rootConnection) {
        Map<Integer, Worker> connections = new HashMap<>();

        for (int i = 1; i <= 3; i++) {
            String dbName = DB_PREFIX + i;
            String tableName = TABLE_PREFIX + i;

            try {
                rootConnection.withHandle(x -> x.execute(String.format(CREATE_DATABASE_QUERY, dbName)));
            } catch (UnableToExecuteStatementException ignored) {
            }

            var connection = Jdbi.create(CONNECTION_STRING + "/" + dbName, defaultUser());
            connection.withHandle(x -> x.execute(SCHEMA_TABLES_QUERY.replace("%name", tableName)));

            connections.put(i, new Worker(connection, tableName));
        }

        return connections;
    }

    Properties defaultUser() {
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "postgres");

        return properties;
    }
}
