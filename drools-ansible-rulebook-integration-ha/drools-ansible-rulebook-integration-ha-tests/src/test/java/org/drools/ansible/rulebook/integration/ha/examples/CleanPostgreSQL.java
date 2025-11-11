package org.drools.ansible.rulebook.integration.ha.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Utility to clean (drop all tables) from the PostgreSQL database.
 *
 * Prerequisites:
 * 1. PostgreSQL must be running:
 *    docker-compose up -d
 *
 * 2. Run this utility:
 *    mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
 *      -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.CleanPostgreSQL" \
 *      -Dexec.classpathScope=test
 *
 * This utility will drop all EDA HA tables from the database:
 * - SessionState
 * - MatchingEvent
 * - ActionInfo
 * - HAStats
 */
public class CleanPostgreSQL {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/eda_ha_db";
    private static final String DB_USER = "eda_user";
    private static final String DB_PASSWORD = "eda_password";

    public static void main(String[] args) {
        // Set a simple thread name for cleaner log output
        Thread.currentThread().setName("main");

        System.out.println("=== PostgreSQL Database Cleanup ===\n");

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            System.out.println("Connected to PostgreSQL database: eda_ha_db");
            System.out.println("Dropping tables...\n");

            // Drop tables in reverse dependency order
            dropTableIfExists(stmt, "ActionInfo");
            dropTableIfExists(stmt, "MatchingEvent");
            dropTableIfExists(stmt, "SessionState");
            dropTableIfExists(stmt, "HAStats");

            System.out.println("\n=== Cleanup completed successfully! ===");
            System.out.println("\nThe database is now clean and ready for fresh runs.");

        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void dropTableIfExists(Statement stmt, String tableName) throws Exception {
        String sql = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
        System.out.println("Executing: " + sql);
        stmt.execute(sql);
        System.out.println("  -> " + tableName + " dropped successfully");
    }
}
