package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.dropTables;

/**
 * Base class for AstRulesEngine HA integration tests
 */
abstract class HAIntegrationTestBase {

    protected static final String HA_UUID = "integration-ha-1";

    protected AstRulesEngine rulesEngine1; // node 1
    protected AstRulesEngine rulesEngine2; // node 2

    protected long sessionId1; // node1
    protected long sessionId2; // node2

    protected AsyncConsumer consumer1; // node1
    protected AsyncConsumer consumer2; // node2

    abstract String getRuleSet();

    @BeforeEach
    void setUp() {
        rulesEngine1 = new AstRulesEngine();
        rulesEngine1.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG); // The same cluster. Both nodes share same DB
        sessionId1 = rulesEngine1.createRuleset(getRuleSet());

        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine2 = new AstRulesEngine();
        rulesEngine2.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG); // The same cluster. Both nodes share same DB
        sessionId2 = rulesEngine2.createRuleset(getRuleSet());

        consumer2 = new AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }

        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
        }

        dropTables();
    }

    // Simulate a python client that consumes async responses
    public static class AsyncConsumer {
        private volatile boolean keepReading = true;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final String name;
        private final List<String> receivedMessages = new ArrayList<>();

        public AsyncConsumer(String name) {
            this.name = name;
        }

        public void startConsuming(int port) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", port)) {
                    socket.setSoTimeout(1000);
                    DataInputStream stream = new DataInputStream(socket.getInputStream());

                    while (keepReading && !Thread.currentThread().isInterrupted()) {
                        try {
                            if (stream.available() > 0) {
                                int l = stream.readInt();
                                byte[] bytes = stream.readNBytes(l);
                                String result = new String(bytes, StandardCharsets.UTF_8);
                                System.out.println(name + " - Async result: " + result);
                                receivedMessages.add(result);
                            }
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                    }
                } catch (IOException e) {
                    if (keepReading) {
                        System.err.println(name + " error: " + e.getMessage());
                    }
                }
            });
        }

        public void stop() {
            keepReading = false;
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }

        public List<String> getReceivedMessages() {
            return receivedMessages;
        }
    }


    // Helper method to create HAStateManager to assert database
    protected HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create();
        manager.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        return manager;
    }

    protected String getRuleSetNameValue() {
        return (String) readValueAsMapOfStringAndObject(getRuleSet()).get("name");
    }
}
