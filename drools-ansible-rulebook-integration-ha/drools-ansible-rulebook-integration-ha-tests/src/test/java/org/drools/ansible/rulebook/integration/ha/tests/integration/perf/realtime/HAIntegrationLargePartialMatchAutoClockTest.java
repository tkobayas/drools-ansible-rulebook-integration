package org.drools.ansible.rulebook.integration.ha.tests.integration.perf.realtime;

import java.io.IOException;
import java.net.Socket;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Realtime perf probe for large retained partial matches with AutomaticPseudoClock.
 * Unlike {@code HAIntegrationLargePartialMatchTest}, this test intentionally
 * does not use {@code FULLY_MANUAL_PSEUDOCLOCK}, so the internal pseudo-clock
 * advances on its background schedule.
 */
class HAIntegrationLargePartialMatchAutoClockTest extends AbstractHATestBase {

    private static final String HA_UUID = "large-partial-match-autoclock-ha";
    private static final int LARGE_PARTIAL_EVENT_COUNT = 1000;
    private static final int PARTIAL_EVENT_BLOB_SIZE = 24 * 1024;

    private static final String RULE_SET_LARGE_PARTIAL_EVENTS = """
            {
                "name": "Large Partial Event AutoClock Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "large_partial_event_rule",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "phase"
                                        },
                                        "rhs": {
                                            "String": "partial"
                                        }
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "complete"
                                        },
                                        "rhs": {
                                            "Boolean": true
                                        }
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [
                                {
                                    "name": "noop.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    static {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "WARN");

        if (USE_POSTGRES) {
            initializePostgres("eda_ha_autoclock_test", "HA large partial match auto-clock tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine;
    private long sessionId;
    private Socket asyncClientSocket;

    @BeforeEach
    void setUp() throws IOException {
        rulesEngine = new AstRulesEngine();

        // HA leader startup requires an async client connection even though this test
        // exercises the synchronous assertEvent path.
        asyncClientSocket = new Socket("localhost", rulesEngine.port());

        rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        // No FULLY_MANUAL_PSEUDOCLOCK — auto-clock runs naturally.
        sessionId = rulesEngine.createRuleset(RULE_SET_LARGE_PARTIAL_EVENTS);
        rulesEngine.enableLeader();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (asyncClientSocket != null) {
            asyncClientSocket.close();
        }
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
            rulesEngine.close();
        }
        cleanupDatabase();
    }

    //@Disabled("Just to check the response time with AutomaticPseudoClock")
    @Test
    void testLargePartialEventLastResponseTime() {
        long lastResponseNanos = 0L;
        String lastResponse = null;

        for (int i = 0; i < LARGE_PARTIAL_EVENT_COUNT; i++) {
            String event = createLargePartialEvent(i, PARTIAL_EVENT_BLOB_SIZE);
            long start = System.nanoTime();
            lastResponse = rulesEngine.assertEvent(sessionId, event);
            lastResponseNanos = System.nanoTime() - start;
            System.out.println("response time for event " + i + ": " + (lastResponseNanos / 1_000_000.0) + " ms");
        }
        System.gc();
        System.out.println("UsedMemory = " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));

        try {
            Thread.sleep(1000000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        double lastResponseMillis = lastResponseNanos / 1_000_000.0;
        System.out.printf("Large partial event auto-clock test: count=%d payloadBytes=%d lastResponseMs=%.3f%n",
                LARGE_PARTIAL_EVENT_COUNT, PARTIAL_EVENT_BLOB_SIZE, lastResponseMillis);
        System.out.println("Large partial event auto-clock test last response: " + lastResponse);
    }

    private static String createLargePartialEvent(int sequence, int blobSize) {
        String payload = "x".repeat(blobSize);
        return createEvent("""
                {
                    "phase": "partial",
                    "sequence": %d,
                    "blob": "%s"
                }
                """.formatted(sequence, payload));
    }
}
