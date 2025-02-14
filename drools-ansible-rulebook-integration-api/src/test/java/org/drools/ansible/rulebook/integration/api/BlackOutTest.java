package org.drools.ansible.rulebook.integration.api;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.junit.Assert.assertEquals;

public class BlackOutTest {

    @Test
    public void yearly() throws ExecutionException, InterruptedException {
        String json =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name": "r1",
                            "condition":{
                               "EqualsExpression":{
                                  "lhs":{
                                     "sensu":"data.i"
                                  },
                                  "rhs":{
                                     "Integer":1
                                  }
                               }
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ],
                            "black_out": {
                               "trigger": "all",
                               "timezone": "local",
                               "start_time": {
                                  "minute": 0,
                                  "hour": 14,
                                  "day_of_month": 1,
                                  "month": 7
                               },
                               "end_time": {
                                  "minute": 0,
                                  "hour": 16,
                                  "day_of_month": 1,
                                  "month": 7
                               }
                            }
                         }
                      }
                   ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        LocalDateTime startDateTime = LocalDateTime.now();
        LocalDateTime testDateTime = startDateTime.withMonth(7).withDayOfMonth(1).withHour(15).withMinute(0); // in the middle of the blackout period
        long secondsToAdd = Duration.between(startDateTime, testDateTime).toSeconds();
        System.out.println("Seconds to add: " + secondsToAdd);
        rulesExecutor.advanceTime(secondsToAdd, TimeUnit.SECONDS);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertEquals(0, matchedRules.size());

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS);
        matches.get().forEach(match -> System.out.println("Match: " + match.getRule().getName()));
        assertEquals(1, matches.get().size());
    }
}
