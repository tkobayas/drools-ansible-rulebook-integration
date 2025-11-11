#!/bin/bash

echo "Starting PostgreSQL with docker-compose..."
docker-compose up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 5

echo ""
echo "Running PostgreSQL Simple Example..."
echo ""

cd ../..
mvn clean test-compile -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
  -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLSimpleExample" \
  -Dexec.classpathScope=test

echo ""
echo "To stop PostgreSQL: docker-compose down"
echo "To view logs: docker-compose logs -f postgres"
