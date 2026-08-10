## Test tips

Other than usual unit tests, we have some irregular tests:

- **MemoryLeakTest**: This test checks for memory leaks in the code. As they are slow and its results are not deterministic, they are not run by default. To test them, run `mvn test -Pmemoryleak-tests`.
- **load test scripts**: See `drools-ansible-rulebook-integration-load-tests/README.md`.

- **migration tests**: See `drools-ansible-rulebook-integration-migration-tests/README.md`.

The above 3 tests are relatively important to detect memory leak issues and migration compatibility. Added to github action `pull-request.yml`.

- **PerfTest**: This test contains various and relatively high load tests, which are run by default.
- **SlownessTest**: This test verifies the behavior under the real slowness (but not very long). It is run by default.
