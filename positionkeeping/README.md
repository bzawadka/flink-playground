### Run
Run `EventsProcessorJob` as IntelliJ run configuration with "include dependencies with 'Provided' scope."

Expected output:

```
EUR position update: 500.0
USD position update: 1000.0
HKD position update: 25000.0
PLN position update: 400.0
EUR position update: 1000.0
USD position update: 2000.0
HKD position update: 50000.0
PLN position update: 800.0
EUR position update: 1500.0
USD position update: 3000.0
HKD position update: 75000.0
PLN position update: 1200.0
...
```

### Web UI
Flink Web UI: http://localhost:8081/

### Additional ideas
Expose internal state as API using queryable state