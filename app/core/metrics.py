from prometheus_client import Counter, Histogram

# Metrics for Saga Orchestration tracking
SAGA_EXECUTIONS_TOTAL = Counter(
    "saga_executions_total",
    "Total count of saga executions by final status",
    ["status"]
)

SAGA_STEP_DURATION_SECONDS = Histogram(
    "saga_step_duration_seconds",
    "Duration of individual saga steps in seconds",
    ["step_name", "status"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

SAGA_COMPENSATIONS_TOTAL = Counter(
    "saga_compensations_total",
    "Total count of compensating transactions triggered",
    ["step_name"]
)

AI_SCHEMA_MAPPINGS_TOTAL = Counter(
    "ai_schema_mappings_total",
    "Total number of schema mappings performed by method",
    ["method", "status"]
)

