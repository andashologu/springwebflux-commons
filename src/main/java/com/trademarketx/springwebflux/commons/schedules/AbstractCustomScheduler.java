package com.trademarketx.springwebflux.commons.schedules;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.springframework.r2dbc.core.DatabaseClient;
import com.trademarketx.springwebflux.commons.conversion.EntityConversion;
import com.trademarketx.springwebflux.commons.util.Util;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
public abstract class AbstractCustomScheduler<T, ID> {
    private final DatabaseClient databaseClient;
    private final EntityConversion entityConversion;
    // Per-entity-class: currently tracked scheduled date (id -> scheduledDate)
    // Map<x, y>
    // x = Class<T>
    // y = Map<ID, Instant>
    // Class<T> is a generic type for entity class
    // Map<ID, Instant> ID is the entity ID type (or UUID), Instant is the scheduled date
    private final Map<Class<T>, Map<ID, Instant>> scheduledEntityDate = new ConcurrentHashMap<>();
    // Per-entity-class: active scheduler subscription
    // Disposable is a reactive subscription that can be disposed to cancel the scheduler
    private final Map<Class<T>, Disposable> scheduledEntitySubscription = new ConcurrentHashMap<>();
    protected AbstractCustomScheduler(DatabaseClient databaseClient, EntityConversion entityConversion) {
        this.databaseClient = databaseClient;
        this.entityConversion = entityConversion;
    }
    protected abstract String getDateColumn();
    protected abstract String getDateGetter();
    // ====================================================
    // TRIGGER RESCHEDULE
    // ====================================================
    public Mono<Void> triggerReschedule(Class<T> clazz, Class<ID> idType, ID entityId, Instant entityDate, Function<T, Mono<Void>> action) {
        Map<ID, Instant> scheduledDateMap = scheduledEntityDate.computeIfAbsent(clazz, _ -> new ConcurrentHashMap<>()); // if exists get, else put // DO NOT DELETE COMMENT
        /* DO NOT DELETE
        IF scheduledEntityDate PUT INSTEAD OF GET
        This shows how computeIfAbsent works
            // First access:
            computeIfAbsent → creates empty map
            scheduledEntityDate = { User.class → {} }
            scheduledDateMap = {}
            // Still empty:
            scheduledDateMap.isEmpty() == true
            // After adding:
            scheduledDateMap.put(42, Instant.now());
            scheduledEntityDate = { User.class → { 42=2025-12-03T10:00 } }
            scheduledDateMap = { 42=2025-12-03T10:00 }
        NB:
            scheduledDateMap & scheduledEntityDate point to the same object in memory. If you change one, another also changes
            scheduledDateMap  ─┐
                                ├──> { 42 = 2025-12-03T10:00 }
            scheduledEntityDate ┘
                    User.class ─┘
        */
        // Nothing tracked -> schedule
        if (scheduledDateMap.isEmpty()) {
            return scheduleNext(clazz, idType, action);
        }
        ID scheduledEntityId = scheduledDateMap.keySet().stream().findFirst().orElse(null);
        Instant scheduledDate = scheduledDateMap.get(scheduledEntityId);
        // Safety reset
        if (scheduledEntityId == null || scheduledDate == null) {
            scheduledDateMap.clear();
            cancelExistingSchedule(clazz);
            return scheduleNext(clazz, idType, action);
        }
        // RESCHEDULE RULES
        boolean sameEntityUpdated = scheduledEntityId.equals(entityId);
        // Existing scheduled entity was updated or unscheduled
        if (sameEntityUpdated && entityDate == null) {
            scheduledDateMap.clear();
            cancelExistingSchedule(clazz);
            return scheduleNext(clazz, idType, action);
        }
        // Another entity was unscheduled; current timer remains valid
        if (entityDate == null) {
            return Mono.empty();
        }
        // Newly scheduled entity has an earlier date
        boolean earlierScheduledDate = !scheduledEntityId.equals(entityId) && entityDate.isBefore(scheduledDate);
        if (sameEntityUpdated || earlierScheduledDate) {
            scheduledDateMap.clear();
            cancelExistingSchedule(clazz);
            return scheduleNext(clazz, idType, action);
        }
        return Mono.empty();
    }
    // ====================================================
    // CANCEL ACTIVE TIMER
    // ====================================================
    private void cancelExistingSchedule(Class<T> clazz) {
        Disposable current = scheduledEntitySubscription.remove(clazz);

        if (current != null && !current.isDisposed()) {
            current.dispose();
        }
    }
    // ====================================================
    // SCHEDULE NEXT
    // ====================================================
    private Mono<Void> scheduleNext(Class<T> clazz, Class<ID> idType, Function<T, Mono<Void>> action) {
        // -----------------------------------
        // ATOMIC GATE: ensure ONLY ONE scheduler per class
        // -----------------------------------
        Disposable placeholder = () -> { /* no-op */ };
        Disposable gate = scheduledEntitySubscription.putIfAbsent(clazz, placeholder);
        if (gate != null) {
            IO.println("\nAbstractCustomScheduler.java > scheduleNext() > Scheduler already running for " + clazz.getSimpleName());
            return Mono.empty();
        }
        Map<ID, Instant> tracked = scheduledEntityDate.computeIfAbsent(clazz, _ -> new ConcurrentHashMap<>());
        return Mono.defer(() -> {
            T entity;

            try {
                entity = createEmptyInstance(clazz);
            } catch (Exception e) {
                scheduledEntitySubscription.remove(clazz);
                IO.println("\nAbstractCustomScheduler.java > scheduleNext() > Error creating entity instance");
                throw new RuntimeException("AbstractCustomScheduler.java > scheduleNext() > Error creating entity instance", e);
            }
            Mono<Void> schedule = findNextEntity(entity)
                .switchIfEmpty(Mono.fromRunnable(() -> {
                    tracked.clear();
                    scheduledEntitySubscription.remove(clazz);
                }))
                .flatMap(nextEntity -> {
                    if (nextEntity == null) {
                        tracked.clear();
                        scheduledEntitySubscription.remove(clazz);
                        IO.println("\nAbstractCustomScheduler.java > scheduleNext() > nextEntity is null");
                        return Mono.error(new IllegalStateException("AbstractCustomScheduler.java > scheduleNext() > nextEntity is null"));
                    }
                    ID nextEntityId = getEntityId(nextEntity, idType);
                    Instant nextEntityDate = getEntityDate(nextEntity);
                    long delayMs = Duration.between(Instant.now(), nextEntityDate).toMillis();
                    if (delayMs < 0) {
                        delayMs = 0;
                    }
                    tracked.clear();
                    tracked.put(nextEntityId, nextEntityDate);

                    return Mono.delay(Duration.ofMillis(delayMs))
                        .then(Mono.defer(() -> action.apply(nextEntity)))
                        //.then(Mono.delay(Duration.ofMillis(300)))   // allow DB commit visibility // i removed this cos add adding latency
                        .then(Mono.defer(() -> {
                            tracked.clear();
                            scheduledEntitySubscription.remove(clazz);
                            return scheduleNext(clazz, idType, action);
                        }));
                }).then()
                .subscribeOn(Schedulers.parallel());
            // Register scheduler subscription
            Disposable disposable = schedule.subscribe(
                null,
                error -> {
                    IO.println("\nAbstractCustomScheduler.java > scheduleNext() > Error occurred while scheduling entity for " + clazz.getSimpleName());
                    IO.println("\nError: " + error);
                    error.printStackTrace();
                    tracked.clear();
                    scheduledEntitySubscription.remove(clazz);
                }
            );
            // Replace the placeholder with the real subscription
            Disposable old = scheduledEntitySubscription.put(clazz, disposable);
            // -------------------------------------------------
            // MULTIPLE SCHEDULER DETECTION (KEEP THIS)
            // -------------------------------------------------
            if (old != null && old != placeholder && !old.isDisposed()) {
                IO.println("\nAbstractCustomScheduler.java > scheduleNext() > MULTIPLE SCHEDULERS detected for " + clazz.getSimpleName());
                old.dispose();
            }
            return Mono.empty();
        });
    }
    // ====================================================
    // HELPERS
    // ====================================================
    private ID getEntityId(T entity, Class<ID> idType) {
        try {
            Object id = entity.getClass().getMethod("getId").invoke(entity);
            if (id == null) {
                throw new IllegalStateException("Entity ID is null for: " + entity.getClass().getSimpleName());
            }
            if (!idType.isInstance(id)) {
                throw new IllegalStateException("ID type mismatch for " + entity.getClass().getSimpleName() + ": expected " + idType.getName() + " but got " + id.getClass().getName());
            }
            return idType.cast(id);
        } catch (IllegalAccessException | IllegalStateException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to read ID from entity: " + entity.getClass().getName(), e);
        }
    }
    private Instant getEntityDate(T entity) {
        try {
            Object entityDate = entity.getClass().getMethod(getDateGetter()).invoke(entity);
            if (entityDate == null) {
                throw new IllegalStateException("Scheduled date is null for: " + entity.getClass().getSimpleName());
            }
            if (!Instant.class.isInstance(entityDate)) {
                throw new IllegalStateException("Scheduled date type mismatch for " + entity.getClass().getSimpleName() + ": expected " + Instant.class.getName() + " but got " + entityDate.getClass().getName());
            }
            return Instant.class.cast(entityDate);
        } catch (IllegalAccessException | IllegalStateException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to read scheduled date from entity: " + entity.getClass().getName(), e);
        }
    }
    private Mono<T> findNextEntity(T entity) {
        String tableName = Util.getTableName(entity.getClass());
        String sql = """
            SELECT *
            FROM "%s"
            WHERE "%s" IS NOT NULL
            ORDER BY "%s" ASC
            LIMIT 1
        """.formatted(tableName, getDateColumn(), getDateColumn());
        return databaseClient.sql(sql)
            .map((row, _) -> entityConversion.rowToEntity(row, entity))
            .one()
            .flatMap(e -> e == null ? Mono.empty() : Mono.just(e));
    }
    private T createEmptyInstance(Class<T> clazz) throws Exception {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new Exception("AbstractCustomScheduler.java > createEmptyInstance() > Cannot create instance for: " + clazz.getSimpleName(), e);
        }
    }
    // ====================================================
    // START
    // ====================================================
    public Mono<Void> start(Class<T> clazz, Class<ID> idType, Function<T, Mono<Void>> action) {
        return scheduleNext(clazz, idType, action);
    }
}