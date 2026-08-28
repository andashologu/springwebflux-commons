package com.trademarketx.springwebflux.commons.softdeletion.repository;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import com.trademarketx.springwebflux.commons.conversion.EntityConversion;
import com.trademarketx.springwebflux.commons.util.Util;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
public class CustomExpiryScheduler<T, ID> {

    private final DatabaseClient databaseClient;
    private EntityConversion entityConversion;

    // Per-entity-class: currently tracked expiry (id -> expiry)
    private final Map<Class<T>, Map<ID, Instant>> scheduledEntityExpiry = new ConcurrentHashMap<>();

    // Per-entity-class: active scheduler subscription
    private final Map<Class<T>, Disposable> scheduledEntitySubscription = new ConcurrentHashMap<>();

    public CustomExpiryScheduler(DatabaseClient databaseClient, EntityConversion entityConversion) {
        this.databaseClient = databaseClient;
        this.entityConversion = entityConversion;
    }

    // ====================================================
    // TRIGGER RESCHEDULE
    // ====================================================
    public Mono<Void> triggerReschedule(
            Class<T> clazz,
            Class<ID> idType,
            ID entityId,
            Instant entityExpiryDate,
            CustomExpiryRepository<T> service
    ) {
        
            //IO.print("\n[TRIGGER] " + clazz.getSimpleName()
            //        + " id=" + entityId
            //        + " expiry=" + entityExpiryDate
            //        + " thread=" + Thread.currentThread().getName());

            //if (entityExpiryDate == null) return Mono.empty();

            Map<ID, Instant> scheduledExpiryMap = scheduledEntityExpiry.computeIfAbsent(clazz, _ -> new ConcurrentHashMap<>()); // if exists get, else put // DO NOT DELETE COMMENT
            /* DO NOT DELETE

            IF scheduledEntityExpiry PUT INSTEAD OF GET

            This shows how computeIfAbsent works

                // First access:
                computeIfAbsent → creates empty map
                scheduledEntityExpiry = { User.class → {} }
                scheduledExpiryMap = {}

                // Still empty:
                scheduledExpiryMap.isEmpty() == true

                // After adding:
                scheduledExpiryMap.put(42, Instant.now());
                scheduledEntityExpiry = { User.class → { 42=2025-12-03T10:00 } }
                scheduledExpiryMap = { 42=2025-12-03T10:00 }
                scheduledExpiryMap.isEmpty() == false

                NB:

                scheduledExpiryMap & scheduledEntityExpiry point to the same object in memory. If you change one, another also changes

                scheduledExpiryMap  ─┐
                                    ├──> { 42 = 2025-12-03T10:00 }
                scheduledEntityExpiry ┘
                        User.class ─┘

            */

            //IO.print("[TRIGGER] current tracked = " + scheduledExpiryMap);

            // Nothing tracked -> schedule
            if (scheduledExpiryMap.isEmpty()) {
                //IO.print("[TRIGGER] nothing tracked -> scheduling for " + clazz.getSimpleName());
                return scheduleNext(clazz, idType, service);
            }

            ID scheduledExpriryId = scheduledExpiryMap.keySet().stream().findFirst().orElse(null);
            Instant scheduledExpriryDate = scheduledExpiryMap.get(scheduledExpriryId);

            // Safety reset
            if (scheduledExpriryId == null || scheduledExpriryDate == null) {
                //IO.print("[TRIGGER] corrupted tracked state -> clearing & rescheduling");
                scheduledExpiryMap.clear();
                cancelExistingSchedule(clazz);
                return scheduleNext(clazz, idType, service);
            }

            // RESCHEDULE RULES
            boolean sameEntityUpdated = scheduledExpriryId.equals(entityId);
            
            if(sameEntityUpdated && entityExpiryDate == null) {
                scheduledExpiryMap.clear();
                cancelExistingSchedule(clazz);
                return scheduleNext(clazz, idType, service);
            }

            if(entityExpiryDate == null) { // recently added! please observe
                return Mono.empty();
            }

            boolean earlierExpiry = !scheduledExpriryId.equals(entityId) && entityExpiryDate.isBefore(scheduledExpriryDate);  

            if (sameEntityUpdated || earlierExpiry) {
                //IO.print("[TRIGGER] rescheduling for " + clazz.getSimpleName());
                scheduledExpiryMap.clear();
                cancelExistingSchedule(clazz);
                return scheduleNext(clazz, idType, service);
            }   

            //IO.print("[TRIGGER] sameEntityUpdated=" + sameEntityUpdated + ", earlierExpiry=" + earlierExpiry);

            if (sameEntityUpdated || earlierExpiry) {
                //IO.print("[TRIGGER] rescheduling for " + clazz.getSimpleName());
                IO.print("CustomExpiryScheduler.java > triggerReschedule() > entityId" + entityId + " cancelling timer");
                scheduledExpiryMap.clear();
                cancelExistingSchedule(clazz);
                return scheduleNext(clazz, idType, service);
            }

            //IO.print("[TRIGGER] NO ACTION for " + clazz.getSimpleName());
            return Mono.empty();

        
    }

    // ====================================================
    // CANCEL ACTIVE TIMER
    // ====================================================
    private void cancelExistingSchedule(Class<T> clazz) {
        //IO.print("[CANCEL] cancelExistingSchedule(" + clazz.getSimpleName() + ")");
        Disposable current = scheduledEntitySubscription.remove(clazz);
        if (current != null && !current.isDisposed()) {
            current.dispose();
        }
    }

    // ====================================================
    // SCHEDULE NEXT
    // ====================================================
    private Mono<Void> scheduleNext(Class<T> clazz, Class<ID> idType, CustomExpiryRepository<T> service) {

        //IO.print("\n========== scheduleNext ENTER ==========" +
        //        " Class=" + clazz.getSimpleName() +
        //        " Thread=" + Thread.currentThread().getName() +
        //        " ActiveSchedules=" + scheduledEntitySubscription.keySet() +
        //        " TrackedMap=" + scheduledEntityExpiry.get(clazz));

        // -----------------------------------
        // ATOMIC GATE: ensure ONLY ONE scheduler per class
        // -----------------------------------
        Disposable placeholder = () -> { /* no-op */ };
        Disposable gate = scheduledEntitySubscription.putIfAbsent(clazz, placeholder);

        if (gate != null) {
            //IO.print("[SCHEDULER] already running for " + clazz.getSimpleName() + " (gate=" + gate + ")");
            return Mono.empty();
        }

        Map<ID, Instant> tracked = scheduledEntityExpiry.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>());

        return Mono.defer(() -> {

            T entity;
            try {
                entity = createEmptyInstance(clazz);
            } catch (Exception e) {
                scheduledEntitySubscription.remove(clazz);
                throw new RuntimeException("\nCustomExpiryRepository.class > scheduleNext() > Error creating entity instance", e);
            }

            Mono<Void> schedule = findNextExpiry(entity)
                    .switchIfEmpty(Mono.fromRunnable(() -> {
                        //IO.print("[SCHEDULE] no records found for " + clazz.getSimpleName());
                        tracked.clear();
                        scheduledEntitySubscription.remove(clazz);
                    }))
                    .flatMap(nextEntity -> {

                        if (nextEntity == null) {
                            tracked.clear();
                            scheduledEntitySubscription.remove(clazz);
                            return Mono.error(new IllegalStateException("\nCustomExpiryRepository.class > scheduleNext() > nextEntity is null"));
                        }

                        ID nextEntityId = getEntityId(nextEntity, idType);
                        Instant nextExpiryDate = getEntityExpiryDate(nextEntity);

                        //IO.print("[FOUND] " + clazz.getSimpleName()
                        //        + " id=" + nextEntityId
                        //        + " expiry=" + nextExpiryDate);

                        long delayMs = Duration.between(Instant.now(), nextExpiryDate).toMillis();
                        if (delayMs < 0) delayMs = 0;

                        tracked.clear();
                        tracked.put(nextEntityId, nextExpiryDate);

                        //IO.print("[TRACK] updating map for " + clazz.getSimpleName()
                        //        + " -> " + tracked);

                        return Mono.delay(Duration.ofMillis(delayMs))
                                .then(Mono.defer(() -> {
                                    //IO.print("[EXECUTE] partialDelete for " + clazz.getSimpleName()
                                    //        + " id=" + nextEntityId);
                                    return service.partialDelete(nextEntity);
                                }))
                                //.then(Mono.delay(Duration.ofMillis(300)))   // allow DB commit visibility // i removed this cos add adding latency
                                .then(Mono.defer(() -> {
                                    //IO.print("[NEXT] rescheduling for " + clazz.getSimpleName());
                                    tracked.clear();
                                    scheduledEntitySubscription.remove(clazz);
                                    return scheduleNext(clazz, idType, service);
                                }));
                    })
                    .then()
                    .subscribeOn(Schedulers.parallel());

            //IO.print("[REGISTERING] scheduler for " + clazz.getSimpleName());
            //IO.print("Before activeSchedules = " + scheduledEntitySubscription.keySet());

            Disposable disposable = schedule.subscribe(
                    null,
                    error -> {
                        //IO.print("\nCustomExpiryRepository.class > scheduleNext() > Error: " + error.getMessage());
                        tracked.clear();
                        scheduledEntitySubscription.remove(clazz);
                    }
            );

            // Replace the placeholder with the real subscription
            Disposable old = scheduledEntitySubscription.put(clazz, disposable);

            // -------------------------------------------------
            // MULTIPLE SCHEDULER DETECTION (KEEP THIS)
            // -------------------------------------------------
            if (old != null && old != placeholder && !old.isDisposed()) { // this must never execute if everything is correct
                IO.print("\n CustomExpiryRepository.class > scheduleNext() > MULTIPLE SCHEDULERS detected for "
                        + clazz.getSimpleName() + " - disposing previous one.");
                old.dispose();
            }

            //IO.print("After activeSchedules = " + scheduledEntitySubscription.keySet());

            return Mono.empty();
        });
    }

    // ====================================================
    // HELPERS
    // ====================================================
    private ID getEntityId(T entity, Class<ID> idType) {
        try {
            Object id = entity.getClass().getMethod("getId").invoke(entity);
            if (id == null)
                throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityId() > Entity ID is null for: " + entity.getClass().getSimpleName());
            if (!idType.isInstance(id))
                throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityId() > ID type mismatch for " + entity.getClass().getSimpleName() +
                        ": expected " + idType.getName() + " but got " + id.getClass().getName());
            return idType.cast(id);
        } catch (IllegalAccessException | IllegalStateException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityId() > Failed to read ID from entity: " + entity.getClass().getName(), e);
        }
    }

    private Instant getEntityExpiryDate(T entity) {
        try {
            Object expiryDate = entity.getClass().getMethod("getExpiryDate").invoke(entity);
            if (expiryDate == null)
                throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityExpiryDate() > expiry is null for: " + entity.getClass().getSimpleName());
            if (!Instant.class.isInstance(expiryDate))
                throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityExpiryDate() > Expiry type mismatch for " + entity.getClass().getSimpleName() +
                        ": expected " + Instant.class.getName() + " but got " + expiryDate.getClass().getName());
            return Instant.class.cast(expiryDate);
        } catch (IllegalAccessException | IllegalStateException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException("\nCustomExpiryRepository.class > getEntityExpiryDate() > Failed to read expiry from entity: " + entity.getClass().getName(), e);
        }
    }

    private Mono<T> findNextExpiry(T entity) {
        String tableName = Util.getTableName(entity.getClass());
        String sql = """
            SELECT * FROM "%s" WHERE "expiryDate" IS NOT NULL ORDER BY "expiryDate" ASC LIMIT 1
        """.formatted(tableName);

        return databaseClient.sql(sql)
                .map((row, _) -> entityConversion.rowToEntity(row, entity))
                .one()
                .flatMap(e -> e == null ? Mono.empty() : Mono.just(e));
    }

    private T createEmptyInstance(Class<T> clazz) throws Exception {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new Exception("\nCustomExpiryRepository.class > findNextExpiry() > Cannot create instance for: " + clazz.getSimpleName(), e);
        }
    }

    public Mono<Void> start(Class<T> clazz, Class<ID> idType, CustomExpiryRepository<T> service) {
        return scheduleNext(clazz, idType, service);
    }
}
