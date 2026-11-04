package com.trademarketx.springwebflux.commons.schedules.publication.repository;
import reactor.core.publisher.Mono;
public interface CustomScheduleRepository<T> {
    Mono<Void> publish(T entity);
}