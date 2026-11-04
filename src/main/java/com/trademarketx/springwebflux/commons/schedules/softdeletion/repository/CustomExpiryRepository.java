package com.trademarketx.springwebflux.commons.schedules.softdeletion.repository;
import reactor.core.publisher.Mono;
public interface CustomExpiryRepository<T> {
    Mono<Void> partialDelete(T entity);
}

