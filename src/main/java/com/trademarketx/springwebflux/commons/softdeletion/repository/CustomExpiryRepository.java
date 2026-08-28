package com.trademarketx.springwebflux.commons.softdeletion.repository;

import reactor.core.publisher.Mono;

public interface CustomExpiryRepository<T> {
    Mono<Void> partialDelete(T entity);
}

