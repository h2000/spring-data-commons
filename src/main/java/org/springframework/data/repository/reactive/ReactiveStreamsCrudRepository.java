/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.repository.reactive;

import java.io.Serializable;

import org.reactivestreams.Publisher;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;

/**
 * Interface for generic CRUD operations on a repository for a specific type. This repository follows reactive paradigms
 * and uses Reactive Streams types.
 *
 * @author Mark Paluch
 * @see Publisher
 */
@NoRepositoryBean
public interface ReactiveStreamsCrudRepository<T, ID extends Serializable> extends Repository<T, ID> {

	/**
	 * Saves a given entity. Use the returned instance for further operations as the save operation might have changed the
	 * entity instance completely.
	 *
	 * @param entity
	 * @return the saved entity
	 */
	<S extends T> Publisher<S> save(S entity);

	/**
	 * Saves all given entities.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entities
	 * @throws IllegalArgumentException in case the given entity is {@literal null}.
	 */
	<S extends T> Publisher<S> save(Iterable<S> entities);

	/**
	 * Saves all given entities.
	 *
	 * @param entityStream must not be {@literal null}.
	 * @return the saved entities
	 * @throws IllegalArgumentException in case the given {@code Publisher} is {@literal null}.
	 */
	<S extends T> Publisher<S> save(Publisher<S> entityStream);

	/**
	 * Retrieves an entity by its id.
	 *
	 * @param id must not be {@literal null}.
	 * @return the entity with the given id or {@literal null} if none found
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Publisher<T> findOne(ID id);

	/**
	 * Retrieves an entity by its id supplied by a {@link Publisher}.
	 *
	 * @param id must not be {@literal null}.
	 * @return the entity with the given id or {@literal null} if none found
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Publisher<T> findOne(Publisher<ID> id);

	/**
	 * Returns whether an entity with the given id exists.
	 *
	 * @param id must not be {@literal null}.
	 * @return true if an entity with the given id exists, {@literal false} otherwise
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Publisher<Boolean> exists(ID id);

	/**
	 * Returns whether an entity with the given id exists supplied by a {@link Publisher}.
	 *
	 * @param id must not be {@literal null}.
	 * @return true if an entity with the given id exists, {@literal false} otherwise
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Publisher<Boolean> exists(Publisher<ID> id);

	/**
	 * Returns all instances of the type.
	 *
	 * @return all entities
	 */
	Publisher<T> findAll();

	/**
	 * Returns all instances of the type with the given IDs.
	 *
	 * @param ids
	 * @return
	 */
	Publisher<T> findAll(Iterable<ID> ids);

	/**
	 * Returns all instances of the type with the given IDs.
	 *
	 * @param idStream
	 * @return
	 */
	Publisher<T> findAll(Publisher<ID> idStream);

	/**
	 * Returns the number of entities available.
	 *
	 * @return the number of entities
	 */
	Publisher<Long> count();

	/**
	 * Deletes the entity with the given id.
	 *
	 * @param id must not be {@literal null}.
	 * @throws IllegalArgumentException in case the given {@code id} is {@literal null}
	 */
	Publisher<Void> delete(ID id);

	/**
	 * Deletes a given entity.
	 *
	 * @param entity
	 * @throws IllegalArgumentException in case the given entity is {@literal null}.
	 */
	Publisher<Void> delete(T entity);

	/**
	 * Deletes the given entities.
	 *
	 * @param entities
	 * @throws IllegalArgumentException in case the given {@link Iterable} is {@literal null}.
	 */
	Publisher<Void> delete(Iterable<? extends T> entities);

	/**
	 * Deletes the given entities.
	 *
	 * @param entityStream
	 * @throws IllegalArgumentException in case the given {@link Publisher} is {@literal null}.
	 */
	Publisher<Void> delete(Publisher<? extends T> entityStream);

	/**
	 * Deletes all entities managed by the repository.
	 */
	Publisher<Void> deleteAll();
}
