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

package org.springframework.data.repository.query;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.reactivestreams.Publisher;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.experimental.UtilityClass;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.Single;

/**
 * Utility class to expose availability of reactive wrapper types and checking which type supports single/multi-element
 * emission.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.reactivestreams.Publisher
 * @see rx.Single
 * @see rx.Observable
 * @see rx.Completable
 * @see io.reactivex.Single
 * @see io.reactivex.Maybe
 * @see io.reactivex.Observable
 * @see io.reactivex.Completable
 * @see io.reactivex.Flowable
 * @see Mono
 * @see Flux
 */
@UtilityClass
public class ReactiveWrappers {

	public static final boolean PUBLISHER_PRESENT = ClassUtils.isPresent("org.reactivestreams.Publisher",
			ReactiveWrappers.class.getClassLoader());

	public static final boolean PROJECT_REACTOR_PRESENT = ClassUtils.isPresent("reactor.core.publisher.Flux",
			ReactiveWrappers.class.getClassLoader());

	public static final boolean RXJAVA1_PRESENT = ClassUtils.isPresent("rx.Single",
			ReactiveWrappers.class.getClassLoader());

	public static final boolean RXJAVA2_PRESENT = ClassUtils.isPresent("io.reactivex.Single",
			ReactiveWrappers.class.getClassLoader());

	private static final Set<Class<?>> SINGLE_TYPES;
	private static final Set<Class<?>> MULTI_TYPES;

	static {

		Set<Class<?>> singleTypes = new LinkedHashSet<>();
		Set<Class<?>> multiTypes = new LinkedHashSet<>();

		if (RXJAVA1_PRESENT) {
			singleTypes.add(getRxJava1SingleClass());
			multiTypes.add(getRxJava1ObservableClass());
		}

		if (RXJAVA2_PRESENT) {
			singleTypes.add(getRxJava2SingleClass());
			multiTypes.add(getRxJava2ObservableClass());
			multiTypes.add(getRxJava2FlowableClass());
			multiTypes.add(getRxJava2MaybeClass());
		}

		if (PROJECT_REACTOR_PRESENT) {
			singleTypes.add(getReactorMonoClass());
			multiTypes.add(getReactorFluxClass());
		}

		if (PUBLISHER_PRESENT) {
			multiTypes.add(getReactorPublisherClass());
		}

		SINGLE_TYPES = Collections.unmodifiableSet(singleTypes);
		MULTI_TYPES = Collections.unmodifiableSet(multiTypes);
	}

	/**
	 * Returns {@literal true} if reactive support is available. More specifically, whether RxJava1/2 or Project Reactor
	 * libraries are on the class path.
	 * 
	 * @return {@literal true} if reactive support is available.
	 */
	public static boolean isAvailable() {
		return RXJAVA1_PRESENT || RXJAVA2_PRESENT || PROJECT_REACTOR_PRESENT;
	}

	/**
	 * Returns {@literal true} if the {@code type} is a supported reactive wrapper type.
	 * 
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if the {@code type} is a supported reactive wrapper type.
	 */
	public static boolean supports(Class<?> type) {
		return isSingleType(type) || isMultiType(type);
	}

	/**
	 * Returns {@literal true} if {@code type} is a reactive wrapper type for single element emission.
	 * 
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if {@code type} is a reactive wrapper type for single element emission
	 */
	public static boolean isSingleType(Class<?> type) {

		Assert.notNull(type, "Class type must not be null!");

		return isAssignable(SINGLE_TYPES, type);
	}

	/**
	 * Returns {@literal true} if {@code type} is a reactive wrapper type supporting emission of {@code 0..N} elements.
	 *
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if {@code type} is a reactive wrapper type supporting emission of {@code 0..N} elements.
	 */
	public static boolean isMultiType(Class<?> type) {

		Assert.notNull(type, "Class type must not be null!");

		// Prevent single-types with a multi-hierarchy supertype to be reported as multi type
		// See Mono implements Publisher
		if (isSingleType(type)) {
			return false;
		}

		return isAssignable(MULTI_TYPES, type);
	}

	private static boolean isAssignable(Iterable<Class<?>> lhsTypes, Class<?> rhsType) {

		for (Class<?> type : lhsTypes) {
			if (org.springframework.util.ClassUtils.isAssignable(type, rhsType)) {
				return true;
			}
		}

		return false;
	}

	private static Class<?> getRxJava1SingleClass() {
		return Single.class;
	}

	private static Class<?> getRxJava1ObservableClass() {
		return Observable.class;
	}

	private static Class<?> getRxJava2SingleClass() {
		return io.reactivex.Single.class;
	}

	private static Class<?> getRxJava2ObservableClass() {
		return io.reactivex.Observable.class;
	}

	private static Class<?> getRxJava2FlowableClass() {
		return io.reactivex.Flowable.class;
	}

	private static Class<?> getRxJava2MaybeClass() {
		return io.reactivex.Maybe.class;
	}

	private static Class<?> getReactorMonoClass() {
		return Mono.class;
	}

	private static Class<?> getReactorFluxClass() {
		return Flux.class;
	}

	private static Class<?> getReactorPublisherClass() {
		return Publisher.class;
	}
}
