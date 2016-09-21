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
import java.util.HashSet;
import java.util.Set;

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
 * @see Single
 * @see Observable
 * @see Mono
 * @see Flux
 */
@UtilityClass
public class ReactiveWrappers {

	public static final boolean PROJECT_REACTOR_PRESENT = ClassUtils.isPresent("reactor.core.publisher.Flux",
			ReactiveWrappers.class.getClassLoader());

	public static final boolean RXJAVA1_PRESENT = ClassUtils.isPresent("rx.Single",
			ReactiveWrappers.class.getClassLoader());

	private static final Set<Class<?>> SINGLE_TYPES;
	private static final Set<Class<?>> MULTI_TYPES;

	static {

		Set<Class<?>> singleTypes = new HashSet<>();
		Set<Class<?>> multiTypes = new HashSet<>();

		if (RXJAVA1_PRESENT) {
			singleTypes.add(getRxJava1SingleClass());
			multiTypes.add(getRxJava1ObservableClass());
		}

		if (PROJECT_REACTOR_PRESENT) {
			singleTypes.add(getReactorMonoClass());
			multiTypes.add(getReactorFluxClass());
		}

		SINGLE_TYPES = Collections.unmodifiableSet(singleTypes);
		MULTI_TYPES = Collections.unmodifiableSet(multiTypes);
	}

	/**
	 * Returns {@literal true} if {@code theClass} is a reactive wrapper type for single element emission.
	 * 
	 * @param theClass must not be {@literal null}.
	 * @return {@literal true} if {@code theClass} is a reactive wrapper type for single element emission
	 */
	public static boolean isSingleType(Class<?> theClass) {

		Assert.notNull(theClass, "Class type must not be null!");

		return isAssignable(SINGLE_TYPES, theClass);
	}

	/**
	 * Returns {@literal true} if {@code theClass} is a reactive wrapper type supporting emission of {@code 0..N}
	 * elements.
	 *
	 * @param theClass must not be {@literal null}.
	 * @return {@literal true} if {@code theClass} is a reactive wrapper type supporting emission of {@code 0..N}
	 *         elements.
	 */
	public static boolean isMultiType(Class<?> theClass) {

		Assert.notNull(theClass, "Class type must not be null!");

		return isAssignable(MULTI_TYPES, theClass);
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

	private static Class<?> getReactorMonoClass() {
		return Mono.class;
	}

	private static Class<?> getReactorFluxClass() {
		return Flux.class;
	}
}
