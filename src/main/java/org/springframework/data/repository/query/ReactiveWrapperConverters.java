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

import static org.springframework.data.repository.query.ReactiveWrappers.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.reactivex.Flowable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.Single;

/**
 * Conversion support for reactive wrapper types.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class ReactiveWrapperConverters {

	private static final List<AbstractReactiveWrapper<?>> REACTIVE_WRAPPERS = new ArrayList<>();
	private static final GenericConversionService GENERIC_CONVERSION_SERVICE = new GenericConversionService();

	static {

		if (RXJAVA1_PRESENT) {
			REACTIVE_WRAPPERS.add(RxJava1SingleWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(RxJava1ObservableWrapper.INSTANCE);
		}

		if (RXJAVA2_PRESENT) {
			REACTIVE_WRAPPERS.add(RxJava2SingleWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(RxJava2MaybeWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(RxJava2ObservableWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(RxJava2FlowableWrapper.INSTANCE);
		}

		if (PROJECT_REACTOR_PRESENT) {
			REACTIVE_WRAPPERS.add(FluxWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(MonoWrapper.INSTANCE);
			REACTIVE_WRAPPERS.add(PublisherWrapper.INSTANCE);
		}

		QueryExecutionConverters.registerConvertersIn(GENERIC_CONVERSION_SERVICE);
	}

	private ReactiveWrapperConverters() {

	}

	/**
	 * Returns whether the given type is a supported wrapper type.
	 * 
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if the {@code type} is a supported reactive wrapper type.
	 */
	public static boolean supports(Class<?> type) {
		return assignableStream(type).isPresent();
	}

	/**
	 * Returns whether the type is a single-like wrapper.
	 * 
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if the {@code type} is a single-like reactive wrapper type.
	 * @see rx.Single
	 * @see io.reactivex.Single
	 * @see Mono
	 */
	public static boolean isSingleLike(Class<?> type) {
		return ReactiveWrappers.isSingleType(type);
	}

	/**
	 * Returns whether the type is a collection/multi-element-like wrapper.
	 * 
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if the {@code type} is a collection/multi-element-like reactive wrapper type.
	 * @see rx.Observable
	 * @see io.reactivex.Observable
	 * @see io.reactivex.Flowable
	 * @see Flux
	 * @see Publisher
	 */
	public static boolean isCollectionLike(Class<?> type) {
		return ReactiveWrappers.isMultiType(type);
	}

	/**
	 * Casts or converts the given wrapper type into a different wrapper type.
	 * 
	 * @param stream the stream, must not be {@literal null}.
	 * @param expectedWrapperType must not be {@literal null}.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T toWrapper(Object stream, Class<? extends T> expectedWrapperType) {

		Assert.notNull(stream, "Stream must not be null!");
		Assert.notNull(expectedWrapperType, "Converter must not be null!");

		if (expectedWrapperType.isAssignableFrom(stream.getClass())) {
			return (T) stream;
		}

		return GENERIC_CONVERSION_SERVICE.convert(stream, expectedWrapperType);
	}

	/**
	 * Maps elements of a reactive element stream to other elements.
	 * 
	 * @param stream must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T map(Object stream, Converter<Object, Object> converter) {

		Assert.notNull(stream, "Stream must not be null!");
		Assert.notNull(converter, "Converter must not be null!");

		for (AbstractReactiveWrapper<?> reactiveWrapper : REACTIVE_WRAPPERS) {

			if (ClassUtils.isAssignable(reactiveWrapper.getWrapperClass(), stream.getClass())) {
				return (T) reactiveWrapper.map(stream, converter);
			}
		}

		throw new IllegalStateException(String.format("Cannot apply converter to %s", stream));
	}

	private static Optional<AbstractReactiveWrapper<?>> assignableStream(Class<?> type) {

		Assert.notNull(type, "Type must not be null!");

		return findWrapper(wrapper -> ClassUtils.isAssignable(wrapper.getWrapperClass(), type));
	}

	private static Optional<AbstractReactiveWrapper<?>> findWrapper(
			Predicate<? super AbstractReactiveWrapper<?>> predicate) {

		return REACTIVE_WRAPPERS.stream().filter(predicate).findFirst();
	}

	private interface AbstractReactiveWrapper<T> {

		Class<? super T> getWrapperClass();

		Object map(Object wrapper, Converter<Object, Object> converter);
	}

	private enum MonoWrapper implements AbstractReactiveWrapper<Mono<?>> {

		INSTANCE;

		@Override
		public Class<? super Mono<?>> getWrapperClass() {
			return Mono.class;
		}

		@Override
		public Mono<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((Mono<?>) wrapper).map(converter::convert);
		}
	}

	private enum FluxWrapper implements AbstractReactiveWrapper<Flux<?>> {

		INSTANCE;

		@Override
		public Class<? super Flux<?>> getWrapperClass() {
			return Flux.class;
		}

		public Flux<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((Flux<?>) wrapper).map(converter::convert);
		}
	}

	private enum PublisherWrapper implements AbstractReactiveWrapper<Publisher<?>> {

		INSTANCE;

		@Override
		public Class<? super Publisher<?>> getWrapperClass() {
			return Publisher.class;
		}

		@Override
		public Publisher<?> map(Object wrapper, Converter<Object, Object> converter) {

			if (wrapper instanceof Mono) {
				return MonoWrapper.INSTANCE.map((Mono<?>) wrapper, converter);
			}

			if (wrapper instanceof Flux) {
				return FluxWrapper.INSTANCE.map((Flux<?>) wrapper, converter);
			}

			return FluxWrapper.INSTANCE.map(Flux.from((Publisher<?>) wrapper), converter);
		}
	}

	private enum RxJava1SingleWrapper implements AbstractReactiveWrapper<Single<?>> {

		INSTANCE;

		@Override
		public Class<? super Single<?>> getWrapperClass() {
			return Single.class;
		}

		@Override
		public Single<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((Single<?>) wrapper).map(converter::convert);
		}
	}

	private enum RxJava1ObservableWrapper implements AbstractReactiveWrapper<Observable<?>> {

		INSTANCE;

		@Override
		public Class<? super Observable<?>> getWrapperClass() {
			return Observable.class;
		}

		@Override
		public Observable<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((Observable<?>) wrapper).map(converter::convert);
		}
	}

	private enum RxJava2SingleWrapper implements AbstractReactiveWrapper<io.reactivex.Single<?>> {

		INSTANCE;

		@Override
		public Class<? super io.reactivex.Single<?>> getWrapperClass() {
			return io.reactivex.Single.class;
		}

		@Override
		public io.reactivex.Single<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((io.reactivex.Single<?>) wrapper).map(converter::convert);
		}
	}

	private enum RxJava2MaybeWrapper implements AbstractReactiveWrapper<io.reactivex.Maybe<?>> {

		INSTANCE;

		@Override
		public Class<? super io.reactivex.Maybe<?>> getWrapperClass() {
			return io.reactivex.Maybe.class;
		}

		@Override
		public io.reactivex.Maybe<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((io.reactivex.Maybe<?>) wrapper).map(converter::convert);
		}
	}

	private enum RxJava2ObservableWrapper implements AbstractReactiveWrapper<io.reactivex.Observable<?>> {

		INSTANCE;

		@Override
		public Class<? super io.reactivex.Observable<?>> getWrapperClass() {
			return io.reactivex.Observable.class;
		}

		@Override
		public io.reactivex.Observable<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((io.reactivex.Observable<?>) wrapper).map(converter::convert);
		}
	}

	private enum RxJava2FlowableWrapper implements AbstractReactiveWrapper<io.reactivex.Flowable<?>> {

		INSTANCE;

		@Override
		public Class<? super Flowable<?>> getWrapperClass() {
			return io.reactivex.Flowable.class;
		}

		@Override
		public io.reactivex.Flowable<?> map(Object wrapper, Converter<Object, Object> converter) {
			return ((io.reactivex.Flowable<?>) wrapper).map(converter::convert);
		}
	}

}
