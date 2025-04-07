//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A dagger module that provides {@link ExchangeMarshaller exchange marshallers} and
 * {@link io.deephaven.server.arrow.ArrowFlightUtil.DoExchangeMarshaller.Handler handlers} for use by the
 * {@link ArrowFlightUtil} {@code DoExchangeMarshaller}, loaded using a {@link ServiceLoader} constructed with the
 * injected {@link Scheduler}, {@link io.deephaven.server.session.SessionService.ErrorTransformer} and
 * {@link BarrageMessageWriter.Factory} parameters.
 *
 * <p>
 * Note, the user of the ExchangeMarshaller set must sort the marshallers according to priority. The set cannot be
 * sorted at our injection point, because there may be multiple @ElementsIntoSet injectors.
 * </p>
 */
@Module
public class ExchangeMarshallerModule {
    @Provides
    @ElementsIntoSet
    public static Set<ExchangeMarshaller> provideExchangeMarshallers(final Scheduler scheduler,
            final SessionService.ErrorTransformer errorTransformer,
            final BarrageMessageWriter.Factory streamGeneratorFactory) {
        return ServiceLoader.load(ExchangeMarshallerModule.Factory.class)
                .stream()
                .map(factory -> factory.get().create(scheduler, errorTransformer, streamGeneratorFactory))
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    /**
     * To add an additional {@link ExchangeMarshaller}, implement this Factory and add it as a service.
     */
    public interface Factory {
        ExchangeMarshaller create(final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory);
    }

    @Provides
    @ElementsIntoSet
    public static Set<ExchangeRequestHandlerFactory> provideRequestHandlers() {
        final Iterator<ExchangeRequestHandlerFactory> it =
                ServiceLoader.load(ExchangeRequestHandlerFactory.class).iterator();
        if (!it.hasNext()) {
            return Collections.emptySet();
        }

        final List<ExchangeRequestHandlerFactory> list = new ArrayList<>();

        while (it.hasNext()) {
            list.add(it.next());
        }

        return Collections.unmodifiableSet(new HashSet<>(list));
    }
}
