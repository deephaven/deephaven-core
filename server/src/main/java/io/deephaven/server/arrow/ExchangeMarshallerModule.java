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

/**
 * A dagger module that provides {@link ExchangeMarshaller} s for use by the {@link ArrowFlightUtil}
 * {@code DoExchangeMarshaller}, loaded using a {@link ServiceLoader} constructed with the injected {@link Scheduler},
 * {@link io.deephaven.server.session.SessionService.ErrorTransformer} and {@link BarrageMessageWriter.Factory}
 * parameters.
 */
@Module
public class ExchangeMarshallerModule {
    @Provides
    @ElementsIntoSet
    public static Set<ExchangeMarshaller> provideExchangeMarshallers(final Scheduler scheduler,
            final SessionService.ErrorTransformer errorTransformer,
            final BarrageMessageWriter.Factory streamGeneratorFactory) {
        final Iterator<Factory> it = ServiceLoader.load(ExchangeMarshallerModule.Factory.class).iterator();
        if (!it.hasNext()) {
            return Collections.emptySet();
        }

        final List<ExchangeMarshaller> list = new ArrayList<>();

        while (it.hasNext()) {
            list.add(it.next().create(scheduler, errorTransformer, streamGeneratorFactory));
        }

        // Note that although we sort the marshallers by priority, the eventual user of the marshaller must sort the
        // complete set by priority.
        list.sort(Comparator.comparingInt(ExchangeMarshaller::priority));

        return Collections.unmodifiableSet(new LinkedHashSet<>(list));
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
