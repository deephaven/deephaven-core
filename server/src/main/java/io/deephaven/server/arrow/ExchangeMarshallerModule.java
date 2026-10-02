//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import dagger.BindsOptionalOf;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import org.jetbrains.annotations.NotNull;

import javax.inject.Named;
import java.util.*;
import java.util.function.Supplier;
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
 *
 * <p>
 * The marshallers are also given the {@code Supplier<JobScheduler>} bound as
 * {@value BarrageMessageProducer#PROPAGATION_JOB_SCHEDULER} when the component has one, and
 * {@link BarrageMessageProducer#SEQUENTIAL_PROPAGATION} when it does not.
 * </p>
 */
@Module(includes = ExchangeMarshallerModule.PropagationJobSchedulerModule.class)
public class ExchangeMarshallerModule {
    /**
     * Declares the propagation job scheduler optional, so that a component without one still builds; its producers then
     * write to their subscribers in turn.
     */
    @Module
    public interface PropagationJobSchedulerModule {
        @BindsOptionalOf
        @Named(BarrageMessageProducer.PROPAGATION_JOB_SCHEDULER)
        Supplier<JobScheduler> propagationJobScheduler();
    }

    /**
     * Multiple modules could have injected a marshaller, we must sort the complete list by priority.
     *
     * @param marshallers the input set of marshallers
     * @return the marshallers sorted in ascending priority.
     */
    @Provides
    public static List<ExchangeMarshaller> sortMarshallersByPriority(
            @NotNull final Set<ExchangeMarshaller> marshallers) {
        return marshallers.stream().sorted(Comparator.comparingInt(ExchangeMarshaller::priority))
                .collect(Collectors.toUnmodifiableList());
    }

    @Provides
    @ElementsIntoSet
    public static Set<ExchangeMarshaller> provideExchangeMarshallers(final Scheduler scheduler,
            final SessionService.ErrorTransformer errorTransformer,
            final BarrageMessageWriter.Factory streamGeneratorFactory,
            @Named(BarrageMessageProducer.PROPAGATION_JOB_SCHEDULER) final Optional<Supplier<JobScheduler>> propagationJobScheduler) {
        final Supplier<JobScheduler> propagationJobSchedulerFactory =
                propagationJobScheduler.orElse(BarrageMessageProducer.SEQUENTIAL_PROPAGATION);
        return ServiceLoader.load(ExchangeMarshallerModule.Factory.class)
                .stream()
                .map(factory -> factory.get().create(scheduler, errorTransformer, streamGeneratorFactory,
                        propagationJobSchedulerFactory))
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    /**
     * To add an additional {@link ExchangeMarshaller}, implement this Factory and add it as a service.
     */
    public interface Factory {
        ExchangeMarshaller create(final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory);

        /**
         * Creates the marshaller, given also the supplier of the job scheduler that its
         * {@link BarrageMessageProducer}s, if it makes any, should write to their subscribers on. The module calls this
         * one; the default ignores the supplier and calls
         * {@link #create(Scheduler, SessionService.ErrorTransformer, BarrageMessageWriter.Factory)}.
         *
         * @param propagationJobSchedulerFactory supplies the scheduler each propagation phase's writes to subscribers
         *        run on, in parallel when it can
         */
        default ExchangeMarshaller create(final Scheduler scheduler,
                final SessionService.ErrorTransformer errorTransformer,
                final BarrageMessageWriter.Factory streamGeneratorFactory,
                final Supplier<JobScheduler> propagationJobSchedulerFactory) {
            return create(scheduler, errorTransformer, streamGeneratorFactory);
        }
    }

    @Provides
    @ElementsIntoSet
    public static Set<ExchangeRequestHandlerFactory> provideRequestHandlers() {
        return ServiceLoader.load(ExchangeRequestHandlerFactory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }
}
