package io.deephaven.demo.deploy;

import io.deephaven.demo.NameGen;
import io.vertx.core.impl.ConcurrentHashSet;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * MachinePool:
 * <p>
 * <p>
 * <p> A class containing information about all running machines.
 * <p>
 */
public class MachinePool {

    private static final Logger LOG = Logger.getLogger(MachinePool.class);

    private final Set<Machine> machines = new ConcurrentHashSet<>();

    public Machine createMachine(final GoogleDeploymentManager manager, final String name, final IpMapping ip) {
        final String newName = name == null || name.isEmpty() ? NameGen.newName() : name;
        final Machine machine = new Machine(newName);
        if (ip != null) {
            // important: setting the --address ip-name will create a machine with a stable IP address across restarts
            //            settings the --address 1.2.3.4 to an IP will a) fail b/c IP is used by our gcloud address name, and b) change on restart
            machine.setIp(ip.getName());
        }
        machines.add(machine);
        try {
            manager.createMachine(machine);
            machine.setIp(ip.getIp());
        } catch (IOException | InterruptedException e) {
            String msg = "Failed to create machine " + name;
            System.err.println(msg);
            throw new RuntimeException(msg, e);
        }
        return machine;
    }

    public void addMachine(final Machine machine) {
        machines.add(machine);
    }

    public Optional<Machine> maybeGetMachine(final GoogleDeploymentManager manager) {
        Machine candidate = null;
        synchronized (machines) {
            for (final Iterator<Machine> itr = machines.iterator(); itr.hasNext(); ) {
                Machine next = itr.next();
                if (!next.isInUse()) {
                    if (next.isOnline()) {
                        LOG.info("Sending user already-warm machine " + next);
                        next.setInUse(true);
                        return Optional.of(next);
                    }
                    candidate = next;
                }
            }
        }
        if (candidate != null) {
            candidate.setInUse(true);
            LOG.warn("Sending user a machine we must turn on: " + candidate);
            try {
                if (!candidate.isOnline()) {
                    candidate.setOnline(true);
                    manager.turnOn(candidate);
                }
            } catch (IOException | InterruptedException e) {
                String msg = "Unable to turn on machine " + candidate;
                LOG.error(msg, e);
                throw new IllegalStateException(msg, e);
            }
        }
        return Optional.ofNullable(candidate);
    }

    public Stream<Machine> getAllMachines() {
        return machines.parallelStream();
    }

    public void remove(final Machine machine) {
        machines.remove(machine);
    }

    public boolean needsMoreMachines(final int poolBuffer, final int poolSize) {
        int unused = 0, total = 0;
        for (Machine machine : machines) {
            total++;
            if (!machine.isInUse() && machine.isOnline()) {
                unused++;
            }
        }
        return unused < poolBuffer || total < poolSize;
    }

    public int getNumberMachines() {
        return machines.size();
    }
}
