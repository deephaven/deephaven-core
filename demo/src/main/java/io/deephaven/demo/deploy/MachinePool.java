package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import io.deephaven.demo.NameGen;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

/**
 * MachinePool:
 * <p>
 * <p>
 * <p> A class containing information about all running machines.
 * <p>
 */
public class MachinePool {

    // NOTE: this is dangerous! we could lose an item if we don't remove before changing expiry and put back after.
    // thus, it's private, you shouldn't use this.
    private static final Comparator<Machine> CMP = (a, b) -> {
        if (a.getExpiry() == b.getExpiry()) {
            return a.getHost().compareTo(b.getHost());
        }
        // this stops being correct if workers are more than 42 days old.
        long diff = a.getExpiry() - b.getExpiry();
        // take the oldest last!
        return diff > 0 ? -1 : diff < 0 ? 1 : 0;
    };

    private static final Logger LOG = Logger.getLogger(MachinePool.class);

    private final Map<String, Machine> machinesByName = new ConcurrentHashMap<>();
    private final Set<Machine> machines = new ConcurrentSkipListSet<>(CMP);

    public Machine createMachine(final ClusterController ctrl, final String name) {
        GoogleDeploymentManager manager = ctrl.getDeploymentManager();
        final IpPool ips = manager.getIpPool();
        final String newName = name == null || name.isEmpty() ? NameGen.newName() : name;
        final Machine machine = getOrCreate(newName, ctrl, null);
        if (machine.getIp() == null) {
            machine.setIp(ips.reserveIp(ctrl, machine));
        }
        try {
            manager.createMachine(machine, ips);
            machines.add(machine);
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
        List<Machine> candidates = new ArrayList<>();
        synchronized (machines) {
            for (Machine next : machines) {
                if (!next.isInUse()) {
                    if (next.isOnline()) {
                        LOG.info("Sending user already-warm machine " + next);
                        next.setInUse(true);
                        return Optional.of(next);
                    }
                    candidates.add(next);
                }
            }
            while (!candidates.isEmpty()) {
                final Machine candidate = candidates.remove(candidates.size() - 1);
                candidate.setInUse(true);
                // hm... if synchronized is not enough,
                // here is where we should try to claim a candidate through a contention-breaking set-label operation...
                // we don't _really_ want that, though, since there is code that calls us which does not want to publicly reserve a machine.

                LOG.warn("Sending user a machine we must turn on: " + candidate);
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    public Stream<Machine> getAllMachines() {
        return machines.parallelStream();
    }

    public void removeMachine(final Machine machine) {
        machines.remove(machine);
        machinesByName.remove(machine.getHost());
    }

    public boolean needsMoreMachines(final int poolBuffer, final int poolSize, final int maxPoolSize) {
        if (machines.size() >= maxPoolSize) {
            return false;
        }
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

    public Machine findByName(final String name) {
        return machinesByName.get(name);
    }
    public Machine getOrCreate(final String name, final ClusterController ctrl, final IpMapping ip) {
        return machinesByName.computeIfAbsent(name, missing-> {
            final Machine machine = new Machine(missing, ip == null ? ctrl.requestIp() : ip);
            machines.add(machine);
            final IpPool ips = ctrl.getDeploymentManager().getIpPool();
            ips.reserveIp(ctrl, machine);
            return machine;
        });
    }

    public void expireInMillis(final Machine machine, final long sessionTtl) {
        // we remove a machine from this set before updating the expiry,
        // and then put it back after, because our comparator looks at expiry, so we don't want to lose items!
        machines.remove(machine);
        machine.setExpiry(System.currentTimeMillis() + sessionTtl);
        machine.setInUse(machine.isOnline() && System.currentTimeMillis() < machine.getExpiry());
        machines.add(machine);
    }

    public void expireInTimeString(final Machine machine, final String lease) {
        final long parsedTime = ClusterController.parseTime(lease);
        if (parsedTime < machine.getExpiry()) {
            // machine was taken while we were loading metadata. ignore!
            return;
        }
        expireInMillis(machine, parsedTime - System.currentTimeMillis());
    }
}
