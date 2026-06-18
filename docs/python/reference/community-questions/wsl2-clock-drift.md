---
title: Why do my ticking tables stall on WSL 2?
sidebar_label: Why do ticking tables stall on WSL 2?
---

_I'm running Deephaven on WSL 2. My ticking tables seem to stop ticking at random, and I sometimes see warnings like `System clock's jumped back by ~13 sec` from my IDE or `Time jumped backwards, rotating` in `journalctl`. What's going on?_

This is a structural quirk of how WSL 2 keeps time, not anything wrong with Deephaven or your distro. WSL 2 runs every distro inside a single Microsoft-supplied kernel that pushes a Windows-host time sample into the guest every five seconds via Hyper-V TimeSync. At the same time, the guest's own NTP daemon (`systemd-timesyncd` on Ubuntu 24.04 and earlier, `chrony` on 25.10+) disciplines the clock toward public NTP servers. The Windows host's `w32time` service polls `time.windows.com` once a week by default and is often off by several seconds. When the two authorities disagree, they take turns stepping the guest's wall clock and it yo-yos by 10–20 seconds. Anything that depends on a monotonically advancing clock — Deephaven's update graph, TLS, `apt`'s freshness checks, log timestamps — misbehaves.

You can stop the fight in either of two ways. Pick one.

## Option 1: disable NTP inside WSL (quickest)

Run inside your WSL 2 distro:

```bash
sudo timedatectl set-ntp false
```

The guest now inherits whatever time Windows thinks it is — typically within a few seconds of correct. Acceptable for development; not great if you care about cryptographic time or cross-host log correlation.

## Option 2: give Windows an accurate clock (recommended)

Tighten `w32time` on the Windows host so it tracks real NTP within milliseconds. Both authorities then agree and there is nothing to fight about. Run the following in an **elevated** PowerShell:

```powershell
w32tm /config /manualpeerlist:"time.cloudflare.com,0x9 time.nist.gov,0x9 pool.ntp.org,0x9" /syncfromflags:manual /update
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\NtpClient" -Name "SpecialPollInterval" -Value 3600 -Type DWord
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\W32Time\Config" -Name "MaxAllowedPhaseOffset" -Value 0 -Type DWord
Set-Service w32time -StartupType Automatic
Restart-Service w32time
w32tm /resync /rediscover
```

The `Set-Service ... -StartupType Automatic` line is required. By default `w32time` only runs for a few seconds per week (triggered by a scheduled task), so `SpecialPollInterval` has no effect unless the service stays running. The `Set-ItemProperty` calls must be run from an elevated shell, or they silently write to a per-user registry view and the change has no effect.

## Verify the fix

On the Windows host:

```powershell
w32tm /stripchart /computer:time.nist.gov /samples:5 /dataonly
```

You want single-digit-millisecond offsets and low jitter between samples.

Inside WSL, watch for new entries to stop accumulating:

```bash
journalctl -k | grep -i "time jumped"
```

## References

- [Time synchronization for Ubuntu on WSL](https://documentation.ubuntu.com/wsl/latest/explanation/time-sync/)
- [microsoft/WSL#12765 — `hv_utils.timesync_implicit=0` not honored](https://github.com/microsoft/WSL/issues/12765)
- [Windows Time Service technical reference](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
