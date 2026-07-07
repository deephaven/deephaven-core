---
title: Why do my ticking tables stall on WSL 2?
sidebar_label: Why do ticking tables stall on WSL 2?
---

_I'm running Deephaven on WSL 2. My [ticking tables](../../conceptual/table-types.md) seem to stop ticking at random, and I sometimes see warnings like `System clock's jumped back by ~13 sec` from my IDE or `Time jumped backwards, rotating` in `journalctl`. What's going on?_

This is a structural quirk of how WSL 2 keeps time, not anything wrong with Deephaven or your distro. WSL 2 runs every distro inside a single Microsoft-supplied kernel that pushes a Windows-host time sample into the guest every five seconds via Hyper-V TimeSync. At the same time, the guest's own NTP daemon (`systemd-timesyncd` on Ubuntu 24.04 and earlier, `chrony` on 25.10+) disciplines the clock toward public NTP servers. The Windows host's `w32time` service polls `time.windows.com` once a week by default and is often off by several seconds. When the two authorities disagree, they take turns stepping the guest's wall clock and it yo-yos by 10–20 seconds. Anything that depends on a monotonically advancing clock — Deephaven's [update graph](../../conceptual/dag.md), TLS, `apt`'s freshness checks, log timestamps — misbehaves.

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

The `MaxAllowedPhaseOffset = 0` line tells `w32time` to **step** the clock immediately on any offset rather than **slew** it gradually. The default of 1 second sounds reasonable, but `w32time`'s default slew rate is so slow that the local oscillator's natural drift (~10 ppm) competes with it — you can end up in a stable state where `w32time` reports "successfully synced" while the clock sits 700 ms off NIST indefinitely. Setting `MaxAllowedPhaseOffset = 0` ("always step, never slew") is what you want for a workstation. It does not cause the clock to whipsaw on noisy samples: `w32time` rejects obviously-bad samples via separate spike detection (`LargePhaseOffset`, default 5 s, and `HoldPeriod`) before they reach the step logic.

The `Set-Service ... -StartupType Automatic` line is required. By default `w32time` only runs for a few seconds per week (triggered by a scheduled task), so `SpecialPollInterval` has no effect unless the service stays running. The `Set-ItemProperty` calls must be run from an elevated shell, or they silently write to a per-user registry view and the change has no effect.

## Verify the fix

On the Windows host, measure the actual offset to a stratum-1 NTP server:

```powershell
w32tm /stripchart /computer:time.nist.gov /samples:5 /dataonly
```

You want single-digit-millisecond offsets and low jitter between samples. Healthy output looks like this:

```text
22:38:01, +00.0017001s
22:38:03, +00.0019795s
22:38:05, +00.0022397s
22:38:07, +00.0017654s
22:38:09, +00.0020052s
```

(~2 ms off NIST and ~0.5 ms jitter is gold-standard for software NTP over the public internet.)

Three other `w32tm` commands are useful for diagnosis:

```powershell
w32tm /query /status         # Stratum, source, last sync time
w32tm /query /configuration  # Effective settings
w32tm /query /peers          # Per-peer reachability
```

In `/status`, you want `Stratum: 2` (one hop from atomic), a recent `Last Successful Sync Time`, and a `Source` matching one of your configured servers — not `Local CMOS Clock` or `Free-running System Clock`.

In `/configuration`, check the `NtpClient` section for `SpecialPollInterval: 3600` and the `NtpServer:` line listing all three peers with `,0x9`.

Inside WSL 2:

```bash
timedatectl
date                                  # eyeball comparison to Windows
journalctl -k | grep -i "time jumped"
```

If you chose Option 1, `NTP service: inactive` is correct. If you chose Option 2 and left `systemd-timesyncd` running, `NTP service: active` and `System clock synchronized: yes` is what you want. Either way, new `time jumped` log entries should stop accumulating.

## Things that look broken but aren't

A few `w32time` quirks that confuse people:

- **`Poll Interval: 10 (1024s)` in `/status` even though you set `SpecialPollInterval=3600`.** The `/status` display surfaces `MinPollInterval`, not your effective polling cadence. Confirm with `w32tm /query /configuration` — if `SpecialPollInterval: 3600` is there, you're polling hourly regardless of what `/status` says. You can also confirm empirically by watching `Last Successful Sync Time` advance in roughly 3600-second steps.
- **`Root Dispersion` starts at several seconds.** That's `w32time`'s pessimistic uncertainty estimate, not your actual offset. It settles to low single-digit seconds over a day. Your real offset is what `/stripchart` reports.
- **No `Phase Offset` line in `/status` output.** It's not consistently present across Windows builds. Use `/stripchart` for direct measurement.
- **`Windows version: 10.0.26200.xxxx`** doesn't mean Windows 10. The NT kernel version was frozen at 10.0 from Windows 10 onward — Windows 11 is identified by build number (22000 and up). The "10.0" is a legacy artifact, not a misdetection.

## References

- [Time synchronization for Ubuntu on WSL](https://documentation.ubuntu.com/wsl/latest/explanation/time-sync/)
- [microsoft/WSL#12765 — `hv_utils.timesync_implicit=0` not honored](https://github.com/microsoft/WSL/issues/12765)
- [Windows Time Service technical reference](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
