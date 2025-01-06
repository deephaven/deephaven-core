namespace Deephaven.ExcelAddIn.Util;

internal class VersionTracker {
  private readonly object _sync = new();
  private VersionTrackerCookie _cookie;

  public VersionTracker() {
    _cookie = new VersionTrackerCookie(this);
  }

  public VersionTrackerCookie SetNewVersion() {
    lock (_sync) {
      _cookie = new VersionTrackerCookie(this);
      return _cookie;
    }
  }

  public bool HasCookie(VersionTrackerCookie cookie) {
    lock (_sync) {
      return ReferenceEquals(_cookie, cookie);
    }
  }
}

internal class VersionTrackerCookie(VersionTracker owner) {
  public bool IsCurrent => owner.HasCookie(this);
}
