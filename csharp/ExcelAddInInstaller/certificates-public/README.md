The deephaven.cer file contains Deephaven's public key. Like all .cer files
it contains only a public key (and not a private key). It therefore does not
contain any secrets and does not need to be protected.

This file can be obtained by exporting the certificate from an installer
built by Advanced Installer (yes, that is a self-referential process), or
by exporting it from the Windows Certificate manager (certmgr.msc).

We store this file here so that (with the user's permission) Advanced Installer
can install the Deephaven public key in the local user's "Trusted Publishers",
which in turn allows Excel to trust the Deephaven Excel AddIn.
