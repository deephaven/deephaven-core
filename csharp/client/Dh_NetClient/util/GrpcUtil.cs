//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using System.Security.Cryptography.X509Certificates;

namespace Deephaven.Dh_NetClient;

public static class GrpcUtil {
  public static GrpcChannel CreateChannel(string target, ClientOptions clientOptions) {
    var channelOptions = GrpcUtil.MakeChannelOptions(clientOptions);
    var address = GrpcUtil.MakeAddress(clientOptions, target);

    var channel = GrpcChannel.ForAddress(address, channelOptions);
    return channel;
  }

  public static GrpcChannelOptions MakeChannelOptions(ClientOptions clientOptions) {
    var channelOptions = new GrpcChannelOptions();

    if (!clientOptions.UseTls && !clientOptions.TlsRootCerts.IsEmpty()) {
      throw new Exception("GrpcUtil.MakeChannelOptions: UseTls is false but pem provided");
    }

    if (clientOptions.TlsRootCerts.IsEmpty() || clientOptions.OverrideAuthority == null) {
      return channelOptions;
    }

    var handler = new HttpClientHandler();
    handler.ServerCertificateCustomValidationCallback = (_, cert, _, errors) => {
      if (errors == SslPolicyErrors.None) {
        return true;
      }

      if (cert == null) {
        return false;
      }

      var subjectName = cert.GetNameInfo(X509NameType.SimpleName, false);
      if (subjectName != clientOptions.OverrideAuthority) {
        return false;
      }

      var certColl = new X509Certificate2Collection();
      certColl.ImportFromPem(clientOptions.TlsRootCerts);

      var chain = new X509Chain();
      var chainPol = chain.ChainPolicy;
      chainPol.TrustMode = X509ChainTrustMode.CustomRootTrust;
      chainPol.RevocationMode = X509RevocationMode.Online;
      chainPol.UrlRetrievalTimeout = new TimeSpan(0, 0, 30);
      chainPol.VerificationFlags = X509VerificationFlags.NoFlag;

      for (var i = 0; i != certColl.Count; ++i) {
        chainPol.CustomTrustStore.Add(certColl[i]);
      }

      try {
        return chain.Build(cert);
      } catch (Exception) {
        return false;
      }
    };

    channelOptions.HttpHandler = handler;
    return channelOptions;
  }

  public static string MakeAddress(ClientOptions clientOptions, string target) {
    return (clientOptions.UseTls ? "https://" : "http://") + target;
  }

  private static ChannelCredentials GetCredentials(
    bool useTls,
    string tlsRootCerts,
    string clientRootChain,
    string clientPrivateKey) {
    if (!useTls) {
      return ChannelCredentials.Insecure;
    }

    var certPair = new KeyCertificatePair(clientRootChain, clientPrivateKey);
    return new SslCredentials(tlsRootCerts, certPair);
  }
}
