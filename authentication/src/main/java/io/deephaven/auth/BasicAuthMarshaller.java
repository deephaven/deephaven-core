package io.deephaven.auth;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.impl.Flight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import static com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED;

/**
 * Manually decode the payload as a BasicAuth message, confirm that only tags 2 and 3 are present as strings, otherwise
 * pass. This is stricter than a usual protobuf decode, under the assumption that FlightClient will always only write
 * those two fields, and user code couldn't customize the payload further to repeatedly write those fields or any other
 * field.
 *
 * Despite being stricter than a standard protobuf decode, this is also very generic and might accidentally match the
 * wrong message type. For this reason, this handler should not run until other more selective handlers have finished.
 *
 * This class delegates to a typed auth handler once it is certain that the payload appears to be a BasicAuth value.
 */
public class BasicAuthMarshaller implements AuthenticationRequestHandler {
    private final static Logger log = LoggerFactory.getLogger(AnonymousAuthenticationHandler.class);

    public interface Handler {
        Optional<AuthContext> login(String username, String password) throws AuthenticationException;
    }

    private final Handler handler;

    public BasicAuthMarshaller(Handler handler) {
        this.handler = handler;
    }

    @Override
    public String getAuthType() {
        return Auth2Constants.BASIC_PREFIX.trim();
    }

    @Override
    public void initialize(String targetUrl) {
        for (int ii = 0; ii < 5; ++ii) {
            log.warn().endl();
        }
        log.warn().append("================================================================================").endl();
        log.warn().append("Basic Authentication is enabled.").endl();
        log.warn().append("       Listening on ").append(targetUrl).endl();
        log.warn().append("================================================================================").endl();
        for (int ii = 0; ii < 5; ++ii) {
            log.warn().endl();
        }
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException {
        CodedInputStream inputStream = CodedInputStream.newInstance(payload);

        String username = null, password = null;

        try {
            while (!inputStream.isAtEnd()) {
                int tag = inputStream.readTag();
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case Flight.BasicAuth.USERNAME_FIELD_NUMBER: {
                        if (username == null && WireFormat.getTagWireType(tag) == WIRETYPE_LENGTH_DELIMITED) {
                            username = inputStream.readString();
                        } else {
                            return Optional.empty();
                        }
                        break;
                    }
                    case Flight.BasicAuth.PASSWORD_FIELD_NUMBER: {
                        if (password == null && WireFormat.getTagWireType(tag) == WIRETYPE_LENGTH_DELIMITED) {
                            password = inputStream.readString();
                        } else {
                            return Optional.empty();
                        }
                        break;
                    }
                    default:
                        // Found an unexpected field; this is not a BasicAuth request.
                        return Optional.empty();
                }
            }
        } catch (IOException e) {
            return Optional.empty();
        }
        if (username != null && password != null) {
            // This is likely to be an un-wrapped BasicAuth instance, attempt to read it and login with it
            return handler.login(username, password);
        }

        return Optional.empty();
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException {
        // The value has the format Base64(<username>:<password>)
        final String authDecoded = new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
        final int colonPos = authDecoded.indexOf(':');
        if (colonPos == -1) {
            return Optional.empty();
        }

        final String username = authDecoded.substring(0, colonPos);
        final String password = authDecoded.substring(colonPos + 1);
        return handler.login(username, password);
    }
}
