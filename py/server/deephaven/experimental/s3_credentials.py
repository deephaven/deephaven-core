#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy

from deephaven._wrapper import JObjectWrapper

_JCredentials = jpy.get_type("io.deephaven.extensions.s3.Credentials")


class S3Credentials(JObjectWrapper):
    """
    Credentials object for authenticating with S3 server.
    """
    j_object_type = _JCredentials

    def __init__(self, _j_object: jpy.JType):
        """
        Initializes the credentials object.

        Args:
            _j_object (S3Credentials): the Java credentials object.
        """
        self._j_object = _j_object

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object

    @classmethod
    def resolving(cls):
        """
        Default credentials provider used by Deephaven which resolves credentials in the following order:
        1. If a profile name, config file path, or credentials file path is provided, use ProfileCredentialsProvider.
        Ref: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/ProfileCredentialsProvider.html

        2. If not, check all places mentioned in DefaultCredentialsProvider and fall back to AnonymousCredentialsProvider.
        Ref: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html
        Ref: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AnonymousCredentialsProvider.html

        Returns:
            S3Credentials: the credentials object.
        """
        return cls(_JCredentials.resolving())

    @classmethod
    def default(cls):
        """
        Default credentials provider used by the AWS SDK that looks for credentials at a number of locations as
        described in DefaultCredentialsProvider.
        Ref: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html

        Returns:
            S3Credentials: the credentials object.
        """
        return cls(_JCredentials.defaultCredentials())

    @classmethod
    def basic(cls, access_key_id: str, secret_access_key: str):
        """
        Basic credentials with the specified access key id and secret access key.

        Args:
            access_key_id (str): the access key id, used to identify the user.
            secret_access_key (str): the secret access key, used to authenticate the user.

        Returns:
            S3Credentials: the credentials object.
        """
        return cls(_JCredentials.basic(access_key_id, secret_access_key))

    @classmethod
    def anonymous(cls):
        """
        Creates a new anonymous credentials object.
        Ref: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AnonymousCredentialsProvider.html

        Returns:
            S3Credentials: the credentials object.
        """
        return cls(_JCredentials.anonymous())

    @classmethod
    def profile(cls):
        """
        Profile specific credentials that uses configuration and credentials files.
        Ref: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/ProfileCredentialsProvider.html

        Returns:
            S3Credentials: the credentials object.
        """
        return cls(_JCredentials.profile())
