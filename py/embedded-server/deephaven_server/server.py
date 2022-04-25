from . import start_jvm

# This is explicitly not a JObjectWrapper, as that would require importing deephaven and jpy
# before the JVM was running.
class Server:

    @property
    def j_object(self):
        return self.j_server

    # TODO don't allow re-starting the server
    def __init__(self, port:int = 8080, jvm_args:List[str] = [], dh_args: Dict[str, str] = {}):
        # given the jvm args, ensure that the jvm has started
        start_jvm(jvm_args=jvm_args)

        # it is now safe to import jpy
        import jpy

        # Start the engine if not already running, this can only happen once

        # create a wrapped java server that we can reference to talk to the platform
        self.j_server = jpy.get_type('io.deephaven.python.server.EmbeddedServer')(port, dh_args)

    def start_engine(self):
        self.j_server.startEngine()

    def start_grpc(self):
        self.j_server.startGrpc()


    def stop_grpc(self):
        self.j_server.stopGrpc()


    def start(self, console: bool = False):
