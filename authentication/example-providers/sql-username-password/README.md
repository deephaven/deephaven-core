# Simple PostgreSQL username/password
Many server software packages require a username and password to connect. Through this example provider, we can see how
such a feature could be added to Deephaven Core, by storing usernames and bcrypt-hashed passwords in a SQL database, 
so as to permit some other tool to manipulate that user store as needed. This is just an example, and while we've tried
to follow best practices in storing secure information like the password itself, the example is otherwise very
rudimentary.

## Example database setup
In this directory, run `docker compose up` to start a SQL database. The `init-users.sql` file will be used to initialize
this database, which has one user, named `admin`, with a password of `p@ssw0rd`. This docker compose configuration is
designed to be fast to start and ephemeral, and is not meant for production use.

After that has run, there will be a docker container running with port 5432 exposed on localhost. The password for the
postgres account is `password`.

## Server setup
To test this when building from gradle with `:server-jetty-app:run`, specify `-Psql-username-password` to enable this.

To use this from a pre-built installation:
* Add the sql-username-password-authentication-provider jar to your Deephaven installation's classpath:
    * If running the `bin/start` script, this can be accomplished by adding it to the `EXTRA_CLASSPATH` environment variable.
    * If running from docker, add the jar to the `/apps/lib/` directory.
* Configure the server to use this by specifying that the `AuthHandlers` configuration should include
  `io.deephaven.authentication.sql.BasicSqlAuthenticationHandler`. AuthHandlers is a comma-separated string, more than one
  can be specified to enable multiple authentication handlers.

## Testing from the browser
Connect to http://<server-hostname>/jsapi/authentication/basic.html (the sample username and password are already
populated) and click "Go" to test the connection.
