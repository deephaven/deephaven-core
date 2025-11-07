# Code Block Snapshotter

All `python` and `groovy` code blocks in the documentation are run and the output is saved as a snapshot. This allows us to ensure that the code examples are up-to-date and working correctly. The most up-to-date documentation on the tool is [here](https://github.com/deephaven/salmon/tree/main/tools/snapshotter#snapshotter-tool).

You can run the snapshotter tool against the latest published Deephaven Core release, a Docker tag, or a local build of Deephaven Core. If you are documenting a new feature, you will need to run against the local build.

To run the snapshotter tool, use the following command:

```
./docs/updateSnapshots
./docs/updateSnapshots -t local # to use a local build of Deephaven Core
./docs/updateSnapshots -t local -a # Run all against local build. Very rarely needed. Takes 30+ minutes.
```

> [!NOTE]
> Snapshots of tables will be limited to the first 100 rows and plots to an equally spaced 1000 points (based on index) to ensure the files are not too large.

There are some meta tags that can be used to control the behavior of the snapshotter tool. These tags are added to the code block after the language. Full details are in the [snapshotter tool documentation](https://github.com/deephaven/salmon/tree/main/tools/snapshotter#snapshotter-tool).

## order/default

This tag is used to control the order that results are displayed. If your code block produces multiple objects (i.e., multiple buttons would appear in the web UI when running the code), you __must__ use this tag to specify the order in which they should be displayed.

You can also set `order=null` if the output is not useful (e.g., a ticking table which we don't support proper snapshotting yet).

Console logs are always put as the last tab of the output and you do not need to specify them in the `order`. If you need to, use `:log` as its key.

`default` can be used to specify the default tab. If not specified, the first tab will be the default.

````markdown
Show t as the first tab and p as the second tab with p being selected by default.

```python order=t,p default=p
t = some_table(...)
p = some_plot(t)
```

Do not display the output, but still run the code block to ensure it works.

```python order=null
t = some_ticking_table(...)
```
````

## test-set

Used when code blocks need to be run with a previous block in scope. You can put as many code blocks as you want in a test set. The test set name is unique within a file.

````markdown
```python test-set=1
a = 4
```

Some more text here before another code block that needs the first code block result.

```python test-set=1
b = a + 1
```

This code block does not have access to `a` or `b`.

```python
print('hello')
```
````

## docker-config

This allows you to specify that the snapshot needs to load extra docker configs such as TensorFlow or NLTK. The configs specified here will be applied as extra config files. See [Docker docs on multiple compose files](https://docs.docker.com/compose/extends/#understanding-multiple-compose-files).

Note that these are extra configs and the base `docker-compose.yml` will always be the first config loaded. The extra configs can override its properties. See `snapshotter/docker` for the existing configs for each language.

The snapshots will __always__ run with the JS client against the `server` service.

The config files should be in the docker directory as `docker-compose.{name}.yml`. For example, `tensorflow` adds the `docker-compose.tensorflow.yml` config.

Multiple configs can be added with a comma separated string. For example if you want to add a Kafka service and use the NLTK image, define `docker-compose.kafka.yml` and set `docker-config=kafka,nltk`.

In a `test-set`, the `docker-config` only needs to be specified on 1 snippet (typically the first snippet makes the most sense). The other snippets do not need to specify `docker-config`. If they do and the config does not match, an error will be thrown.

````markdown
```python docker-config=kafka
# This code could access a Kafka service started by `docker-compose.kafka.yml`
# Use the Docker container name as the hostname for connecting
```
````

## reset/timeout

These tags are used to control the behavior of the snapshotter tool when running code blocks.

`reset` will completely reset the Docker container after the code block test set is run. Normally, we delete all new global scope variables that have been created by the code block, but this can cause issues with some code blocks such as threading examples or configuring global objects. The `reset` tag ensures we don't break while cleaning up after a test set.

`timeout` adjusts the default 30 second timeout. This should rarely be used as your examples should be relatively small/quick. Set to `0` to disable the timeout (highly discouraged).

````markdown
```python reset
# Some threading code that breaks when the references are just deleted instead of stopping the thread.
```

```python timeout=60
# Longer running code block
```
````

## syntax

Used when a block should not be run because it is a syntax example and not functional code.

````markdown
```python syntax
new_table(cols...)
```
````

## skip-test/should-fail

This tag should be used sparingly (ideally never). Used when a block should not be run because it does not work. `should-fail` does not behave any differently right now, but may in the future.

````markdown
```python skip-test
new_table(cols...)
```

```python should-fail
raise Exception("This code should fail")
```
````