# The Deephaven IDE

This is a demo instance of the Deephaven IDE.
It encapsulates the full richness of the console, notebook, table and plotting widgets, and interactive experiences in this interface, as well as the Deephaven engine and APIs that make it go.
\
\
**THE DEMO INSTANCE WILL LAST FOR 45 MINUTES.**\
We hope you find that to be sufficient time to take a tour and experiment a bit.
\
\
\
The IDE provides a REPL experience for exploring data, one that uniquely supports interactive interrogation of real-time data (as well as batch).
People often use it for building server-side data-driven applications.
\
\
This console is a Python experience.

```python
print("Hello world!")

def some_fcn(some_input):
    return 10 * some_input

some_input = 3.3
x = some_fcn(some_input)

print(x)
print(type(x))
```
\
\
It is also used for interacting with both static tables.....
```python
from deephaven.TableTools import newTable, stringCol, intCol
static_table = newTable(
   stringCol("Name_String_Col", "Data String 1", 'Data String 2', "Data String 3"),
   intCol("Name_Int_Col", 44, 55, 66)
)
```
\
\
... and dynamically updating ones.

```python
from deephaven.TableTools import timeTable
import random
updating_table = timeTable('00:00:00.400').updateView("Row = i", "Some_Int = (int)random.randint(0,100)").reverse()
```


## These notebooks demonstrate Deephaven differentiators and workflows
(You can find the notebooks also listed at top-right under "File Explorer".)


1. [Tables, Updates, and the Engine](01%20Tables,%20Updates,%20and%20the%20Engine.md)
2. [Stream and Batch Together](02%20Stream%20and%20Batch%20Together.md)
3. [Kafka Stream vs Append](03%20Kafka%20Stream%20vs%20Append.md)


Go to [quick start](https://deephaven.io/core/docs/tutorials/quickstart/) to install Deephaven from our pre-built images.

Or simply [open the first notebook.](01%20Tables,%20Updates,%20and%20the%20Engine.md)


```python
print("Cheers!")
print("Should be fun.")
```
