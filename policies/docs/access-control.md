## Rule: unknown metrics and dimensions are rejected, not guessed

If a question cannot be mapped to one of the metrics or dimensions defined
in the semantic layer, the system must say so and decline to answer,
rather than writing ad hoc SQL or guessing at a plausible-sounding
metric definition. Only metrics and dimensions that a developer has
explicitly defined and reviewed may be used to answer a question.

## Rule: the model never produces the final numbers

The language model may select which metric, dimensions, and filters to
use, and may write a short explanation of a result, but the numbers
themselves must always come from executing the compiled, validated SQL
query -- never from the model generating or restating a number on its
own.
