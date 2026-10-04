
## Rule: no raw or hashed PII in query results

IP addresses must never appear in a
query result returned to an end user, whether in raw form or as a hash.
This applies even when the identifier is deterministically hashed (e.g.
SHA-256 with no salt), since a hash of a small, guessable value  can be reversed via a rainbow table and is not a
safe substitute for omitting the field entirely.

## Rule: customer identifiers may be used only as join keys

A hashed customer identifier may be used internally to join an orders
table to a customer dimension table, in order to compute an aggregate such
as "revenue by country." It may never be selected into the output of a
query, nor used as a filter condition, since either usage re-identifies
which rows belong to which customer.
