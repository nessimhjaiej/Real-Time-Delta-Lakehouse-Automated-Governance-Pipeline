## Rule: every query must have a row limit

Every query executed on a user's behalf must include an explicit row
limit, set no higher than the limit configured for that user's role. A
query without a limit, or with a limit above the role's maximum, must be
rejected before execution rather than truncated afterward.

## Rule: only read access is permitted

Only SELECT statements may be executed. No query may create, alter, or
drop a table or view, insert, update, or delete rows, or otherwise modify
the underlying data in any way. This is enforced both by the statement
type check in the SQL validator and by executing exclusively against a
connection that has no write capability to the source tables.

## Rule: one statement per request

A single request may execute exactly one SQL statement. Multiple
statements separated by a semicolon (so-called "stacked queries") must be
rejected outright, regardless of whether the statements that follow the
first appear harmless, since this is a well-known SQL injection pattern.
