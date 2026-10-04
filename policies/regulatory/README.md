# Regulatory text goes here

This directory is for real regulatory text you supply yourself (for
example GDPR articles) -- nothing in this directory is written or
fabricated by the agent that built this project, by design.

## Expected format

Plain Markdown or `.txt` files. The ingestion script
(`src/governance_agent/rag/ingest.py`) chunks files in this directory by
article heading, so each article becomes its own citable chunk. A heading
line is detected as a line matching `Article <number>` (optionally
followed by a title), e.g.:

```
Article 17
Right to erasure ('right to be forgotten')

1. The data subject shall have the right to obtain from the controller...
```

Everything between one `Article N` heading and the next becomes one chunk,
tagged with `doc_type: regulatory` and `section: "Article N"` so it can be
cited precisely (e.g. "per GDPR Article 17") rather than as an
undifferentiated blob of regulatory text.

If a single article's text is long enough to exceed the chunker's size
threshold, it's further split into overlapping sub-chunks that still carry
the same article's section label, so citations stay attributable even when
an article had to be split.

Until you add real files here, regulatory citation retrieval will simply
return no results -- the rest of the system (SQL governance, PII blocking)
does not depend on this directory being populated.
