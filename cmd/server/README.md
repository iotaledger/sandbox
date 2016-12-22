# Server

This is the worker that, in its current form, consumes IRI jobs off of 
an Amazon SQS queue `INCOMING_QUEUE_NAME` and then puts the finished job into
`FINISHED_QUEUE_NAME`.

At this point the only command supported is `attachToTangle`, which does the
proof-of-work via the executable at `CCURL_PATH`.

## environment variables

required:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `INCOMING_QUEUE_NAME`
- `FINISHED_QUEUE_NAME`
- `CCURL_PATH`


optional:

- `IRI_URI` default `http://localhost:14265/`
- `DEBUG` set to `1` for debug logging
