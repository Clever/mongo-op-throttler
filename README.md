# mongo-op-throttler
Applies a list of mongo operations at a limited speed. 

## Usage
mongo-op-throttler takes a oplog and runs the operations in the oplog against a Mongo at a fixed rate.
It applies the operations in an idempotent way which means that inserts are converted to upserts, and
the like. We do this so that even if we fail half-way through applying oplog operations we don't have
to know where we failed and can simply re-run the job with the same input.

It has only been tested against the Mongo 2.4 version of oplogs, but in theory should work with other
versions.
```
go run main.go --mongoURL localhost --path oplog.bson
```

You can specify the following flags

flag          | default      | description
:-----------: | :----------: | :---------:
`--speed`     | `1`          | Number of operations per second
`--mongoURL`  | `localhost`  | Mongo URL to run the operations against
`--path`      | `/dev/stdin` | Oplog file to replay


## Development
You can run the tests with:
```bash
make test
```
