# mongo-op-throttler
Applies a list of mongo operations at a limited speed. 

## Usage
mongo-op-throttler takes a oplog and runs the operations in the oplog against a Mongo at a fixed rate
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
