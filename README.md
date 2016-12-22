# IOTA sandbox

## API endpoints

All endpoints except `attachToTangle` mirror IRI behaviour, i.e. they expect the 
same json and will return the same answer unless there's an error caused by the
webserver itself.

- POST /commands/getNodeInfo
- POST /commands/getTips
- POST /commands/findTransactions
- POST /commands/getTrytes
- POST /commands/getInclusionStates
- POST /commands/getBalances
- POST /commands/getTransactionsToApprove
- POST /commands/broadcastTransactions
- POST /commands/storeTransactions

`attachToTangle` uses a rate limited queue to guarantee fair distribution of 
compute resource and to encourage users to setup their own nodes.

- POST /commands/attachToTangle

## Building

Use glide to install the dependencies:

```
# glide install
```

and then build the server (or the worker):

```
# go build -o sandbox-server ./cmd/server
```
