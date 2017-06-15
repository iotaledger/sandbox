# IOTA sandbox

The sandbox is currently required to run inside the google cloud.

## Building

Use glide to install the dependencies:

```
# glide install
```

and then build the server (or the worker):

```
# go build -o sandbox-server ./cmd/server
```
