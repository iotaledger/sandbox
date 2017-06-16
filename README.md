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


## Usage

The easiest way to run the full system is to use the supplied `docker-compose.yml`
file, which is going to run one instance of the API server, worker and emulators for
the two google cloud services it relies on—pubsub and datastore.

If you want to run either without using the provided docker images then you need to
provide emulators—or run them inside the google cloud—, which can be done after installing
the google cloud sdk.

```
$ CLOUDSDK_INSTALL_DIR=./ curl https://sdk.cloud.google.com | bash
$ ./google-cloud-sdk/bin/gcloud init
$ ./google-cloud-sdk/bin/gcloud components install -q pubsub-emulator beta 
$ ./google-cloud-sdk/bin/gcloud components install -q cloud-datastore-emulator beta 
$ ./google-cloud-sdk/bin/gcloud beta emulators pubsub start
$ $(./google-cloud-sdk/bin/gcloud beta emulators pubsub env-init) # sets PUBSUB_EMULATOR_HOST
$ ./google-cloud-sdk/bin/gcloud beta emulators datastore start
$ $(./google-cloud-sdk/bin/gcloud beta emulators datastore env-init) # sets DATASTORE_EMULATOR_HOST
```

### Sandbox API
