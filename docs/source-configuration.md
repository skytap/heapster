Configuring sources
===================

Heapster can get data from multiple sources. These are specified on the command line
via the `--source` flag. The flag takes an argument of the form `PREFIX:CONFIG[?OPTIONS]`.
Options (optional!) are specified as URL query parameters, separated by `&` as normal.
This allows each source to have custom configuration passed to it without needing to
continually add new flags to Heapster as new sources are added. This also means
heapster can capture metrics from multiple sources at once, potentially even multiple
Kubernetes clusters.

## Current sources
### Kubernetes
To use the kubernetes source add the following flag:

```
--source=kubernetes:<KUBERNETES_MASTER>[?<KUBERNETES_OPTIONS>]
```

If you're running Heapster in a Kubernetes pod you can use the following flag:

```
--source=kubernetes
```
Heapster requires access to `token-system-monitoring` secret to connect with the master securely.
To run without auth file, use the following config:
```
--source=kubernetes:http://kubernetes-ro?auth=""
```
*Note: Remove "monitoring-token" volume from heaspter controller config if you are running without auth.*

Alternatively, create a [service account](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/design/service_accounts.md)
like this:

```
cat <EOF | kubectl create -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: heapster
EOF
```

This will generate a token on the API server. You will then need to reference the service account in your Heapster pod spec like this:

```
apiVersion: "v1"
kind: "ReplicationController"
metadata:
  labels:
    name: "heapster"
  name: "monitoring-heapster-controller"
spec:
  replicas: 1
  selector:
    name: "heapster"
  template:
    metadata:
      labels:
        name: "heapster"
    spec:
      serviceAccount: "heapster"
      containers:
        -
          image: "kubernetes/heapster:v0.13.0"
          name: "heapster"
          command:
            - "/heapster"
            - "--source=kubernetes:http://kubernetes-ro?useServiceAccount=true&auth="
            - "--sink=influxdb:http://monitoring-influxdb:80"
```

This will mount the generated token at `/var/run/secrets/kubernetes.io/serviceaccount/token` in the heapster container.


The following options are available:
* `inClusterConfig` - Use kube config in service accounts associated with heapster's namesapce. (default: true)
* `kubeletPort` - kubelet port to use (default: `10255`)
* `kubeletHttps` - whether to use https to connect to kubelets (default: `false`)
* `apiVersion` - API version to use to talk to Kubernetes. Defaults to the version in kubeConfig.
* `insecure` - whether to trust kubernetes certificates (default: `false`)
* `auth` - client auth file to use. Set auth if the service accounts are not usable.
* `useServiceAccount` - whether to use the service account token if one is mounted at `/var/run/secrets/kubernetes.io/serviceaccount/token` (default: `false`)


### Cadvisor
Cadvisor source comes in two types: standalone & CoreOS:

#### External
External cadvisor source "discovers" hosts from the specified file. Use it like this:

```
--source=cadvisor:external[?<OPTIONS>]
```

The following options are available:

* `standalone` - only use `localhost` (default: `false`)
* `hostsFile` - file containing list of hosts to gather cadvisor metrics from (default: `/var/run/heapster/hosts`)
* `cadvisorPort` - cadvisor port to use (default: `8080`)

Here is an example:
```shell
./heapster --source="cadvisor:external?cadvisorPort=4194"
```

The `hostsFile` parameter defines a list of hosts to poll for metrics and must be in JSON format. See below for an example:

```
{
  "Items": [
    {
      "Name": "server-105",
      "IP": "192.168.99.105"
    },
    {
      "Name": "server-106",
      "IP": "192.168.99.105"
    }
  ]
}
```

#### CoreOS
CoreOS cadvisor source discovers nodes from the specified fleet endpoints. Use it like this:

```
--source=cadvisor:coreos[?<OPTIONS>]
```

The following options are available:

* `fleetEndpoint` - fleet endpoints to use. This can be specified multiple times (no default)
* `cadvisorPort` - cadvisor port to use (default: `8080`)
