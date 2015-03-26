#!/bin/bash

set -e

pushd $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
godep go build -a github.com/skytap/heapster

docker build -t heapster:canary .
popd
