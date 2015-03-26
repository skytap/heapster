// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datasource

import (
	"github.com/skytap/heapster/sources/api"
)

type Host struct {
	IP       string
	Port     string
	Resource string
}

type Cadvisor interface {
	// GetAllContainers returns container spec and stats for the root cgroup as 'root' and
	// and all containers on the 'host' as 'subcontainers'.
	GetAllContainers(host Host, numStats int) (subcontainers []*api.Container, root *api.Container, err error)
}

func NewCadvisor() Cadvisor {
	return &cadvisorSource{}
}

type Kubelet interface {
	// GetContainer returns container spec and stats for the container pointed to by 'host.Resource', running on the kubelet specified in 'host.IP'.
	// TODO(vishh): Once kubelet exposes a get all stats API, modify this API to return stats for all Pods.
	GetContainer(host Host, numStats int) (containers *api.Container, err error)
}

func NewKubelet() Kubelet {
	return &kubeletSource{}
}
