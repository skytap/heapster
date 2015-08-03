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

// This file implements a cadvisor datasource, that collects metrics from an instance
// of cadvisor runing on a specific host.

package datasource

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/GoogleCloudPlatform/heapster/sources/api"
	kube_client "github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	"github.com/golang/glog"
	cadvisor "github.com/google/cadvisor/info/v1"
)

// TODO(vmarmol): Use Kubernetes' if we export it as an API.
const kubernetesPodNameLabel = "io.kubernetes.pod.name"

type kubeletSource struct {
	config *kube_client.KubeletConfig
	client *http.Client
}

func (self *kubeletSource) postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body - %v", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed - %q, response: %q", response.Status, string(body))
	}
	err = json.Unmarshal(body, value)
	if err != nil {
		return fmt.Errorf("failed to parse output. Response: %q. Error: %v", string(body), err)
	}
	return nil
}

func (self *kubeletSource) getContainer(url string, start, end time.Time, resolution time.Duration, align bool) (*api.Container, error) {
	body, err := json.Marshal(cadvisor.ContainerInfoRequest{Start: start, End: end})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	var containerInfo cadvisor.ContainerInfo
	client := self.client
	if client == nil {
		client = http.DefaultClient
	}
	err = self.postRequestAndGetValue(client, req, &containerInfo)
	if err != nil {
		glog.V(2).Infof("failed to get stats from kubelet url: %s - %s\n", url, err)
		return nil, err
	}
	glog.V(4).Infof("url: %q, body: %q, data: %+v", url, string(body), containerInfo)
	return parseStat(&containerInfo, start, resolution, align), nil
}

func (self *kubeletSource) GetContainer(host Host, start, end time.Time, resolution time.Duration, align bool) (container *api.Container, err error) {
	var schema string
	if self.config != nil && self.config.EnableHttps {
		schema = "https"
	} else {
		schema = "http"
	}

	url := fmt.Sprintf("%s://%s:%d/%s", schema, host.IP, host.Port, host.Resource)
	glog.V(3).Infof("about to query kubelet using url: %q", url)

	return self.getContainer(url, start, end, resolution, align)
}

// TODO(vmarmol): Use Kubernetes' if we export it as an API.
type statsRequest struct {
	// The name of the container for which to request stats.
	// Default: /
	ContainerName string `json:"containerName,omitempty"`

	// Max number of stats to return.
	// If start and end time are specified this limit is ignored.
	// Default: 60
	NumStats int `json:"num_stats,omitempty"`

	// Start time for which to query information.
	// If ommitted, the beginning of time is assumed.
	Start time.Time `json:"start,omitempty"`

	// End time for which to query information.
	// If ommitted, current time is assumed.
	End time.Time `json:"end,omitempty"`

	// Whether to also include information from subcontainers.
	// Default: false.
	Subcontainers bool `json:"subcontainers,omitempty"`
}

// Get stats for all non-Kubernetes containers.
func (self *kubeletSource) GetAllRawContainers(host Host, start, end time.Time, resolution time.Duration, align bool) ([]api.Container, error) {
	url := fmt.Sprintf("http://%s:%d/stats/container/", host.IP, host.Port)
	return self.getAllContainers(url, start, end, resolution, align)
}

func (self *kubeletSource) getAllContainers(url string, start, end time.Time, resolution time.Duration, align bool) ([]api.Container, error) {
	// Request data from all subcontainers.
	request := statsRequest{
		ContainerName: "/",
		Start:         start,
		End:           end,
		Subcontainers: true,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	var containers map[string]cadvisor.ContainerInfo
	err = self.postRequestAndGetValue(http.DefaultClient, req, &containers)
	if err != nil {
		return nil, fmt.Errorf("failed to get all container stats from Kubelet URL %q: %v", url, err)
	}

	// TODO(vmarmol): Use this for all stats gathering.
	// Don't include the Kubernetes containers, those are included elsewhere.
	result := make([]api.Container, 0, len(containers))
	for _, containerInfo := range containers {
		if _, ok := containerInfo.Spec.Labels[kubernetesPodNameLabel]; ok {
			continue
		}

		cont := parseStat(&containerInfo, start, resolution, align)
		if cont != nil {
			result = append(result, *cont)
		}
	}

	return result, nil
}
