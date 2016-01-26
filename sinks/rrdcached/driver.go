// Copyright 2015 Google Inc. All Rights Reserved.
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

package rrdcached

import (
	"bytes"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/heapster/extpoints"
	sink_api "k8s.io/heapster/sinks/api"
	kube_api "k8s.io/kubernetes/pkg/api"
	"github.com/golang/glog"
	rrdcached "github.com/skytap/go-rrdcached"
)

// ------------------------------
// Sink

type config struct {
	user          string
	password      string
	host          string
	port          int64
	baseDir       string
	ignoreEmptyNS bool
	step          int64
	heartbeat     int64
}

type rrdcachedClient interface {
	Create(filename string, start int64, step int64, overwrite bool, ds []string, rra []string) (*rrdcached.Response, error)
	Update(filename string, values ...string) (*rrdcached.Response, error)
}

type rrdcachedSink struct {
	client rrdcachedClient
	c      config
}

func (self *rrdcachedSink) Register(metrics []sink_api.MetricDescriptor) error {
	return nil
}

func (self *rrdcachedSink) Unregister(metrics []sink_api.MetricDescriptor) error {
	return nil
}

func (sink *rrdcachedSink) StoreEvents(events []kube_api.Event) error {
	/*
		Events are things like created pod, killed pod, pod container image already present on machine, etc.

		eg:
		{ TypeMeta: ...
		  ObjectMeta: ...
		  InvolvedObject: ...
		  Reason: pulled
		  Message: Pod container image "gcr.io/google_containers/pause:0.8.0" already present on machine
		  Source: { Component:kubelet Host:10.0.0.7 }
		  FirstTimestamp: 2015-07-29 21:57:16 +0000 UTC
		  LastTimestamp: 2015-07-29 21:57:16 +0000 UTC
		  Count:1 }

		These don't really map to RRD metrics, so ignore this data for RRD sink.
	*/
	return nil
}

func debugTimeseriesData(datapoint sink_api.Timeseries) {
	glog.Infof("datapoint => %+v", datapoint)

	point := datapoint.Point
	desc := datapoint.MetricDescriptor

	glog.Infof("    Point => %+v", point)
	glog.Infof("     Desc => %+v", desc)

	glog.Infof("   Metric => %v: %v", point.Name, point.Value)
	glog.Infof("          => host: %v", point.Labels["host"])
	glog.Infof("          => container: %v", point.Labels["container_name"])
	glog.Infof("          => pod: %v", point.Labels["pod_name"])
	glog.Infof("          => resource: %v", point.Labels["resource_id"])
	glog.Infof("          => units: %v", desc.Units.String())
	glog.Infof("          => type: %v", desc.Type.String())
	glog.Infof("          => value type: %v", desc.ValueType.String())
}

type PathBuffer struct {
	bytes.Buffer
}

func (buffer *PathBuffer) appendDirectory(dir string) {
	buffer.WriteString(dir)
	buffer.WriteString("/")
}

func GetRRDLocation(datapoint sink_api.Timeseries, baseDir string) string {
	// Generates RRD location of the following format, omitting any missing data.
	// If 'baseDir' is specified, RRD filenames will be absolute. Otherwise they'll be relative and rrdcached daemon's default base dir will apply.
	//
	//   {baseDir}/namespace_{namespace}/{container_name}/{hostname}/{pod_name|pod_id}/{metric}_{unit}_{type}/{resource_id}.rrd

	point := datapoint.Point
	desc := datapoint.MetricDescriptor

	buffer := &PathBuffer{}

	// Organize by namespace
	if val, ok := point.Labels["pod_namespace"]; ok && val != "" {
		buffer.appendDirectory("namespace_" + val)
	} else {
		buffer.appendDirectory("namespace_default")
	}

	// ... then by container name
	//   Note: Don't allow container name to start with '/', this is weird mid-filepath. eg "/" and "/docker" containers.
	containerName := point.Labels["container_name"]
	if strings.HasPrefix(containerName, "/") {
		containerName = strings.Replace(containerName, "/", "_", 1)
	}
	buffer.appendDirectory(containerName)

	// ... then by hostname
	if val, ok := point.Labels["hostname"]; ok && val != "" {
		buffer.appendDirectory(val)
	} else {
		buffer.appendDirectory("no_hostname")
	}

	// ... then by pod name||id
	if val, ok := point.Labels["pod_name"]; ok && val != "" {
		buffer.appendDirectory(val)
	} else if val, ok := point.Labels["pod_id"]; ok && val != "" {
		buffer.appendDirectory(val)
	}

	// ... then by '{metric name}_{units}_{resource id}', flattening any '/' to '_'
	var rrdName bytes.Buffer
	rrdName.WriteString(point.Name)
	if val := desc.Units.String(); val != "" {
		rrdName.WriteString("_")
		rrdName.WriteString(val)
	}
	if val := desc.Type.String(); val != "" {
		rrdName.WriteString("_")
		rrdName.WriteString(val)
	}
	if val, ok := point.Labels["resource_id"]; ok && val != "" {
		rrdName.WriteString("_")
		rrdName.WriteString(val)
	}

	// Complete rrd path is now ready, join the pieces and return.
	buffer.WriteString(strings.Replace(rrdName.String(), "/", "_", -1))
	buffer.WriteString(".rrd")

	rrdPath := buffer.String()

	// Prepend base directory if specified.
	if baseDir != "" {
		rrdPath = baseDir + "/" + rrdPath
	}

	return rrdPath
}

func (self *rrdcachedSink) StoreTimeseries(timeseries []sink_api.Timeseries) error {
	for index := range timeseries {
		//debugTimeseriesData(timeseries[index])

		point := timeseries[index].Point
		desc := timeseries[index].MetricDescriptor

		if self.c.ignoreEmptyNS {
			if val, ok := point.Labels["pod_namespace"]; !ok || val == "" {
				continue
			}
		}

		filename := GetRRDLocation(timeseries[index], self.c.baseDir)

		if err := self.writeData(filename, desc.Type.String(), point.End.Unix(), point.Value); err != nil {
			glog.Error(err)
			continue
		}
	}
	return nil
}

func (self *rrdcachedSink) getAppropriateDS(dataType string) []string {
	switch strings.ToLower(dataType) {
	case "gauge":
		return []string{fmt.Sprintf("DS:sum:GAUGE:%d:0:U", self.c.heartbeat)}
	case "cumulative":
		return []string{fmt.Sprintf("DS:sum:DERIVE:%d:0:U", self.c.heartbeat)}
	default:
		return nil
	}
}

func (self *rrdcachedSink) getAppropriateRRA() []string {
	return []string{
		"RRA:AVERAGE:0.5:1:244", "RRA:AVERAGE:0.5:24:7320", "RRA:AVERAGE:0.5:168:1030", "RRA:AVERAGE:0.5:672:1543", "RRA:AVERAGE:0.5:5760:374",
		"RRA:MAX:0.5:1:244", "RRA:MAX:0.5:24:7320", "RRA:MAX:0.5:168:1030", "RRA:MAX:0.5:672:1543", "RRA:MAX:0.5:5760:374",
		"RRA:MIN:0.5:1:244", "RRA:MIN:0.5:24:7320", "RRA:MIN:0.5:168:1030", "RRA:MIN:0.5:672:1543", "RRA:MIN:0.5:5760:374"}
}

func (self *rrdcachedSink) writeData(filename string, dataType string, timestamp int64, value interface{}) error {
	formattedValue := fmt.Sprintf("%v:%v", timestamp, value)

	// Rather than attempting to check whether the RRD exists already, instead simply attempt to UPDATE,
	//   then if error check for type FileDoesNotExistError, in which case CREATE and re-UPDATE.
	updateResp, updateErr := self.client.Update(filename, formattedValue)
	if updateErr != nil {
		if _, ok := updateErr.(*rrdcached.FileDoesNotExistError); ok {
			ds := self.getAppropriateDS(dataType)
			if ds == nil {
				return fmt.Errorf("RRD CREATE failed: %v, unrecognized data type '%v'", filename, dataType)
			}
			rra := self.getAppropriateRRA()
			createResp, _ := self.client.Create(filename, timestamp, self.c.step, true, ds, rra)
			if createResp.Status != 0 {
				return fmt.Errorf("RRD CREATE failed: %v, error %v", filename, createResp.Raw)
			}

		}
		updateResp, updateErr = self.client.Update(filename, formattedValue)
	}
	if updateResp.Status != 0 {
		return fmt.Errorf("RRD UPDATE failed: %v, value %v, error %v", filename, value, updateResp.Raw)
	}
	return nil
}

func (self *rrdcachedSink) DebugInfo() string {
	desc := "Sink type: Rrdcached\n"
	desc += "\tDataset: cadvisor\n\n"
	desc += fmt.Sprintf("\tclient: Host \"%+v:%d\"\n", self.c.host, self.c.port)
	desc += fmt.Sprintf("\t- step %ds, heartbeat %ds\n", self.c.step, self.c.heartbeat)
	desc += fmt.Sprintf("\t- base rrd directory? %v\n", self.c.baseDir)
	desc += fmt.Sprintf("\t- ignore empty namespaces? %v\n", self.c.ignoreEmptyNS)
	return desc
}

func (self *rrdcachedSink) Name() string {
	return "RRDCacheD Sink"
}

// Returns a thread-compatible implementation of rrdcached interactions.
func new(c config) (sink_api.ExternalSink, error) {
	glog.Infof("Using rrdcached with config: %q", c)

	host_and_port := strings.Split(c.host, ":")
	host := host_and_port[0]
	port, _ := strconv.ParseInt(host_and_port[1], 10, 64)
	client := rrdcached.ConnectToIP(host, port)

	return &rrdcachedSink{
		client: client,
		c:      c,
	}, nil
}

func init() {
	extpoints.SinkFactories.Register(CreateRrdcachedSink, "rrdcached")
}

func CreateRrdcachedSink(uri *url.URL, _ extpoints.HeapsterConf) ([]sink_api.ExternalSink, error) {
	defaultConfig := config{
		user:          "root",
		password:      "root",
		host:          "localhost:9010",
		baseDir:       "",
		ignoreEmptyNS: false,
		step:          15,
		heartbeat:     60,
	}

	if len(uri.Host) > 0 {
		defaultConfig.host = uri.Host
	}
	opts := uri.Query()
	if len(opts["user"]) >= 1 {
		defaultConfig.user = opts["user"][0]
	}
	if len(opts["pw"]) >= 1 {
		defaultConfig.password = opts["pw"][0]
	}
	if len(opts["baseDir"]) >= 1 {
		defaultConfig.baseDir = opts["baseDir"][0]
	}
	if len(opts["ignoreEmptyNamespaces"]) >= 1 {
		val, err := strconv.ParseBool(opts["ignoreEmptyNamespaces"][0])
		if err != nil {
			return nil, fmt.Errorf("invalid value %q for option 'ignoreEmptyNamespaces' passed to rrdcached sink", opts["ignoreEmptyNamespaces"][0])
		}
		defaultConfig.ignoreEmptyNS = val
	}
	// Note: The 'step' and 'heartbeat' values are technically configurable,
	//   however it raises the scenario of restarting heapster with different values
	//   after some RRDs have already been created with old values, which gets... messy.
	// Initial support places this responsibility on the user, to delete or recreate RRDs as appropriate.
	if len(opts["step"]) >= 1 {
		val, err := strconv.ParseInt(opts["step"][0], 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid value %q for option 'step' passed to rrdcached sink", opts["step"][0])
		}
		defaultConfig.step = val
	}
	if len(opts["heartbeat"]) >= 1 {
		val, err := strconv.ParseInt(opts["heartbeat"][0], 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid value %q for option 'heartbeat' passed to rrdcached sink", opts["heartbeat"][0])
		}
		defaultConfig.heartbeat = val
	}
	sink, err := new(defaultConfig)
	if err != nil {
		return nil, err
	}
	glog.Infof("created rrdcached sink with options: %v", defaultConfig)

	return []sink_api.ExternalSink{sink}, nil
}
