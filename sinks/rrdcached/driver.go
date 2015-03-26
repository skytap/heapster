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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang/glog"
	rrdcached "github.com/skytap/go-rrdcached"
	sink_api "github.com/skytap/heapster/sinks/api"
)

var (
	step      = 300
	heartbeat = 600
	defineDS  = []string{"DS:sum:GAUGE:" + strconv.Itoa(heartbeat) + ":0:U"}
	defineRRA = []string{
		"RRA:AVERAGE:0.5:1:244", "RRA:AVERAGE:0.5:24:7320", "RRA:AVERAGE:0.5:168:1030", "RRA:AVERAGE:0.5:672:1543", "RRA:AVERAGE:0.5:5760:374",
		"RRA:MAX:0.5:1:244", "RRA:MAX:0.5:24:7320", "RRA:MAX:0.5:168:1030", "RRA:MAX:0.5:672:1543", "RRA:MAX:0.5:5760:374",
		"RRA:MIN:0.5:1:244", "RRA:MIN:0.5:24:7320", "RRA:MIN:0.5:168:1030", "RRA:MIN:0.5:672:1543", "RRA:MIN:0.5:5760:374"}
)

// ------------------------------
// Sink

type rrdcachedSink struct {
	host       string
	port       int64
	directory  string
	remappings map[string]string
	driver     *rrdcached.Rrdcached // The driver for RRDCacheD interactions.
}

func NewSink(hostname string, directory string, remappings string) (sink_api.ExternalSink, error) {
	glog.Infof("Using rrdcached on host %q with directory %q", hostname, directory)

	host_and_port := strings.Split(hostname, ":")
	host := host_and_port[0]
	port, _ := strconv.ParseInt(host_and_port[1], 10, 64)
	driver := rrdcached.ConnectToIP(host, port)

	remappingsMap := make(map[string]string)
	if len(remappings) > 0 {
		for _, remapKey := range strings.Split(remappings, ",") {
			remap := strings.Split(remapKey, "=")
			oldKey := remap[0]
			newKey := remap[1]
			remappingsMap[oldKey] = newKey
		}
	}

	return &rrdcachedSink{
		host:       host,
		port:       port,
		directory:  directory,
		remappings: remappingsMap,
		driver:     driver,
	}, nil
}

func (self *rrdcachedSink) Register(metrics []sink_api.MetricDescriptor) error {
	return nil
}

func (self *rrdcachedSink) DebugInfo() string {
	desc := "Sink type: Rrdcached\n"
	desc += "\tDataset: cadvisor\n\n"
	desc += fmt.Sprintf("\tclient: Host %q:%d\n", self.host, self.port)
	desc += fmt.Sprintf("\tstorage: %v\n", self.directory)
	desc += fmt.Sprintf("\tkey remappings: %v\n", self.remappings)
	return desc
}

func (self *rrdcachedSink) StoreTimeseries(timeseries []sink_api.Timeseries) error {
	glog.Infof(self.DebugInfo()) // TODO: Remove

	for index := range timeseries {
		// debugTimeseriesData(timeseries[index])

		point := timeseries[index].Point

		if point.Labels["container_name"] == "/" {
			// TODO: Consider whether we care about this data. Perhaps add a flag to include/ignore. Base path logic would need to change slightly.
			continue
		}

		basepath := fmt.Sprintf("%v/%v/%v", self.directory, point.Labels["container_name"], point.Labels["hostname"]) // TODO: configurable? how? hmm.

		// remap key to custom name, if provided.
		key := point.Name
		if remap, ok := self.remappings[key]; ok {
			if len(remap) == 0 {
				continue // key remapped to empty string, so skip this metric.
			}
			key = remap
		}

		// construct filename
		filename := fmt.Sprintf("%v/%v.rrd", basepath, key)

		if err := self.ensureRRDExists(filename, point.End.Unix()); err != nil {
			glog.Error(err)
			continue
		}

		// update RRD with value
		// TODO: group by key, submit multiple updates at once
		resp := self.driver.Update(filename, fmt.Sprintf("%v:%v", point.End.Unix(), point.Value))
		if resp.Status != 0 {
			glog.Error(fmt.Sprintf("UPDATE failed: %v, value %v, error %v", filename, point.Value, resp.Raw))
		}

	}
	return nil
}

func debugTimeseriesData(datapoint sink_api.Timeseries) {
	point := datapoint.Point
	desc := datapoint.MetricDescriptor

	glog.Infof("datapoint => %+v", datapoint)
	glog.Infof("    Point => %+v", point)
	glog.Infof("     Desc => %+v", desc)

	glog.Infof("   Metric => %v: %v", point.Name, point.Value)
	glog.Infof("          => container: %v, %v", point.Labels["container_name"], point.Labels["hostname"])
	glog.Infof("          => units: %v", desc.Units.String())
	glog.Infof("          => type: %v", desc.Type.String())
	glog.Infof("          => value type: %v", desc.ValueType.String())
	glog.Infof("          => labels: %v", sink_api.LabelsToString(point.Labels, "_"))
}

func (self *rrdcachedSink) ensureRRDExists(filename string, timestamp int64) error {
	if err := self.ensureRRDDirectoryExists(filename); err != nil {
		return err
	}
	if err := self.ensureRRDFileExists(filename, timestamp); err != nil {
		return err
	}
	return nil
}

func (self *rrdcachedSink) ensureRRDDirectoryExists(filename string) error {
	// keys contain slash by default, eg "network/rx", so build full file path then mkdir as needed.
	lastSlashPos := strings.LastIndex(filename, "/")
	fileDir := filename[:lastSlashPos]
	return os.MkdirAll(fileDir, 0755)
}

func (self *rrdcachedSink) ensureRRDFileExists(filename string, timestamp int64) error {
	_, err := os.Stat(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		resp := self.driver.Create(filename, timestamp, -1, false, defineDS, defineRRA)
		if resp.Status != 0 {
			return fmt.Errorf("RRD CREATE failed: %v, error %v", filename, resp.Raw)
		}
	}
	return nil
}
