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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sink_api "github.com/GoogleCloudPlatform/heapster/sinks/api/v1"
	kube_api "github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	kube_time "github.com/GoogleCloudPlatform/kubernetes/pkg/util"
	rrdcached "github.com/skytap/go-rrdcached"
	"github.com/stretchr/testify/assert"
)

type capturedCreateCall struct {
	filename string
}

type capturedWriteCall struct {
	filename string
	values   []string
}

type fakeRrdcachedClient struct {
	capturedCreateCalls map[string]int
	capturedWriteCalls  []capturedWriteCall
}

func NewFakeRrdcachedClient() *fakeRrdcachedClient {
	return &fakeRrdcachedClient{make(map[string]int), []capturedWriteCall{}}
}

func (sink *fakeRrdcachedClient) Create(filename string, start int64, step int64, overwrite bool, ds []string, rra []string) (*rrdcached.Response, error) {
	// Track which files have been created (and how many times).
	if existing, ok := sink.capturedCreateCalls[filename]; ok {
		sink.capturedCreateCalls[filename] = existing + 1
	} else {
		sink.capturedCreateCalls[filename] = 1
	}
	return &rrdcached.Response{Status: 0, Message: "", Raw: ""}, nil
}

func (sink *fakeRrdcachedClient) Update(filename string, values ...string) (*rrdcached.Response, error) {
	// If file hasn't been created yet, simulate failure response.
	if sink.capturedCreateCalls[filename] == 0 {
		errorMsg := fmt.Sprintf("No such file: %v", filename)
		err := &rrdcached.FileDoesNotExistError{errors.New(errorMsg)}
		return &rrdcached.Response{Status: -1, Message: "", Raw: ""}, err
	}

	// File exists, so simulate success response.
	sink.capturedWriteCalls = append(sink.capturedWriteCalls, capturedWriteCall{filename, values})
	return &rrdcached.Response{Status: 0, Message: "", Raw: ""}, nil
}

type fakeRrdcachedSink struct {
	sink_api.ExternalSink
	fakeClient *fakeRrdcachedClient
}

// Returns a fake rrdcached sink.
func NewFakeSink(flatten bool, ignoreEmptyNS bool) fakeRrdcachedSink {
	client := NewFakeRrdcachedClient()

	return fakeRrdcachedSink{
		&rrdcachedSink{
			client: client,
			c: config{
				host:          "hostname",
				port:          1337,
				flatten:       flatten,
				ignoreEmptyNS: ignoreEmptyNS,
				step:          15,
				heartbeat:     600,
			},
		},
		client,
	}
}

func TestGetRRDLocation(t *testing.T) {
	// Arrange
	smd := sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	type Labels map[string]string

	// Assert - no pod id, no pod name, no resource id
	labels0 := Labels{
		"spooky":                        "notvisible",
		sink_api.LabelHostname.Key:      "localhost",
		sink_api.LabelContainerName.Key: "docker",
	}
	point0 := sink_api.Point{Name: "test/metric/1", Labels: labels0, Start: time.Now(), End: time.Now(), Value: int64(123456)}
	ts0 := sink_api.Timeseries{MetricDescriptor: &smd, Point: &point0}
	assert.Equal(t, "docker/localhost/test/metric/1_cumulative.rrd" /* expected */, GetRRDLocation(ts0, false) /* actual */)
	assert.Equal(t, "docker/localhost_test_metric_1_cumulative.rrd" /* expected */, GetRRDLocation(ts0, true) /* actual */)

	// Assert - pod id, no pod name, no resource id
	labels1 := Labels{
		"spooky":                        "notvisible",
		sink_api.LabelHostname.Key:      "localhost",
		sink_api.LabelContainerName.Key: "docker",
		sink_api.LabelPodId.Key:         "aaaa-bbbb-cccc-dddd",
	}
	point1 := sink_api.Point{Name: "test/metric/1", Labels: labels1, Start: time.Now(), End: time.Now(), Value: int64(123456)}
	ts1 := sink_api.Timeseries{MetricDescriptor: &smd, Point: &point1}
	assert.Equal(t, "docker/localhost/aaaa-bbbb-cccc-dddd/test/metric/1_cumulative.rrd" /* expected */, GetRRDLocation(ts1, false) /* actual */)
	assert.Equal(t, "docker/localhost_aaaa-bbbb-cccc-dddd_test_metric_1_cumulative.rrd" /* expected */, GetRRDLocation(ts1, true) /* actual */)

	// Assert - pod id, pod name, no resource id
	labels2 := Labels{
		"spooky":                        "notvisible",
		sink_api.LabelHostname.Key:      "localhost",
		sink_api.LabelContainerName.Key: "docker",
		sink_api.LabelPodName.Key:       "pod-name",
		sink_api.LabelPodId.Key:         "aaaa-bbbb-cccc-dddd",
	}
	point2 := sink_api.Point{Name: "test/metric/1", Labels: labels2, Start: time.Now(), End: time.Now(), Value: int64(123456)}
	ts2 := sink_api.Timeseries{MetricDescriptor: &smd, Point: &point2}
	assert.Equal(t, "docker/localhost/pod-name/test/metric/1_cumulative.rrd" /* expected */, GetRRDLocation(ts2, false) /* actual */)
	assert.Equal(t, "docker/localhost_pod-name_test_metric_1_cumulative.rrd" /* expected */, GetRRDLocation(ts2, true) /* actual */)

	// Assert - resource id, no pod id, no pod name
	labels3 := Labels{
		"spooky":                        "notvisible",
		sink_api.LabelHostname.Key:      "localhost",
		sink_api.LabelContainerName.Key: "docker",
		sink_api.LabelResourceID.Key:    "/dev/sda1",
	}
	point3 := sink_api.Point{Name: "test/metric/1", Labels: labels3, Start: time.Now(), End: time.Now(), Value: int64(123456)}
	ts3 := sink_api.Timeseries{MetricDescriptor: &smd, Point: &point3}
	assert.Equal(t, "docker/localhost/test/metric/1_cumulative/_dev_sda1.rrd" /* expected */, GetRRDLocation(ts3, false) /* actual */)
	assert.Equal(t, "docker/localhost_test_metric_1_cumulative__dev_sda1.rrd" /* expected */, GetRRDLocation(ts3, true) /* actual */)
}

func TestStoreEventsNilInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)

	// Act
	err := fakeSink.StoreEvents(nil /* events */)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreEventsEmptyInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)

	// Act
	err := fakeSink.StoreEvents([]kube_api.Event{})

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreEventsSingleEventInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)
	eventTime := kube_time.Unix(12345, 0)
	eventSourceHostname := "event1HostName"
	eventReason := "event1"
	events := []kube_api.Event{
		kube_api.Event{
			Reason:        eventReason,
			LastTimestamp: eventTime,
			Source: kube_api.EventSource{
				Host: eventSourceHostname,
			},
		},
	}

	// Act
	err := fakeSink.StoreEvents(events)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreEventsMultipleEventsInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)
	event1Time := kube_time.Unix(12345, 0)
	event2Time := kube_time.Unix(12366, 0)
	event1SourceHostname := "event1HostName"
	event2SourceHostname := "event2HostName"
	event1Reason := "event1"
	event2Reason := "event2"
	events := []kube_api.Event{
		kube_api.Event{
			Reason:        event1Reason,
			LastTimestamp: event1Time,
			Source: kube_api.EventSource{
				Host: event1SourceHostname,
			},
		},
		kube_api.Event{
			Reason:        event2Reason,
			LastTimestamp: event2Time,
			Source: kube_api.EventSource{
				Host: event2SourceHostname,
			},
		},
	}

	// Act
	err := fakeSink.StoreEvents(events)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreTimeseriesInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)

	// Act
	err := fakeSink.StoreTimeseries(nil /* timeseries */)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreTimeseriesEmptyInput(t *testing.T) {
	// Arrange
	fakeSink := NewFakeSink(false, false /* flatten, ignoreEmptyNS */)

	// Act
	err := fakeSink.StoreTimeseries([]sink_api.Timeseries{})

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	assert.Equal(t, 0 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
}

func TestStoreTimeseriesSingleMetricInput(t *testing.T) {
	// Arrange
	flatten := false
	ignoreEmptyNS := false
	fakeSink := NewFakeSink(flatten, ignoreEmptyNS)

	smd := sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	labels := make(map[string]string)
	labels["spooky"] = "notvisible"
	labels[sink_api.LabelHostname.Key] = "localhost"
	labels[sink_api.LabelContainerName.Key] = "docker"
	labels[sink_api.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"

	point := sink_api.Point{
		Name:   "test/metric/1",
		Labels: labels,
		Start:  time.Now(),
		End:    time.Now(),
		Value:  int64(123456),
	}

	ts := []sink_api.Timeseries{
		sink_api.Timeseries{
			MetricDescriptor: &smd,
			Point:            &point,
		},
	}

	// Act
	err := fakeSink.StoreTimeseries(ts)

	// Assert
	assert.NoError(t, err)

	// Filename
	filename := GetRRDLocation(ts[0], flatten)

	// Data - unix_time:value
	data := fmt.Sprintf("%v:%v", point.End.Unix(), point.Value)

	// Create calls - one per filename
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	for key, value := range fakeSink.fakeClient.capturedCreateCalls {
		assert.Equal(t, filename /* expected */, key /* actual */)
		assert.Equal(t, 1 /* expected */, value /* actual */)
	}

	// Update calls
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
	assert.Equal(t, filename /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].filename /* actual */)
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls[0].values) /* actual */)
	assert.Equal(t, data /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].values[0] /* actual */)
}

func TestStoreTimeseriesFlattenMetricFiles(t *testing.T) {
	// Arrange
	flatten := true
	ignoreEmptyNS := false
	fakeSink := NewFakeSink(flatten, ignoreEmptyNS)

	smd := sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	labels := make(map[string]string)
	labels["spooky"] = "notvisible"
	labels[sink_api.LabelHostname.Key] = "localhost"
	labels[sink_api.LabelContainerName.Key] = "docker"
	labels[sink_api.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"

	point := sink_api.Point{
		Name:   "test/metric/1",
		Labels: labels,
		Start:  time.Now(),
		End:    time.Now(),
		Value:  int64(123456),
	}

	ts := []sink_api.Timeseries{
		sink_api.Timeseries{
			MetricDescriptor: &smd,
			Point:            &point,
		},
	}

	// Act
	err := fakeSink.StoreTimeseries(ts)

	// Assert
	assert.NoError(t, err)

	// Filename
	filename := GetRRDLocation(ts[0], flatten)

	// Data - unix_time:value
	data := fmt.Sprintf("%v:%v", point.End.Unix(), point.Value)

	// Create calls - one per filename
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	for key, value := range fakeSink.fakeClient.capturedCreateCalls {
		assert.Equal(t, filename /* expected */, key /* actual */)
		assert.Equal(t, 1 /* expected */, value /* actual */)
	}

	// Update calls
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
	assert.Equal(t, filename /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].filename /* actual */)
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls[0].values) /* actual */)
	assert.Equal(t, data /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].values[0] /* actual */)
}

func TestStoreTimeseriesIgnoreEmptyNamespace(t *testing.T) {
	// Arrange
	flatten := false
	ignoreEmptyNS := true
	fakeSink := NewFakeSink(flatten, ignoreEmptyNS)

	smd := sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	type Labels map[string]string

	labels0 := Labels{
		"spooky":                        "notvisible",
		sink_api.LabelHostname.Key:      "localhost",
		sink_api.LabelContainerName.Key: "docker",
	}

	labels1 := Labels{
		"spooky":                       "notvisible",
		sink_api.LabelHostname.Key:     "localhost",
		sink_api.LabelPodNamespace.Key: "namespace",
	}

	point0 := sink_api.Point{Name: "test/metric/1", Labels: labels0, Start: time.Now(), End: time.Now(), Value: int64(123456)}
	point1 := sink_api.Point{Name: "test/metric/2", Labels: labels1, Start: time.Now(), End: time.Now(), Value: int64(123456)}

	ts := []sink_api.Timeseries{
		sink_api.Timeseries{MetricDescriptor: &smd, Point: &point0},
		sink_api.Timeseries{MetricDescriptor: &smd, Point: &point1},
	}

	// Act
	err := fakeSink.StoreTimeseries(ts)

	// Assert
	assert.NoError(t, err)

	// Filename
	filename := GetRRDLocation(ts[1], false)

	// Data - unix_time:value
	data := fmt.Sprintf("%v:%v", point1.End.Unix(), point1.Value)

	// Create calls - one per filename
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedCreateCalls) /* actual */)
	for key, value := range fakeSink.fakeClient.capturedCreateCalls {
		assert.Equal(t, filename /* expected */, key /* actual */)
		assert.Equal(t, 1 /* expected */, value /* actual */)
		assert.Equal(t, true /* expected */, strings.Contains(key, point1.Name) /* actual */)
	}

	// Update calls
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls) /* actual */)
	assert.Equal(t, filename /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].filename /* actual */)
	assert.Equal(t, 1 /* expected */, len(fakeSink.fakeClient.capturedWriteCalls[0].values) /* actual */)
	assert.Equal(t, data /* expected */, fakeSink.fakeClient.capturedWriteCalls[0].values[0] /* actual */)
}
