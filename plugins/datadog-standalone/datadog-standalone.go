// Copyright 2017 Mesosphere, Inc.
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

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"
	plugin "github.com/dcos/dcos-metrics/plugins"
	"github.com/dcos/dcos-metrics/producers"
	"github.com/urfave/cli"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	pluginFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "datadog-key",
			Usage: "DataDog API Key",
		},
		cli.BoolFlag{
			Name:  "vvv",
			Usage: "Enable printing metrics json for debug",
		},
		cli.StringSliceFlag{
			Name:  "tag, t",
			Usage: "extra tags",
		},
		cli.StringFlag{
			Name:  "extra-tags-file, e",
			Usage: "A file containing k=v or k:v one per line to add as tags to metrics",
		},
		cli.StringFlag{
			Name:  "metric-prefix",
			Usage: "Datadog metric name prefix",
		},
	}
)

// DDDataPoint is a tuple of [UNIX timestamp, value]. This has to use floats
// because the value could be non-integer.
type DDDataPoint [2]float64

// DDMetric represents a collection of data points that we might send or receive
// on one single metric line.
type DDMetric struct {
	Metric string        `json:"metric,omitempty"`
	Points []DDDataPoint `json:"points,omitempty"`
	Type   *string       `json:"type,omitempty"`
	Host   *string       `json:"host,omitempty"`
	Tags   []string      `json:"tags,omitempty"`
	Unit   string        `json:"unit,omitempty"`
}

// DDSeries represents a collection of data points we get when we query for timeseries data
type DDSeries struct {
	Series []DDMetric `json:"series"`
}

// DDResult represents the result from a DataDog API Query
type DDResult struct {
	Status   string   `json:"status,omitempty"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}



func main() {
	log.Info("Starting Standalone DataDog DC/OS metrics plugin")
	datadogPlugin, err := plugin.New(
		plugin.Name("standalone-datadog"),
		plugin.ExtraFlags(pluginFlags),
		plugin.ConnectorFunc(datadogConnector))

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(datadogPlugin.StartPlugin())
}

func datadogConnector(metrics []producers.MetricsMessage, c *cli.Context) error {
	if len(metrics) == 0 {
		log.Info("No messages received from metrics service")
		return nil
	}

	log.Info("Transmitting metrics to DataDog")
	datadogURL := fmt.Sprintf("https://app.datadoghq.com/api/v1/series?api_key=%s", c.String("datadog-key"))

	result, err := postMetricsToDatadog(datadogURL, metrics, c)
	if err != nil {
		log.Errorf("Unexpected error while processing DataDog response: %s", err)
		return nil
	}

	if len(result.Errors) > 0 {
		log.Error("Encountered errors:")
		for _, err := range result.Errors {
			log.Error(err)
		}
	}
	if len(result.Warnings) > 0 {
		log.Warn("Encountered warnings:")
		for _, wrn := range result.Warnings {
			log.Warn(wrn)
		}
	}
	if result.Status == "ok" {
		log.Info("Successfully transmitted metrics")
	} else if result.Status != "" {
		log.Warnf("Expected status to be ok, actually: %v", result.Status)
	}

	return nil
}

func postMetricsToDatadog(datadogURL string, metrics []producers.MetricsMessage, c *cli.Context) (*DDResult, error) {
	series := messagesToSeries(metrics, c)
	b := new(bytes.Buffer)
	err := json.NewEncoder(b).Encode(series)
	if err != nil {
		return nil, fmt.Errorf("Could not encode metrics to JSON: %v", err)
	}

	if c.Bool("vvv") {
		log.Warnf("Json metrics '%v'", b)
	}

	res, err := http.Post(datadogURL, "application/json; charset=utf-8", b)
	if err != nil {
		return nil, fmt.Errorf("Could not post payload to DataDog: %v", err)
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Could not read response: %v", err)
	}

	result := DDResult{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Warnf("Could not unmarshal datadog response %s: %s", body, err)
		return nil, err
	}
	return &result, nil
}

func messagesToSeries(messages []producers.MetricsMessage, c *cli.Context) *DDSeries {
	series := new(DDSeries)

	for _, message := range messages {
		dimensions := &message.Dimensions
		host := &dimensions.Hostname
		messageTags := []string{}
		addMessageTag := func(key, value string) {
			if len(value) > 0 {
				messageTags = append(messageTags, fmt.Sprintf("%s:%s", key, value))
			}
		}

		// Disable because we dont want labels leaking to DD
		// if c.Bool('enable-labels') {
		// 	for name, value := range dimensions.Labels {
		// 		addMessageTag(name, value)
		// 	}
		// }

		// the first string before . is our marathon name
		executorName := strings.Split(dimensions.ExecutorID, ".")
		addMessageTag("service", executorName[0])

		// Here to add instance id
		ec2client := ec2metadata.New(session.New())
		ec2InstanceIdentifyDocument, _ := ec2client.GetInstanceIdentityDocument()
		addMessageTag("instance_id", ec2InstanceIdentifyDocument.InstanceID)

		// Disabled due to high cardinality
		//addMessageTag("mesosId", dimensions.MesosID)
		//addMessageTag("clusterId", dimensions.ClusterID)
		//addMessageTag("containerId", dimensions.ContainerID)
		//addMessageTag("executorId", dimensions.ExecutorID)
		//addMessageTag("frameworkName", dimensions.FrameworkName)
		//addMessageTag("frameworkId", dimensions.FrameworkID)
		//addMessageTag("frameworkRole", dimensions.FrameworkRole)
		//addMessageTag("frameworkPrincipal", dimensions.FrameworkPrincipal)
		addMessageTag("slave_ip", dimensions.Hostname)

		// Look for extra k=v or k:v in a file and add them to tags
		if len(c.String("extra-tags-file")) > 0 {
			messageTags = append(messageTags, extraTagsFromFile(c)...)
		}

		for _, tag := range c.StringSlice("tag") {
			if len(tag) > 0 {
				messageTags = append(messageTags, tag)
			}
		}

		for _, datapoint := range message.Datapoints {
			m, err := datapointToDDMetric(datapoint, messageTags, host, c)
			if err != nil {
				log.Error(err)
				continue
			}
			series.Series = append(series.Series, *m)
		}
	}

	return series
}

// Import tags from KV file
// file must be newline separated k=v or k:v
func extraTagsFromFile(c *cli.Context) (extraTags []string) {
	extra, err := ioutil.ReadFile(c.String("extra-tags-file"))
	if err != nil {
		return extraTags
	}

	var validEquals = regexp.MustCompile(`^[a-zA-Z0-9_]+=[a-zA-Z0-9]+$`)
	var validColon = regexp.MustCompile(`^[a-zA-Z0-9_]+:[a-zA-Z0-9]+$`)

	scanner := bufio.NewScanner(strings.NewReader(string(extra)))
	for scanner.Scan() {
		switch {
		case validEquals.MatchString(scanner.Text()):
			extraTags = append(extraTags, strings.Replace(scanner.Text(), "=", ":", -1))
		case validColon.MatchString(scanner.Text()):
			extraTags = append(extraTags, scanner.Text())
		}
	}
	return extraTags
}

func datapointToDDMetric(datapoint producers.Datapoint, messageTags []string, host *string, c *cli.Context) (*DDMetric, error) {
	t, err := plugin.ParseDatapointTimestamp(datapoint.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("Could not parse timestamp '%s': %v", datapoint.Timestamp, err)
	}

	v, err := plugin.DatapointValueToFloat64(datapoint.Value)
	if err != nil {
		return nil, err
	}

	datapointTags := []string{}
	// addDatapointTag := func(key, value string) {
	// 	datapointTags = append(datapointTags, fmt.Sprintf("%s:%s", key, value))
	// }

	// Disable datapoint tags due to high cardinality
	// see collectors/mesos/agent/metrics.go for what tags are excluded
	//    -JC 11/6/17
	// for name, value := range datapoint.Tags {
	// 	addDatapointTag(name, value)
	// }

	metricName := datapoint.Name
	if len(c.String("metric-prefix")) > 0 {
		metricName = strings.Join([]string{c.String("metric-prefix"), metricName}, ".")
	}

	metric := DDMetric{
		Metric: metricName,
		Points: []DDDataPoint{{float64(t.Unix()), v}},
		Tags:   append(messageTags, datapointTags...),
		Unit:   datapoint.Unit,
		Host:   host,
	}
	return &metric, nil
}
