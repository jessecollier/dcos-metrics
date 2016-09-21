package main

import (
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	testConfig := defaultConfig()

	testFileType := reflect.TypeOf(testConfig)
	if testFileType.Name() != "ConfigFile" {
		t.Error("defaultConfig() should return ConfigFile type, got", testFileType.Name())
	}

	if !testConfig.PollAgentEnabled {
		t.Error("Expected agent polling to be enabled by default")
	}

	if !testConfig.HttpProfilerEnabled {
		t.Error("Expected HTTP profiler to be enabled by default")
	}

	if !testConfig.KafkaFlagEnabled {
		t.Error("Expected Kafka flag to be enabled by default")
	}
}

func TestSetFlags(t *testing.T) {
	testConfig := ConfigFile{
		ConfigPath: "/some/default/path",
	}
	testFS := flag.NewFlagSet("", flag.PanicOnError)
	testConfig.setFlags(testFS)
	testFS.Parse([]string{"-config", "/another/config/path"})

	if testConfig.ConfigPath != "/another/config/path" {
		t.Error("Expected /another/config/path for config path, got", testConfig.ConfigPath)
	}
}

func TestLoadConfig(t *testing.T) {
	configContents := []byte(`
poll_agent: false
http_profiler_enabled: false
kafka_flag_enabled: false`)

	tmpConfig, err := ioutil.TempFile("", "testConfig")
	if err != nil {
		panic(err)
	}

	defer os.Remove(tmpConfig.Name())

	if _, err := tmpConfig.Write(configContents); err != nil {
		panic(err)
	}

	testConfig := ConfigFile{
		ConfigPath: tmpConfig.Name(),
	}

	loadErr := testConfig.loadConfig()

	if loadErr != nil {
		t.Error("Expected no errors loading config, got", loadErr.Error())
	}

	if testConfig.PollAgentEnabled {
		t.Error("Expected all false config, got", testConfig.PollAgentEnabled)
	}

	if testConfig.HttpProfilerEnabled {
		t.Error("Expected all false config, got", testConfig.HttpProfilerEnabled)
	}

	if testConfig.KafkaFlagEnabled {
		t.Error("Expected all false config, got", testConfig.KafkaFlagEnabled)
	}
}
