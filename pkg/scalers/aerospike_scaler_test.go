package scalers

import (
	"context"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type parseAerospikeMetadataTestData struct {
	name       string
	metadata   map[string]string
	authParams map[string]string
	isError    bool
}

type parseAerospikeTLSTestData struct {
	name       string
	authParams map[string]string
	isError    bool
}

type aerospikeMetricIdentifier struct {
	name             string
	metadataTestData *parseAerospikeMetadataTestData
	triggerIndex     int
	metricName       string
}

var testAerospikeMetadata = []parseAerospikeMetadataTestData{
	{
		name:       "nothing passed",
		metadata:   map[string]string{},
		authParams: map[string]string{},
		isError:    true,
	},
	{
		name:       "everything passed",
		metadata:   aerospikeMetaData,
		authParams: map[string]string{"password": "aerospike"},
		isError:    false,
	},
	{
		name: "no command",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"aerospikePort":        "3000",
			"threshold":            "1",
			"metricName":           "test-metric",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no threshold",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"aerospikePort":        "3000",
			"metricName":           "test-metric",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no username",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"aerospikePort":        "3000",
			"threshold":            "1",
			"metricName":           "test-metric",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no aerospikePort",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"threshold":            "1",
			"metricName":           "test-metric",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no aerospikeHost",
		metadata: map[string]string{
			"aerospikePort":        "3000",
			"threshold":            "1",
			"metricName":           "test-metric",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no metricName",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"aerospikePort":        "3000",
			"threshold":            "1",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{"password": "aerospike"},
		isError:    true,
	},
	{
		name: "no password",
		metadata: map[string]string{
			"aerospikeHost":        "127.0.0.1",
			"aerospikePort":        "3000",
			"threshold":            "1",
			"metricName":           "test-metric",
			"userName":             "aerospike",
			"useServicesAlternate": "true",
			"connTimeout":          "1000",
			"command":              "statistics",
		},
		authParams: map[string]string{},
		isError:    true,
	},
}

var testAerospikeMetadataWithTLS = []parseAerospikeTLSTestData{
	{
		name: "success with cert/key",
		authParams: map[string]string{
			"tlsName":     "aerospike-tls",
			"cert":        "test-cert",
			"key":         "test-key",
			"keyPassword": "aerospike",
			"ca":          "test-ca",
		},
		isError: false,
	},
	{
		name: "failure missing cert",
		authParams: map[string]string{
			"tlsName":     "aerospike-tls",
			"key":         "test-key",
			"keyPassword": "aerospike",
			"ca":          "test-ca",
		},
		isError: true,
	},
	{
		name: "failure missing key",
		authParams: map[string]string{
			"tlsName":     "aerospike-tls",
			"cert":        "test-cert",
			"keyPassword": "aerospike",
			"ca":          "test-ca",
		},
		isError: true,
	},
}

var aerospikeMetricIdentifiers = []aerospikeMetricIdentifier{
	{
		name:             "everything passed",
		metadataTestData: &testAerospikeMetadata[1],
		triggerIndex:     0,
		metricName:       "s0-aerospike-test-metric",
	},
}

var aerospikeMetaData = map[string]string{
	"aerospikeHost":        "127.0.0.1",
	"aerospikePort":        "3000",
	"threshold":            "1",
	"metricName":           "test-metric",
	"userName":             "aerospike",
	"useServicesAlternate": "true",
	"connTimeout":          "1000",
	"command":              "statistics",
}

func TestAerospikeParseMetadata(t *testing.T) {
	for _, testData := range testAerospikeMetadata {
		t.Run(testData.name, func(t *testing.T) {
			_, err := parseAerospikeMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: testData.metadata,
				AuthParams:      testData.authParams,
			})
			if err != nil && !testData.isError {
				t.Error("Expected success but got error", err)
			}
			if testData.isError && err == nil {
				t.Error("Expected error but got success")
			}
		})
	}
}

func TestAerospikeGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range aerospikeMetricIdentifiers {
		t.Run(testData.name, func(t *testing.T) {
			meta, err := parseAerospikeMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: testData.metadataTestData.metadata,
				TriggerIndex:    testData.triggerIndex,
				AuthParams:      testData.metadataTestData.authParams,
			})
			if err != nil {
				t.Fatal("Could not parse metadata:", err)
			}
			mockAerospikeScaler := aerospikeScaler{
				metadata:    meta,
				connections: map[string]*as.Connection{},
				policy:      &as.ClientPolicy{},
				client:      &as.Client{},
				logger:      logr.Discard(),
			}

			metricSpec := mockAerospikeScaler.GetMetricSpecForScaling(context.Background())
			metricName := metricSpec[0].External.Metric.Name
			assert.Equal(t, testData.metricName, metricName)
		})
	}
}

func TestParseAerospikeTLS(t *testing.T) {
	for _, testData := range testAerospikeMetadataWithTLS {
		t.Run(testData.name, func(t *testing.T) {
			_, err := parseAerospikeMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: aerospikeMetaData,
				AuthParams:      testData.authParams,
			})

			if testData.isError {
				assert.Error(t, err)
			}
		})
	}
}
