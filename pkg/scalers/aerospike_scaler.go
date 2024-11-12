package scalers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type aerospikeScaler struct {
	metadata *aerospikeMetadata
	logger   logr.Logger
	client   *as.Client
	policy   *as.ClientPolicy
}

type aerospikeMetadata struct {
	AerospikeHost        string `keda:"name=aerospikeHost,        order=triggerMetadata"`
	AerospikePort        int    `keda:"name=aerospikePort,        order=triggerMetadata"`
	Threshold            int64  `keda:"name=threshold,            order=triggerMetadata"`
	MetricName           string `keda:"name=metricName,           order=triggerMetadata"`
	UserName             string `keda:"name=userName,             order=triggerMetadata"`
	Password             string `keda:"name=password,             order=authParams"`
	UseServicesAlternate bool   `keda:"name=useServicesAlternate, order=triggerMetadata"`
	ConnTimeout          int64  `keda:"name=connTimeout,          order=triggerMetadata"`
	Command              string `keda:"name=command,              order=triggerMetadata"`

	// TLS
	TLSName     string `keda:"name=tlsName,     order=triggerMetadata, optional"`
	Cert        string `keda:"name=cert,        order=authParams,      optional"`
	Key         string `keda:"name=key,         order=authParams,      optional"`
	KeyPassword string `keda:"name=keyPassword, order=authParams,      optional"`
	CA          string `keda:"name=ca,          order=authParams,      optional"`
}

func (s *aerospikeMetadata) Validate() error {
	if s.TLSName != "" {
		if s.Cert == "" || s.Key == "" {
			return fmt.Errorf("cert and key are required for TLS")
		}
	}

	return nil
}

// NewAerospikeScaler creates a new scaler for Aerospike
func NewAerospikeScaler(config *scalersconfig.ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "aerospike_scaler")

	metadata, err := parseAerospikeMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing Aerospike metadata: %s", err)
	}

	policy := getClientPolicy(metadata)

	host := as.Host{
		Name:    metadata.AerospikeHost,
		Port:    metadata.AerospikePort,
		TLSName: metadata.TLSName,
	}
	client, err := as.NewClientWithPolicyAndHost(policy, &host)
	if err != nil {
		return nil, fmt.Errorf("error creating Aerospike client: %s", err)
	}

	return &aerospikeScaler{
		metadata: metadata,
		client:   client,
		policy:   policy,
		logger:   logger,
	}, nil
}

// GetMetricSpecForScaling returns the metric specifications for scaling
func (s *aerospikeScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	targetValue := resource.NewQuantity(s.metadata.Threshold, resource.DecimalSI)

	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: s.metadata.MetricName,
		},
		Target: v2.MetricTarget{
			Type:  v2.ValueMetricType,
			Value: targetValue,
		},
	}

	metricSpec := v2.MetricSpec{
		External: externalMetric,
		Type:     v2.ExternalMetricSourceType,
	}

	return []v2.MetricSpec{metricSpec}
}

func parseAerospikeMetadata(config *scalersconfig.ScalerConfig) (*aerospikeMetadata, error) {
	meta := aerospikeMetadata{}
	err := config.TypedConfig(&meta)
	if err != nil {
		return nil, fmt.Errorf("error parsing aerospike metadata: %w", err)
	}

	return &meta, nil
}

// GetMetricsAndActivity fetches external metrics and determines if scaling is needed
func (s *aerospikeScaler) GetMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	// Fetch the current number of records from Aerospike
	records, err := s.getAerospikeStats()
	if err != nil {
		return nil, false, err
	}

	// Prepare the metric value to return
	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(records, resource.DecimalSI),
	}

	// Return the metric, and scaling is always active
	return []external_metrics.ExternalMetricValue{metric}, true, nil
}

// Close closes the Aerospike client
func (s *aerospikeScaler) Close(_ context.Context) error {
	s.client.Close()

	return nil
}

func getClientPolicy(metadata *aerospikeMetadata) *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.User = metadata.UserName
	policy.Password = metadata.Password
	policy.UseServicesAlternate = metadata.UseServicesAlternate
	// Consider exposing this as a configurable parameter.
	if metadata.ConnTimeout != 0 {
		policy.Timeout = time.Duration(metadata.ConnTimeout) * time.Second
	}

	// tls config
	if metadata.TLSName != "" {
		tlsConfig, err := kedautil.NewTLSConfigWithPassword(metadata.Cert, metadata.Key, metadata.KeyPassword, metadata.CA, false)
		if err != nil {
			logrus.Errorf("Error creating TLS config: %v", err)
		}
		policy.TlsConfig = tlsConfig
	}

	return policy
}

func (s *aerospikeScaler) createNewConnection(asServerHost *as.Host) (*as.Connection, error) {
	asConnection, err := as.NewConnection(s.policy, asServerHost)
	if err != nil {
		return nil, err
	}

	if s.policy.RequiresAuthentication() {
		if err := asConnection.Login(s.policy); err != nil {
			return nil, err
		}
	}

	// Set no connection deadline to re-use connection, but socketTimeout will be in effect
	var deadline time.Time
	err = asConnection.SetTimeout(deadline, s.policy.Timeout)
	if err != nil {
		return nil, err
	}

	return asConnection, nil
}

// getAerospikeStats fetches the current number of records in the specified Aerospike namespace
func (s *aerospikeScaler) getAerospikeStats() (int64, error) {
	sumVal := 0

	nodes := s.client.GetNodes()
	for _, node := range nodes {
		host := node.GetHost()
		conn, err := s.createNewConnection(host)
		if err != nil {
			return 0, err
		}

		result, err := fetchRequestInfoFromAerospike(s.logger, []string{s.metadata.Command}, conn)
		if err != nil {
			return 0, fmt.Errorf("error fetching info from Aerospike: %s", err)
		}

		val := parseStatValue(result[s.metadata.Command], s.metadata.MetricName)

		valInt, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, err
		}

		sumVal += int(valInt)

		conn.Close()
	}

	return int64(sumVal / len(nodes)), nil
}

// Helper function to parse specific stat value from the info response
func parseStatValue(stats string, statName string) string {
	parts := strings.Split(stats, ";")
	for _, part := range parts {
		statParts := strings.Split(part, "=")
		if len(statParts) == 2 && statParts[0] == statName {
			return statParts[1]
		}
	}
	return ""
}

func fetchRequestInfoFromAerospike(logger logr.Logger, infoKeys []string, asConnection *as.Connection) (map[string]string, error) {
	var err error
	rawMetrics := make(map[string]string)
	retryCount := 3

	if asConnection == nil {
		return nil, errors.New("aerospike connection is nil")
	}

	// Retry for connection, timeout, network errors
	// including errors from RequestInfo()
	for i := 0; i < retryCount; i++ {
		// Validate existing connection
		if !asConnection.IsConnected() {
			logger.Info("Error while connecting to aerospike server, reconnecting")
			continue
		}

		// Info request
		rawMetrics, err = asConnection.RequestInfo(infoKeys...)
		if err != nil {
			logrus.Debug("Error while requestInfo ( infoKeys...), closing connection : Error is: ", err, " and infoKeys: ", infoKeys)
			asConnection.Close()
			//TODO: do we need to assign nil to asConnection? i.e. asConnection = nil
			continue
		}

		break
	}

	if len(rawMetrics) == 1 {
		for k := range rawMetrics {
			if strings.HasPrefix(strings.ToUpper(k), "ERROR:") {
				return nil, errors.New(k)
			}
		}
	}

	return rawMetrics, err
}
