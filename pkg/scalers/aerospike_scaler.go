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
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type aerospikeScaler struct {
	metadata    *aerospikeMetadata
	logger      logr.Logger
	client      *as.Client
	policy      *as.ClientPolicy
	connections map[string]*as.Connection
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
	if s.TLSName != "" && (s.Cert == "" || s.Key == "") {
		return fmt.Errorf("cert and key are required for TLS")
	}

	allowedPrefixes := []string{"statistics", "namespace", "get-stats"}
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(s.Command, prefix) {
			return nil
		}
	}

	return fmt.Errorf("command must start with one of the following prefixes: %v", allowedPrefixes)
}

// NewAerospikeScaler creates a new scaler for Aerospike
func NewAerospikeScaler(config *scalersconfig.ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "aerospike_scaler")

	metadata, err := parseAerospikeMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing Aerospike metadata: %s", err.Error())
	}

	policy := getClientPolicy(logger, metadata)

	client, err := as.NewClientWithPolicyAndHost(policy, &as.Host{
		Name:    metadata.AerospikeHost,
		Port:    metadata.AerospikePort,
		TLSName: metadata.TLSName,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Aerospike client: %s", err.Error())
	}

	return &aerospikeScaler{
		metadata:    metadata,
		client:      client,
		policy:      policy,
		logger:      logger,
		connections: make(map[string]*as.Connection),
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

// parseAerospikeMetadata parses and validates scaler metadata
func parseAerospikeMetadata(config *scalersconfig.ScalerConfig) (*aerospikeMetadata, error) {
	meta := aerospikeMetadata{}
	if err := config.TypedConfig(&meta); err != nil {
		return nil, fmt.Errorf("error parsing aerospike metadata: %w", err)
	}

	return &meta, nil
}

// GetMetricsAndActivity fetches external metrics and determines if scaling is needed
func (s *aerospikeScaler) GetMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	records, err := s.getAerospikeStats()
	if err != nil {
		return nil, false, err
	}

	// Prepare the metric value to return
	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(records, resource.DecimalSI),
	}

	return []external_metrics.ExternalMetricValue{metric}, true, nil
}

// Close closes the Aerospike client
func (s *aerospikeScaler) Close(_ context.Context) error {
	for _, conn := range s.connections {
		if conn != nil {
			conn.Close()
		}
	}

	s.client.Close()

	return nil
}

// createClientPolicy initializes the client policy with configuration and TLS
func getClientPolicy(logger logr.Logger, metadata *aerospikeMetadata) *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.User = metadata.UserName
	policy.Password = metadata.Password
	policy.UseServicesAlternate = metadata.UseServicesAlternate
	if metadata.ConnTimeout != 0 {
		policy.Timeout = time.Duration(metadata.ConnTimeout) * time.Second
	}

	// tls config
	if metadata.TLSName != "" {
		tlsConfig, err := kedautil.NewTLSConfigWithPassword(metadata.Cert, metadata.Key, metadata.KeyPassword, metadata.CA, false)
		if err != nil {
			logger.Info("Error creating TLS config: %v", err)
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

// getAerospikeStats fetches the average value of the specified Aerospike metric
func (s *aerospikeScaler) getAerospikeStats() (int64, error) {
	var sum int

	nodes := s.client.GetNodes()
	for _, node := range nodes {
		result, err := s.fetchRequestInfoFromAerospike(node)
		if err != nil {
			return 0, fmt.Errorf("error fetching info from Aerospike: %s", err)
		}

		val, err := parseStatValue(result[s.metadata.Command], s.metadata.MetricName)
		if err != nil {
			return 0, fmt.Errorf("error parsing stat value: %s", err)
		}

		sum += int(val)
	}

	return int64(sum / len(nodes)), nil
}

// Helper function to parse specific stat value from the info response
func parseStatValue(stats string, metricName string) (int64, error) {
	for _, part := range strings.Split(stats, ";") {
		statParts := strings.SplitN(part, "=", 2)
		if len(statParts) != 2 {
			continue // Skip invalid formats
		}

		if statParts[0] == metricName {
			valInt, err := strconv.ParseInt(statParts[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse metric value: %v", err)
			}
			return valInt, nil
		}
	}

	return 0, fmt.Errorf("metric not found: %s", metricName)
}

func (s *aerospikeScaler) fetchRequestInfoFromAerospike(node *as.Node) (map[string]string, error) {
	var err error
	rawMetrics := make(map[string]string)
	retryCount := 3

	// Retry for connection, timeout, network errors
	// including errors from RequestInfo()
	for i := 0; i < retryCount; i++ {
		conn := s.connections[node.GetName()]
		if conn == nil || !conn.IsConnected() {
			newConn, err := s.createNewConnection(node.GetHost())
			if err != nil {
				s.logger.Info("Error while creating new connection, retrying", "Error is: ", err.Error())
				return nil, err
			}

			s.connections[node.GetName()] = newConn
		}
		// Info request
		rawMetrics, err = s.connections[node.GetName()].RequestInfo(s.metadata.Command)
		if err != nil {
			s.logger.Info("Error while requestInfo closing connection and retrying", "Error is: ", err.Error(), "infoKey: ", s.metadata.Command)
			s.connections[node.GetName()].Close()

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
