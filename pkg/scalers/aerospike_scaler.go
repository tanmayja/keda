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
)

type aerospikeScaler struct {
	metadata *aerospikeMetadata
	logger   logr.Logger
	client   *as.Client
	policy   *as.ClientPolicy
}

type aerospikeMetadata struct {
	aerospikeHost        string
	aerospikePort        int
	namespace            string
	threshold            int64
	metricName           string
	userName             string
	password             string
	useServicesAlternate bool
	connTimeout          int64
	command              string
}

// NewAerospikeScaler creates a new scaler for Aerospike
func NewAerospikeScaler(config *scalersconfig.ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "aerospike_scaler")

	metadata, err := parseAerospikeMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing Aerospike metadata: %s", err)
	}

	policy := getClientPolicy(metadata)

	client, err := as.NewClientWithPolicy(policy, metadata.aerospikeHost, metadata.aerospikePort)
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
	targetValue := resource.NewQuantity(s.metadata.threshold, resource.DecimalSI)

	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: s.metadata.metricName,
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
	// Extracting metadata like host, port, namespace, and threshold
	host := config.TriggerMetadata["aerospikeHost"]
	port, err := strconv.ParseInt(config.TriggerMetadata["aerospikePort"], 10, 64)
	if err != nil {
		return nil, err
	}

	threshold, err := strconv.ParseInt(config.TriggerMetadata["threshold"], 10, 64)
	if err != nil {
		return nil, err
	}

	connTimeout, err := strconv.ParseInt(config.TriggerMetadata["connTimeout"], 10, 64)
	if err != nil {
		return nil, err
	}

	useServicesAlternate, err := strconv.ParseBool(config.TriggerMetadata["useServicesAlternate"])
	if err != nil {
		return nil, err
	}

	return &aerospikeMetadata{
		aerospikeHost:        host,
		aerospikePort:        int(port),
		namespace:            config.TriggerMetadata["namespace"],
		threshold:            threshold,
		metricName:           config.TriggerMetadata["metricName"],
		userName:             config.TriggerMetadata["userName"],
		password:             config.AuthParams["password"],
		useServicesAlternate: useServicesAlternate,
		connTimeout:          connTimeout,
		command:              config.TriggerMetadata["command"],
	}, nil
}

// GetMetricsAndActivity fetches external metrics and determines if scaling is needed
func (s *aerospikeScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
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

	// Determine if scaling should be active (e.g., records >= threshold)
	isActive := records >= s.metadata.threshold

	// Return the metric, and whether scaling should be active
	return []external_metrics.ExternalMetricValue{metric}, isActive, nil
}

// Close closes the Aerospike client
func (s *aerospikeScaler) Close(_ context.Context) error {
	s.client.Close()

	return nil
}

func getClientPolicy(metadata *aerospikeMetadata) *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.User = metadata.userName
	policy.Password = metadata.password
	policy.UseServicesAlternate = metadata.useServicesAlternate
	// Consider exposing this as a configurable parameter.
	if metadata.connTimeout != 0 {
		policy.Timeout = time.Duration(metadata.connTimeout) * time.Second
	}
	// TODO: Implement TLS policy configuration based on cluster requirements.

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

		result, err := fetchRequestInfoFromAerospike([]string{s.metadata.command}, conn)
		if err != nil {
			return 0, err
		}

		val := parseStatValue(result[s.metadata.command], s.metadata.metricName)

		fmt.Printf("Result node name: %s: %v\n", node.GetName(), val)

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

func fetchRequestInfoFromAerospike(infoKeys []string, asConnection *as.Connection) (map[string]string, error) {
	var err error
	rawMetrics := make(map[string]string)
	retryCount := 3

	// Retry for connection, timeout, network errors
	// including errors from RequestInfo()
	for i := 0; i < retryCount; i++ {
		// Validate existing connection
		if asConnection == nil || !asConnection.IsConnected() {
			logrus.Debug("Error while connecting to aerospike server: ", err)
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
