// +build unit

package gocql

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const testShardCount = 3

func TestShardAwarePortMockedNoReconnections(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortNoReconnections)
}

func TestShardAwarePortMockedMaliciousNAT(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortMaliciousNAT)
}

func TestShardAwarePortMockedUnusedIfNotEnabled(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortUnusedIfNotEnabled)
}

func testWithAndWithoutTLS(t *testing.T, test func(t *testing.T, makeCluster makeClusterTestFunc)) {
	t.Run("without TLS", func(t *testing.T) {
		makeCluster, stop := startServerWithShardAwarePort(t, false)
		defer stop()
		test(t, makeCluster)
	})

	t.Run("with TLS", func(t *testing.T) {
		makeCluster, stop := startServerWithShardAwarePort(t, true)
		defer stop()
		test(t, makeCluster)
	})
}

func startServerWithShardAwarePort(t testing.TB, useTLS bool) (makeCluster func() *ClusterConfig, stop func()) {
	var shardAwarePort int32

	shardAwarePortKey := "SCYLLA_SHARD_AWARE_PORT"
	if useTLS {
		shardAwarePortKey = "SCYLLA_SHARD_AWARE_PORT_SSL"
	}

	regularSupportedFactory := func(conn net.Conn) map[string][]string {
		// Assign a random shard. Although Scylla uses a slightly more sophisticated
		// algorithm for shard assignment, the driver should not make any assumptions.
		saPort := int(atomic.LoadInt32(&shardAwarePort))

		t.Log("Connecting to the regular port")

		shardID := rand.Intn(testShardCount)
		supported := getStandardScyllaExtensions(shardID, testShardCount)
		supported[shardAwarePortKey] = []string{strconv.Itoa(saPort)}
		return supported
	}

	shardAwareSupportedFactory := func(conn net.Conn) map[string][]string {
		// Shard ID depends on the source port.
		_, portS, _ := net.SplitHostPort(conn.RemoteAddr().String())
		port, _ := strconv.Atoi(portS)
		shardID := scyllaShardForSourcePort(uint16(port), testShardCount)

		saPort := int(atomic.LoadInt32(&shardAwarePort))

		t.Log("Connecting to the shard-aware port")

		supported := getStandardScyllaExtensions(shardID, testShardCount)
		supported[shardAwarePortKey] = []string{strconv.Itoa(saPort)}
		return supported
	}

	makeServer := func(factory testSupportedFactory) *TestServer {
		if useTLS {
			return NewSSLTestServerWithSupportedFactory(t,
				defaultProto, context.Background(), factory)
		}
		return NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t,
			defaultProto, context.Background(), factory)
	}

	srvRegular := makeServer(regularSupportedFactory)
	srvShardAware := makeServer(shardAwareSupportedFactory)

	_, portS, _ := net.SplitHostPort(srvShardAware.Address)
	saPort, _ := strconv.Atoi(portS)
	atomic.StoreInt32(&shardAwarePort, int32(saPort))

	t.Logf("regular port address: %s, shard aware port address: %s",
		srvRegular.Address, srvShardAware.Address)

	makeCluster = func() *ClusterConfig {
		var cluster *ClusterConfig
		if useTLS {
			cluster = createTestSslCluster(srvRegular.Address, defaultProto, false)

			// Give a long timeout. For some reason, closing tls connections
			// result in an i/o timeout error, and this mitigates this problem.
			cluster.Timeout = 1 * time.Minute
		} else {
			cluster = testCluster(defaultProto, srvRegular.Address)
		}
		return cluster
	}

	stop = func() {
		srvRegular.Stop()
		srvShardAware.Stop()
	}

	return makeCluster, stop
}

func getStandardScyllaExtensions(shardID, shardCount int) map[string][]string {
	return map[string][]string{
		"SCYLLA_SHARD":               []string{strconv.Itoa(shardID)},
		"SCYLLA_NR_SHARDS":           []string{strconv.Itoa(shardCount)},
		"SCYLLA_PARTITIONER":         []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
		"SCYLLA_SHARDING_ALGORITHM":  []string{"biased-token-round-robin"},
		"SCYLLA_SHARDING_IGNORE_MSB": []string{"12"},
	}
}
