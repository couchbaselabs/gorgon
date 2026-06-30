package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
	"github.com/couchbaselabs/gorgon/src/gorgon/nemeses"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
	"github.com/couchbaselabs/gorgon/src/gorgon/workloads"
)

// Flags initialized in main.go
type DatabaseConfig struct {
	User           *string
	Pass           *string
	Port           *int
	Replicas       *int
	Durability     *string
	Timeout        *time.Duration
	ClientOverRpc  *bool
	StorageEngine  *string
	Vbuckets       *int
	EvictionPolicy *string
}

func NewDatabase(config DatabaseConfig) gorgon.Database {
	return &database{config: config}
}

type database struct {
	config     DatabaseConfig
	options    *gorgon.Options
	durability gocb.DurabilityLevel
}

func (*database) Name() string {
	return "couchbase"
}

func (db *database) SetOptions(opt *gorgon.Options) error {
	db.options = opt
	if durability := *db.config.Durability; len(durability) != 0 {
		db.durability = parseDurabilityLevel(durability)
		if db.durability == gocb.DurabilityLevelUnknown {
			return fmt.Errorf("kv: invalid durability level %q", durability)
		}
	}
	if n := *db.config.Replicas; n < 0 || n > 3 {
		return fmt.Errorf("kv: invalid number of replicas %d", n)
	}
	// validate if eviction policy is of the 2 valid types for a couchbase bucket
	evictionType := *db.config.EvictionPolicy
	if evictionType != "fullEviction" && evictionType != "valueOnly" {
		return fmt.Errorf("kv: invalid bucket eviction policy - %s", evictionType)
	}
	return nil
}

func (db *database) SetUp() error {
	opt := db.options
	user := *db.config.User
	pass := *db.config.Pass
	replicas := *db.config.Replicas
	storageEngine := *db.config.StorageEngine
	vbuckets := *db.config.Vbuckets

	for _, node := range opt.Nodes {
		if err := db.httpPost(node, "controller/hardResetNode", nil); err != nil {
			return err
		}
		if err := db.httpPost(node, "nodes/self/controller/settings", nil); err != nil {
			return err
		}
		if err := db.httpPost(node, "node/controller/rename", map[string]string{
			"hostname": node}); err != nil {
			return err
		}
	}
	if err := db.httpPost(opt.Nodes[0], "clusterInit", map[string]string{
		"hostname": opt.Nodes[0],
		"username": user,
		"password": pass,
		"services": "kv",
		"port":     "SAME"}); err != nil {
		return err
	}
	for i, node := range opt.Nodes {
		if i == 0 {
			continue
		}
		if err := db.httpPost(node, "node/controller/doJoinCluster", map[string]string{
			"hostname": opt.Nodes[0],
			"user":     user,
			"password": pass,
			"services": "kv"}); err != nil {
			return err
		}
	}
	if err := db.rebalance(db.options.Nodes[0], db.options.Nodes, nil); err != nil {
		return err
	}
	// Wait for rebalance to complete
	if err := db.waitForRebalance(db.options.Nodes[0]); err != nil {
		return err
	}

	if err := db.httpPost(opt.Nodes[0], "settings/autoFailover", map[string]string{
		"enabled":                            "true",
		"timeout":                            "15",
		"failoverPreserveDurabilityMajority": "true"}); err != nil {
		return err
	}
	if err := db.httpPost(opt.Nodes[0], "pools/default/buckets", map[string]string{
		"name":           "default",
		"ramQuota":       "1024",
		"storageBackend": storageEngine,
		"evictionPolicy": *db.config.EvictionPolicy,
		"replicaNumber":  strconv.Itoa(replicas),
		"numVBuckets":    strconv.Itoa(vbuckets),
		"flushEnabled":   "1"}); err != nil {
		return err
	}
	time.Sleep(5 * time.Second) // Wait for bucket creation

	return nil
}

func (db *database) TearDown() error {
	return nil
}

func (db *database) rebalance(apiNode string, known, ejected []string) error {
	return db.httpPost(apiNode, "controller/rebalance", map[string]string{
		"knownNodes":   formatOtpNodes(known),
		"ejectedNodes": formatOtpNodes(ejected)})
}

func (db *database) waitForRebalance(apiNode string) error {
	for {
		time.Sleep(time.Second)
		bytes, err := db.httpGet(apiNode, "pools/default/rebalanceProgress")
		if err != nil {
			return err
		}
		obj := make(map[string]interface{})
		if err := json.Unmarshal(bytes, &obj); err != nil {
			return fmt.Errorf("kv: cannot parse rebalance progress: %v", err)
		}
		status, ok := obj["status"].(string)
		if !ok {
			return fmt.Errorf("kv: cannot find rebalance status in %s", string(bytes))
		}
		if status == "none" {
			log.Info("Rebalance completed")
			break
		}
		log.Info("Rebalance in progress: %s", string(bytes))
	}
	return nil
}

func (db *database) requestAddNode(apiNode, addNode string) error {
	return db.httpPost(apiNode, "controller/addNode", map[string]string{
		"hostname": addNode,
		"user":     *db.config.User,
		"password": *db.config.Pass,
		"services": "kv",
	})
}

func (db *database) findOrchestrator(apiNode string) (string, error) {
	bytes, err := db.httpGet(apiNode, "pools/default/terseClusterInfo")
	if err != nil {
		return "", err
	}
	obj := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &obj); err != nil {
		return "", err
	}
	orchestrator, ok := obj["orchestrator"].(string)
	if !ok {
		return "", errors.New("kv: cannot parse orchestrator response body")
	}
	if orchestrator == "undefined" {
		return "", errors.New("kv: orchestrator-node not known")
	}
	return orchestrator, nil
}

func formatOtpNodes(nodes []string) string {
	var builder strings.Builder
	for i, node := range nodes {
		if i == 0 {
			builder.WriteString("ns_1@")
		} else {
			builder.WriteString(",ns_1@")
		}
		builder.WriteString(node)
	}
	return builder.String()
}

func (db *database) httpGet(node, endpoint string) ([]byte, error) {
	uri := fmt.Sprintf("http://%s:%s@%s:8091/%s", *db.config.User, *db.config.Pass, node, endpoint)
	log.Info("HTTP GET %s %s", node, endpoint)
	resp, err := http.Get(uri)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP GET returned %d: %s", resp.StatusCode, string(bytes))
	}
	return bytes, nil
}

func (db *database) httpPost(node, endpoint string, form map[string]string) error {
	values := make(url.Values, len(form))
	for k, v := range form {
		values.Set(k, v)
	}
	uri := fmt.Sprintf("http://%s:%s@%s:8091/%s", *db.config.User, *db.config.Pass, node, endpoint)
	log.Info("HTTP POST %s %s %s", node, endpoint, values.Encode())
	resp, err := http.PostForm(uri, values)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusAccepted {
			return nil
		}
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("HTTP POST returned %d: %s", resp.StatusCode, string(bytes))
	}
	return nil
}

func (db *database) NewClient(id int) (gorgon.Client, error) {
	nodes := db.options.Nodes
	if *db.config.ClientOverRpc {
		return rpcs.NewClientOverRpc(id, nodes[id%len(nodes)], db.options), nil
	}
	uri := fmt.Sprintf("couchbase://%s:%d", strings.Join(nodes, ","), *db.config.Port)
	return NewClient(id, uri, *db.config.User, *db.config.Pass), nil
}

func (db *database) ClientConfig() string {
	config := ClientConfig{
		Durability: *db.config.Durability,
		Timeout:    *db.config.Timeout}
	configJson, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return string(configJson)
}

func (db *database) Workloads() []gorgon.Workload {
	nodes := db.options.Nodes
	additionalNode := db.options.AdditionalNodes[0]

	return []gorgon.Workload{
		// Basic workload with getset instruction
		workloads.GetSetWorkload(),
		// Workload with Kill nemesis to kill memcached process
		workloads.GetSetWorkload().Add(nemeses.NewKillNemesis("memcached")).Add(NewSetAfterKillGenerator()),
		// Partition the cluster, but don't block the web UI port
		workloads.GetSetWorkload().Add(nemeses.NewNetworkPartitionNemesis(8091)).Add(NewPartitionAwareGetSetGenerator()),
		// Workload to failover (hard or graceful) and recover (full or delta)
		workloads.GetSetWorkload().Add(NewFailoverAndRecoveryNemesis(db, "Graceful", "Full")),
		workloads.GetSetWorkload().Add(NewFailoverAndRecoveryNemesis(db, "Hard", "Full")),
		// Swap rebalance: add additionalNode, remove nodes[0]
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, additionalNode, nodes[0])),
		// Sequential rebalance: remove nodes[0] and nodes[1], add nodes[0] and nodes[1]
		workloads.GetSetWorkload().Add(NewAdditionalRebalanceGenerator(db, []string{nodes[0], nodes[1]}, []string{nodes[0], nodes[1]}, "sequential")),
		// Bulk rebalance: remove nodes[0] and nodes[1], add nodes[0] and nodes[1]
		workloads.GetSetWorkload().Add(NewAdditionalRebalanceGenerator(db, []string{nodes[0], nodes[1]}, []string{nodes[0], nodes[1]}, "bulk")),
		// Swap rebalance followed by memcached kill on swap-in node
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, additionalNode, nodes[0], "memcached", additionalNode)),
		// Rebalance-out nodes[0] followed by memcached kill on nodes[0]
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, "", nodes[0], "memcached", nodes[0])),
		// Rebalance-in additionalNode followed by cluster orchestrator crash
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, additionalNode, "", "beam.smp", additionalNode)),
		// Rebalance-in additionalNode followed by partitioning additionalNode
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, additionalNode, "", additionalNode)),
		// Rebalance-out nodes[0] followed by partitioning nodes[0]
		workloads.GetSetWorkload().Add(NewRebalanceGenerator(db, "", nodes[0], nodes[0])),
	}
}
