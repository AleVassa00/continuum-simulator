package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDeploygenUsesCurrentTwoColumnTopology(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	err := runDeploygen([]string{"-mode", "local", "-experiment", "../../experiments/baseline.yaml"}, deploygenOptions{
		TopologyPath:         "../../dataset/output/kmeans_topology.csv",
		PartitionWeightsPath: "../../dataset/output/edge_partition_weights.csv",
		OutputPath:           filepath.Join(root, "continuum.yml"),
		ArtifactsRoot:        filepath.Join(root, "artifacts"),
		Stdout:               &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "continuum.yml")); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitiveTopologyCSVContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmeans_topology.csv")
	if err := os.WriteFile(path, []byte("sensor_id,edge_id\n1,edge-1\n2,edge-0\n3,edge-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	edges, err := loadEdges(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 2 || edges[0].EdgeID != "edge-0" || edges[0].SensorCount != 1 || edges[1].EdgeID != "edge-1" || edges[1].SensorCount != 2 {
		t.Fatalf("topologia non valida: %+v", edges)
	}
}

func TestTopologyRejectsAdditionalColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmeans_topology.csv")
	if err := os.WriteFile(path, []byte("sensor_id,edge_id,lat,lon\n1,edge-0,1,2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadEdges(path); err == nil {
		t.Fatal("topologia con il vecchio schema accettata")
	}
}
