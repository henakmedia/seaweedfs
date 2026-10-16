package ec

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestEcCheckReplication exercises the ec.check.replication classification and reporting over a
// fake topology. EC volume 1 is fully replicated, EC volume 2 is
// under-replicated, and EC volume 3 is over-replicated.
func TestEcCheckReplication(t *testing.T) {
	nodes := []*master_pb.DataNodeInfo{
		{
			Address: "localhost:1111",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d1": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{
							Id:          1,
							EcIndexBits: 0b000111, // shards 0-2
						},
						{
							Id:          2,
							EcIndexBits: 0b000111, // shards 0-2
						},
						{
							Id:          3,
							EcIndexBits: 0b111111, // all 4+2 shards
						},
					},
				},
			},
		},
		{
			Address: "localhost:2222",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d2": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{
							Id:          1,
							EcIndexBits: 0b111000, // shards 3-5
						},
						{
							Id:          2,
							EcIndexBits: 0b110000, // shards 4-5; missing 3
						},
						{
							Id:          3,
							EcIndexBits: 0b001111, // duplicate 0-3
						},
					},
				},
			},
		},
	}

	testCases := []struct {
		name        string
		showDetails bool
		volumeIDMap map[uint32]bool
		want        string
		wantErr     error
	}{
		{
			name:        "default",
			showDetails: false,
			want: `Found 1/3 under-replicated EC volumes: [2]
Found 1/3 over-replicated EC volumes: [3]
`,
		},
		{
			name:        "single healthy volume",
			showDetails: false,
			volumeIDMap: map[uint32]bool{1: true},
			want: `EC volumes are healthy.
`,
		},
		{
			name:        "filtered by volume ID",
			showDetails: false,
			volumeIDMap: map[uint32]bool{1: true, 2: true, 5: true},
			want: `Found 1/2 under-replicated EC volumes: [2]
`,
		},
		{
			name:        "no valid volume IDs",
			showDetails: false,
			volumeIDMap: map[uint32]bool{5: true, 6: true},
			want:        "",
			wantErr:     fmt.Errorf("no EC volumes found"),
		},
		{
			name:        "with details",
			showDetails: true,
			want: `Found 1/3 under-replicated EC volumes: [2]
Found 1/3 over-replicated EC volumes: [3]

Shards map for under-replicated EC volume 2 (5/6 shards):
	00 => [localhost:1111]
	01 => [localhost:1111]
	02 => [localhost:1111]
	03 is missing
	04 (parity) => [localhost:2222]
	05 (parity) => [localhost:2222]

Shards map for over-replicated EC volume 3 (10/6 shards):
	00 => [localhost:1111 localhost:2222]
	01 => [localhost:1111 localhost:2222]
	02 => [localhost:1111 localhost:2222]
	03 => [localhost:1111 localhost:2222]
	04 (parity) => [localhost:1111]
	05 (parity) => [localhost:1111]
`,
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer

		runner := &ecCheckReplicationRunner{
			writer:      &buf,
			dataNodes:   nodes,
			volumeIDMap: tc.volumeIDMap,
		}
		gotErr := runner.checkEcVolumes(tc.showDetails)
		got := buf.String()

		if got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
		if !reflect.DeepEqual(gotErr, tc.wantErr) {
			t.Errorf("%s: got error %v, want %v", tc.name, gotErr, tc.wantErr)
		}
	}
}

// TestEcCheckReplicationNoVolumes checks the empty-topology behavior: an unfiltered
// run over a cluster with no EC volumes is healthy (informational message, no
// error), while a run filtered to volume IDs that are not EC volumes errors.
func TestEcCheckReplicationNoVolumes(t *testing.T) {
	// a data node reporting a normal volume but no EC shards
	nodes := []*master_pb.DataNodeInfo{
		{
			Address:   "localhost:1111",
			DiskInfos: map[string]*master_pb.DiskInfo{"d1": {}},
		},
	}

	t.Run("unfiltered", func(t *testing.T) {
		var buf bytes.Buffer
		runner := &ecCheckReplicationRunner{writer: &buf, dataNodes: nodes}
		if err := runner.checkEcVolumes(false); err != nil {
			t.Errorf("got error %v, want nil", err)
		}
		if got, want := buf.String(), "No EC volumes found.\n"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("filtered no match", func(t *testing.T) {
		var buf bytes.Buffer
		runner := &ecCheckReplicationRunner{writer: &buf, dataNodes: nodes, volumeIDMap: map[uint32]bool{5: true}}
		if err := runner.checkEcVolumes(false); err == nil {
			t.Errorf("got nil error, want %q", "no EC volumes found")
		}
		if got := buf.String(); got != "" {
			t.Errorf("got output %q, want none", got)
		}
	})
}

// TestEcCheckReplicationUnexpectedShard verifies that a shard id beyond a volume's
// expected data+parity range is flagged as over-replication and surfaced in the
// details map, rather than being silently ignored. Volume 20 is a healthy 4+2
// (shards 0..5 on nodeA) with a stray shard 6 on nodeB.
func TestEcCheckReplicationUnexpectedShard(t *testing.T) {
	nodes := []*master_pb.DataNodeInfo{
		{
			Address: "nodeA",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d1": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{Id: 20, EcIndexBits: 0b111111}, // shards 0..5
					},
				},
			},
		},
		{
			Address: "nodeB",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d2": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{Id: 20, EcIndexBits: 0b1000000}, // stray shard 6
					},
				},
			},
		},
	}

	want := `Found 1/1 over-replicated EC volumes: [20]

Shards map for over-replicated EC volume 20 (7/6 shards):
	00 => [nodeA]
	01 => [nodeA]
	02 => [nodeA]
	03 => [nodeA]
	04 (parity) => [nodeA]
	05 (parity) => [nodeA]
	06 (unexpected) => [nodeB]
`

	var buf bytes.Buffer
	runner := &ecCheckReplicationRunner{writer: &buf, dataNodes: nodes}
	if err := runner.checkEcVolumes(true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := buf.String(); got != want {
		t.Errorf("unexpected shard:\n got: %q\nwant: %q", got, want)
	}
}

// TestEcCheckReplicationMixedAnomalies verifies that a volume with both a missing
// shard and a duplicated shard is reported in both lists. Volume 30 has shard 5
// (parity) missing everywhere (under-replicated) and shard 0 on two nodes (over-replicated).
func TestEcCheckReplicationMixedAnomalies(t *testing.T) {
	nodes := []*master_pb.DataNodeInfo{
		{
			Address: "nodeA",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d1": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{Id: 30, EcIndexBits: 0b011111}, // shards 0..4, missing 5
					},
				},
			},
		},
		{
			Address: "nodeB",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"d2": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{Id: 30, EcIndexBits: 0b000001}, // duplicate of shard 0
					},
				},
			},
		},
	}

	want := `Found 1/1 under-replicated EC volumes: [30]
Found 1/1 over-replicated EC volumes: [30]
`

	var buf bytes.Buffer
	runner := &ecCheckReplicationRunner{writer: &buf, dataNodes: nodes}
	if err := runner.checkEcVolumes(false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := buf.String(); got != want {
		t.Errorf("mixed anomalies:\n got: %q\nwant: %q", got, want)
	}
}
