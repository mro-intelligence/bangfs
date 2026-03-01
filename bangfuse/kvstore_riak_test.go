package bangfuse

import (
	"encoding/json"
	"testing"
)

func TestExtractDiskInfo_PreferredPath(t *testing.T) {
	stats := &riakStatsResponse{
		Disk: []riakDiskEntry{
			{ID: "/", Size: 100000, Used: 40000},
			{ID: "/data", Size: 500000, Used: 200000},
		},
	}
	total, used, err := extractDiskInfo(stats, "/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 500000 || used != 200000 {
		t.Errorf("expected 500000/200000, got %d/%d", total, used)
	}
}

func TestExtractDiskInfo_FallbackContainsData(t *testing.T) {
	stats := &riakStatsResponse{
		Disk: []riakDiskEntry{
			{ID: "/", Size: 100000, Used: 40000},
			{ID: "/mnt/data-ssd", Size: 800000, Used: 300000},
		},
	}
	total, used, err := extractDiskInfo(stats, "/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 800000 || used != 300000 {
		t.Errorf("expected 800000/300000, got %d/%d", total, used)
	}
}

func TestExtractDiskInfo_FallbackRoot(t *testing.T) {
	stats := &riakStatsResponse{
		Disk: []riakDiskEntry{
			{ID: "/", Size: 100000, Used: 40000},
			{ID: "/tmp", Size: 50000, Used: 10000},
		},
	}
	total, used, err := extractDiskInfo(stats, "/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 100000 || used != 40000 {
		t.Errorf("expected 100000/40000, got %d/%d", total, used)
	}
}

func TestExtractDiskInfo_FallbackFirstEntry(t *testing.T) {
	stats := &riakStatsResponse{
		Disk: []riakDiskEntry{
			{ID: "/mnt/ssd", Size: 900000, Used: 100000},
		},
	}
	total, used, err := extractDiskInfo(stats, "/data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 900000 || used != 100000 {
		t.Errorf("expected 900000/100000, got %d/%d", total, used)
	}
}

func TestExtractDiskInfo_Empty(t *testing.T) {
	stats := &riakStatsResponse{
		Disk: []riakDiskEntry{},
	}
	_, _, err := extractDiskInfo(stats, "/data")
	if err == nil {
		t.Fatal("expected error for empty disk list")
	}
}

func TestExtractHostsFromMembers(t *testing.T) {
	members := []string{
		"riak@192.168.1.1",
		"riak@192.168.1.2",
		"riak@192.168.1.1", // duplicate
	}
	hosts := extractHostsFromMembers(members)
	if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d: %v", len(hosts), hosts)
	}
	if hosts[0] != "192.168.1.1" || hosts[1] != "192.168.1.2" {
		t.Errorf("unexpected hosts: %v", hosts)
	}
}

func TestExtractHostsFromMembers_Empty(t *testing.T) {
	hosts := extractHostsFromMembers(nil)
	if len(hosts) != 0 {
		t.Errorf("expected empty, got %v", hosts)
	}
}

func TestRiakStatsResponseJSON(t *testing.T) {
	raw := `{
		"ring_members": ["riak@10.0.0.1", "riak@10.0.0.2"],
		"disk": [
			{"id": "/", "size": 102400000, "used": 51200000},
			{"id": "/data", "size": 512000000, "used": 128000000}
		]
	}`
	var stats riakStatsResponse
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	if len(stats.RingMembers) != 2 {
		t.Fatalf("expected 2 ring members, got %d", len(stats.RingMembers))
	}
	if len(stats.Disk) != 2 {
		t.Fatalf("expected 2 disk entries, got %d", len(stats.Disk))
	}
	if stats.Disk[1].ID != "/data" {
		t.Errorf("expected /data, got %s", stats.Disk[1].ID)
	}
	if stats.Disk[1].Size != 512000000 {
		t.Errorf("expected 512000000, got %d", stats.Disk[1].Size)
	}
	if stats.Disk[1].Used != 128000000 {
		t.Errorf("expected 128000000, got %d", stats.Disk[1].Used)
	}
}
