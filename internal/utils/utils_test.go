package utils

import (
	"testing"
)

func TestMSB(t *testing.T) {
	var v int64 = 1
	index := MSB(v)
	if index != 0 {
		t.Fatalf("index should be 0 - found %d", index)
	}

	v = 128 * 1024 * 1024 * 1024
	index = MSB(v)
	if index != 37 {
		t.Fatalf("index should be 37 - found %d", index)
	}
}

func TestLSB(t *testing.T) {
	var v int64 = 1
	index := LSB(v)
	if index != 0 {
		t.Fatalf("index should be 0 - found %d", index)
	}

	v = 1024
	index = LSB(v)
	if index != 10 {
		t.Fatalf("index should be 10 - found %d", index)
	}

	v = 1024 * 1024 * 1024
	index = LSB(v)
	if index != 30 {
		t.Fatalf("index should be 30 - found %d", index)
	}

	v = 1024 * 1024
	index = LSB(v)
	if index != 20 {
		t.Fatalf("index should be 20 - found %d", index)
	}

	v = 128 * 1024 * 1024 * 1024
	index = LSB(v)
	if index != 37 {
		t.Fatalf("index should be 37 - found %d", index)
	}

	v = 6533 * 1024 * 1024
	index = LSB(v)
	if index != 20 {
		t.Fatalf("index should be 20 - found %d", index)
	}
}

func TestPrettyPrint(t *testing.T) {
	var v int64 = 128 * 1024
	pretty := PrettyPrint(v)
	if pretty != "128kB" {
		t.Fatalf("expected 128kB, found %s", pretty)
	}

	v = 128 * 1024 * 1024
	pretty = PrettyPrint(v)
	if pretty != "128MB" {
		t.Fatalf("expected 128MB, found %s", pretty)
	}

	v = 128 * 1024 * 1024 * 1024
	pretty = PrettyPrint(v)
	if pretty != "128GB" {
		t.Fatalf("expected 128GB, found %s", pretty)
	}

	v = 12
	pretty = PrettyPrint(v)
	if pretty != "12" {
		t.Fatalf("expected 128GB, found %s", pretty)
	}
	v = 12 * 1024
	pretty = PrettyPrint(v)
	if pretty != "12kB" {
		t.Fatalf("expected 12kB, found %s", pretty)
	}
	v = 12 * 1024 * 1024
	pretty = PrettyPrint(v)
	if pretty != "12MB" {
		t.Fatalf("expected 12MB, found %s", pretty)
	}

	v = 192 * 1024 * 1024 * 1024
	pretty = PrettyPrint(v)
	if pretty != "192GB" {
		t.Fatalf("expected 128GB, found %s", pretty)
	}

	v = 6553 * 1024 * 1024
	pretty = PrettyPrint(v)
	if pretty != "6553MB" {
		t.Fatalf("expected 128GB, found %s", pretty)
	}
}

func TestClear(t *testing.T) {
	var v int64 = 128*1024 + 1

	updated := CleartoKB(v)
	if updated != v-1 {
		t.Fatalf("expected 128GB, found %d", updated)
	}

	v = 128*1024*1024 + 1
	updated = CleartoKB(v)
	if updated != v-1 {
		t.Fatalf("expected 128MB, found %d", updated)
	}

	v = 128*1024*1024 + 1
	updated = CleartoMB(v)
	if updated != v-1 {
		t.Fatalf("expected 128MB, found %d", updated)
	}

	v = 1*1024*1024 - 1
	updated = CleartoMB(v)
	if updated != 0 {
		t.Fatalf("expected 0, found %d", updated)
	}

}
