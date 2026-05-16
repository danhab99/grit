package db

import (
	"bytes"
	"testing"
)

func TestDeleteResourceHardSharedObject(t *testing.T) {
	tmp := t.TempDir()
	database, err := NewDatabase(tmp)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer database.Close()

	payload := []byte("shared object payload")
	resourceA, hash, err := database.CreateResourceFromReader("name-a", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("CreateResourceFromReader(name-a) error = %v", err)
	}
	resourceB, _, err := database.CreateResourceFromReader("name-b", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("CreateResourceFromReader(name-b) error = %v", err)
	}

	resultA, err := database.DeleteResourceHard(resourceA)
	if err != nil {
		t.Fatalf("DeleteResourceHard(resourceA) error = %v", err)
	}
	if !resultA.ResourceDeleted {
		t.Fatalf("expected first resource to be deleted")
	}
	if resultA.ObjectDeleted {
		t.Fatalf("expected shared object to be retained on first delete")
	}
	if resultA.RemainingObjectRefs != 1 {
		t.Fatalf("expected remaining refs 1, got %d", resultA.RemainingObjectRefs)
	}
	if !database.ObjectExists(hash) {
		t.Fatalf("expected object hash %s to remain after first delete", hash)
	}

	resultB, err := database.DeleteResourceHard(resourceB)
	if err != nil {
		t.Fatalf("DeleteResourceHard(resourceB) error = %v", err)
	}
	if !resultB.ResourceDeleted {
		t.Fatalf("expected second resource to be deleted")
	}
	if !resultB.ObjectDeleted {
		t.Fatalf("expected shared object to be deleted when final ref is removed")
	}
	if resultB.RemainingObjectRefs != 0 {
		t.Fatalf("expected remaining refs 0, got %d", resultB.RemainingObjectRefs)
	}
	if database.ObjectExists(hash) {
		t.Fatalf("expected object hash %s to be removed after final delete", hash)
	}
}
