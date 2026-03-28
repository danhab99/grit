package db

import "fmt"

// Primary entity key prefixes
const (
	prefixStep     = "s:"
	prefixTask     = "t:"
	prefixResource = "r:"
	prefixColumn   = "c:"
	prefixColTask  = "ct:"
	prefixColVal   = "cv:"
	prefixObject   = "o:"
	prefixMeta     = "m:"
)

// Index key prefixes
const (
	idxStepByName       = "ix:sn:"  // +{name}\x00{version_08d}\x00{ulid}
	idxTaskByStepUnproc = "ix:tsu:" // +{step_ulid}\x00{task_ulid}
	idxTaskByStepProc   = "ix:tsp:" // +{step_ulid}\x00{task_ulid}
	idxTaskByStepAll    = "ix:tsa:" // +{step_ulid}\x00{task_ulid}
	idxTaskUnique       = "ix:tu:"  // +{step_ulid}\x00{resource_ulid} → task_ulid
	idxTaskByInput      = "ix:ti:"  // +{resource_ulid}\x00{task_ulid}
	idxResourceByName   = "ix:rn:"  // +{name}\x00{ulid}
	idxResourceHash     = "ix:rh:"  // +{name}\x00{object_hash} → resource_ulid
	idxResourceProducer = "ix:rp:"  // +{resource_ulid} → task_ulid
	idxResourceProdStep = "ix:rps:" // +{step_name}\x00{resource_ulid}
	idxColumnByName     = "ix:cn:"  // +{name}\x00{resource_name}\x00{version_08d}\x00{ulid}
	idxColTaskUnproc    = "ix:ctu:" // +{column_ulid}\x00{ct_ulid}
	idxColTaskProc      = "ix:ctp:" // +{column_ulid}\x00{ct_ulid}
	idxColTaskUnique    = "ix:ctq:" // +{column_ulid}\x00{resource_ulid} → ct_ulid
	idxColValByColRes   = "ix:cvcr:"// +{column_ulid}\x00{resource_ulid} → cv_ulid
	idxColValByRes      = "ix:cvr:" // +{resource_ulid}\x00{cv_ulid}
	idxColValByColName  = "ix:cvn:" // +{column_name}\x00{resource_ulid} → cv_ulid
)

// --- Primary key builders ---

func stepKey(id string) []byte     { return []byte(prefixStep + id) }
func taskKey(id string) []byte     { return []byte(prefixTask + id) }
func resourceKey(id string) []byte { return []byte(prefixResource + id) }
func columnKey(id string) []byte   { return []byte(prefixColumn + id) }
func colTaskKey(id string) []byte  { return []byte(prefixColTask + id) }
func colValKey(id string) []byte   { return []byte(prefixColVal + id) }
func objectKey(hash []byte) []byte { return append([]byte(prefixObject), hash...) }

// --- Index key builders ---

func idxStepByNameKey(name string, version int, id string) []byte {
	return []byte(fmt.Sprintf("%s%s\x00%08d\x00%s", idxStepByName, name, version, id))
}

func idxTaskByStepUnprocKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepUnproc + stepID + "\x00" + taskID)
}

func idxTaskByStepProcKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepProc + stepID + "\x00" + taskID)
}

func idxTaskByStepAllKey(stepID, taskID string) []byte {
	return []byte(idxTaskByStepAll + stepID + "\x00" + taskID)
}

func idxTaskUniqueKey(stepID, resourceID string) []byte {
	return []byte(idxTaskUnique + stepID + "\x00" + resourceID)
}

func idxTaskByInputKey(resourceID, taskID string) []byte {
	return []byte(idxTaskByInput + resourceID + "\x00" + taskID)
}

func idxResourceByNameKey(name, id string) []byte {
	return []byte(idxResourceByName + name + "\x00" + id)
}

func idxResourceHashKey(name, objectHash string) []byte {
	return []byte(idxResourceHash + name + "\x00" + objectHash)
}

func idxResourceProducerKey(resourceID string) []byte {
	return []byte(idxResourceProducer + resourceID)
}

func idxResourceProdStepKey(stepName, resourceID string) []byte {
	return []byte(idxResourceProdStep + stepName + "\x00" + resourceID)
}

func idxColumnByNameKey(name, resourceName string, version int, id string) []byte {
	return []byte(fmt.Sprintf("%s%s\x00%s\x00%08d\x00%s", idxColumnByName, name, resourceName, version, id))
}

func idxColTaskUnprocKey(columnID, ctID string) []byte {
	return []byte(idxColTaskUnproc + columnID + "\x00" + ctID)
}

func idxColTaskProcKey(columnID, ctID string) []byte {
	return []byte(idxColTaskProc + columnID + "\x00" + ctID)
}

func idxColTaskUniqueKey(columnID, resourceID string) []byte {
	return []byte(idxColTaskUnique + columnID + "\x00" + resourceID)
}

func idxColValByColResKey(columnID, resourceID string) []byte {
	return []byte(idxColValByColRes + columnID + "\x00" + resourceID)
}

func idxColValByResKey(resourceID, cvID string) []byte {
	return []byte(idxColValByRes + resourceID + "\x00" + cvID)
}

func idxColValByColNameKey(columnName, resourceID string) []byte {
	return []byte(idxColValByColName + columnName + "\x00" + resourceID)
}

// --- Prefix builders for scans ---

func idxStepByNamePrefix(name string) []byte {
	return []byte(idxStepByName + name + "\x00")
}

func idxTaskByStepUnprocPrefix(stepID string) []byte {
	return []byte(idxTaskByStepUnproc + stepID + "\x00")
}

func idxTaskByStepAllPrefix(stepID string) []byte {
	return []byte(idxTaskByStepAll + stepID + "\x00")
}

func idxTaskByInputPrefix(resourceID string) []byte {
	return []byte(idxTaskByInput + resourceID + "\x00")
}

func idxResourceByNamePrefix(name string) []byte {
	return []byte(idxResourceByName + name + "\x00")
}

func idxResourceProdStepPrefix(stepName string) []byte {
	return []byte(idxResourceProdStep + stepName + "\x00")
}

func idxColumnByNamePrefix(name string) []byte {
	return []byte(idxColumnByName + name + "\x00")
}

func idxColumnByNameResPrefix(name, resourceName string) []byte {
	return []byte(fmt.Sprintf("%s%s\x00%s\x00", idxColumnByName, name, resourceName))
}

func idxColTaskUnprocPrefix(columnID string) []byte {
	return []byte(idxColTaskUnproc + columnID + "\x00")
}

func idxColValByResPrefix(resourceID string) []byte {
	return []byte(idxColValByRes + resourceID + "\x00")
}

// --- Meta key builders ---

func metaCsvHashKey(path string) []byte {
	return []byte(prefixMeta + "csvhash:" + path)
}
