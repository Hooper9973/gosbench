package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// WorkItem is an interface for general work operations
// They can be read,write,list,delete or a stopper
type WorkItem interface {
	Prepare() error
	Do() error
	Clean() error
}

// ReadOperation stands for a read operation
type ReadOperation struct {
	TestName                 string
	Bucket                   string
	ObjectID                 uint64
	MaxObjectNum             uint64
	ObjectSize               uint64
	WorksOnPreexistingObject bool
}

// WriteOperation stands for a write operation
type WriteOperation struct {
	TestName     string
	Bucket       string
	ObjectID     uint64
	MaxObjectNum uint64
	ObjectSize   uint64
}

// ListOperation stands for a list operation
type ListOperation struct {
	TestName   string
	Bucket     string
	ObjectName string
	ObjectSize uint64
}

// DeleteOperation stands for a delete operation
type DeleteOperation struct {
	TestName   string
	Bucket     string
	ObjectName string
	ObjectSize uint64
}

// Stopper marks the end of a workqueue when using
// maxOps as testCase end criterium
type Stopper struct{}

// KV is a simple key-value struct
type KV struct {
	Key   string
	Value float64
}

// Workqueue contains the Queue and the valid operation's
// values to determine which operation should be done next
// in order to satisfy the set ratios.
type Workqueue struct {
	OperationValues []KV
	Queue           *[]WorkItem
}

// GetNextOperation evaluates the operation values and returns which
// operation should happen next
func GetNextOperation(Queue *Workqueue) string {
	sort.Slice(Queue.OperationValues, func(i, j int) bool {
		return Queue.OperationValues[i].Value < Queue.OperationValues[j].Value
	})
	return Queue.OperationValues[0].Key
}

// IncreaseOperationValue increases the given operation's value by the set amount
func IncreaseOperationValue(operation string, value float64, Queue *Workqueue) error {
	for i := range Queue.OperationValues {
		if Queue.OperationValues[i].Key == operation {
			Queue.OperationValues[i].Value += value
			return nil
		}
	}
	return fmt.Errorf("Could not find requested operation %s", operation)
}

// Prepare prepares the execution of the ReadOperation
func (op *ReadOperation) Prepare() error {
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	log.WithField("bucket", op.Bucket).WithField("object", objectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Debug("Preparing ReadOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	return putObject(housekeepingSvc, objectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
}

// Prepare prepares the execution of the WriteOperation
func (op *WriteOperation) Prepare() error {
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	log.WithField("bucket", op.Bucket).WithField("object", objectName).Debug("Preparing WriteOperation")
	return nil
}

// Prepare prepares the execution of the ListOperation
func (op *ListOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Debug("Preparing ListOperation")
	return putObject(housekeepingSvc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
}

// Prepare prepares the execution of the DeleteOperation
func (op *DeleteOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Debug("Preparing DeleteOperation")
	return putObject(housekeepingSvc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
}

// Prepare does nothing here
func (op *Stopper) Prepare() error {
	return nil
}

// Do executes the actual work of the ReadOperation
func (op *ReadOperation) Do() error {
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	log.WithField("bucket", op.Bucket).WithField("object", objectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Debug("Doing ReadOperation")
	start := time.Now()
	err := getObjectWithoutSize(svc, objectName, op.Bucket)
	duration := time.Since(start)
	promLatency.WithLabelValues(op.TestName, "GET").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "GET").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "GET").Inc()
	}
	promDownloadedBytes.WithLabelValues(op.TestName, "GET").Add(float64(op.ObjectSize))
	log.WithField("bucket", op.Bucket).
		WithField("object", objectName).
		WithField("successful?", err == nil).
		WithField("latency(ms)", float64(duration.Milliseconds())).
		Debug("finish ReadOperation")
	op.ObjectID = uint64(time.Now().UnixNano()) % op.MaxObjectNum
	return err
}

// Do executes the actual work of the WriteOperation
func (op *WriteOperation) Do() error {
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	log.WithField("bucket", op.Bucket).WithField("object", objectName).Debug("Doing WriteOperation")
	start := time.Now()
	err := putObject(svc, objectName, bytes.NewReader(generateFixedBytes(op.ObjectSize)), op.Bucket)
	duration := time.Since(start)
	promLatency.WithLabelValues(op.TestName, "PUT").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "PUT").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "PUT").Inc()
	}
	promUploadedBytes.WithLabelValues(op.TestName, "PUT").Add(float64(op.ObjectSize))
	log.WithField("bucket", op.Bucket).
		WithField("object", objectName).
		WithField("successful?", err == nil).
		WithField("latency(ms)", float64(duration.Milliseconds())).
		Debug("finish WriteOperation")
	if op.ObjectID+1000 >= op.MaxObjectNum {
		//each 1000 objects we hit the max object number
		log.Info("hit max ObjectNum")
	}
	op.ObjectID = (op.ObjectID + 1000) % op.MaxObjectNum
	return err
}

// Do executes the actual work of the ListOperation
func (op *ListOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Debug("Doing ListOperation")
	start := time.Now()
	_, err := listObjects(svc, op.ObjectName, op.Bucket)
	duration := time.Since(start)
	promLatency.WithLabelValues(op.TestName, "LIST").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "LIST").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "LIST").Inc()
	}
	log.WithField("bucket", op.Bucket).
		WithField("object", op.ObjectName).
		WithField("successful?", err == nil).
		WithField("latency(ms)", float64(duration.Milliseconds())).
		Debug("finish ListOperation")
	return err
}

// Do executes the actual work of the DeleteOperation
func (op *DeleteOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Debug("Doing DeleteOperation")
	start := time.Now()
	err := deleteObject(svc, op.ObjectName, op.Bucket)
	duration := time.Since(start)
	promLatency.WithLabelValues(op.TestName, "DELETE").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "DELETE").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "DELETE").Inc()
	}
	log.WithField("bucket", op.Bucket).
		WithField("object", op.ObjectName).
		WithField("successful?", err == nil).
		WithField("latency(ms)", float64(duration.Milliseconds())).
		Debug("finish DeleteOperation")
	return err
}

// Do does nothing here
func (op *Stopper) Do() error {
	return nil
}

// Clean removes the objects and buckets left from the previous ReadOperation
func (op *ReadOperation) Clean() error {
	if op.WorksOnPreexistingObject {
		return nil
	}
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	log.WithField("bucket", op.Bucket).WithField("object", objectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Debug("Cleaning up ReadOperation")
	return deleteObject(housekeepingSvc, objectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous WriteOperation
func (op *WriteOperation) Clean() error {
	objectName := fmt.Sprintf("%s-%d", op.TestName, op.ObjectID)
	return deleteObject(housekeepingSvc, objectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous ListOperation
func (op *ListOperation) Clean() error {
	return deleteObject(housekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous DeleteOperation
func (op *DeleteOperation) Clean() error {
	return nil
}

// Clean does nothing here
func (op *Stopper) Clean() error {
	return nil
}

// DoWork processes the workitems in the workChannel until
// either the time runs out or a stopper is found
func DoWork(workChannel <-chan WorkItem, notifyChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-notifyChan:
			log.Debugf("Runtime over - Got timeout from work context")
			return
		case work := <-workChannel:
			switch work.(type) {
			case *Stopper:
				log.Debug("Found the end of the work Queue - stopping")
				return
			}
			err := work.Do()
			if err != nil {
				log.WithError(err).Error("Issues when performing work - ignoring")
			}
		}
	}
}
func workerFunc(id int, Workqueue *Workqueue, duration time.Duration, numberOfWorker int, wg *sync.WaitGroup) {
	defer wg.Done()
	deadline := time.Now().Add(duration)
	for {
		remainingTime := time.Until(deadline)
		if remainingTime <= 0 {
			log.Infof("Parallel %d reached Runtime end", id)
			return
		}
		for j := id; j < len(*Workqueue.Queue); j += numberOfWorker {
			remainingTime = time.Until(deadline)
			if remainingTime <= 0 {
				log.Infof("Parallel %d reached Runtime end", id)
				return
			}
			//log.Infof("Worker %d processing item %d, remaining time: %v", id, j, remainingTime)
			err := (*Workqueue.Queue)[j].Do()
			if err != nil {
				log.WithError(err).Error("Issues when performing work - ignoring")
			}
		}
	}
}

func generateRandomBytes(size uint64) []byte {
	now := time.Now()
	random := make([]byte, size)
	n, err := rand.Read(random)
	if err != nil {
		log.WithError(err).Fatal("I had issues getting my random bytes initialized")
	}
	log.Tracef("Generated %d random bytes in %v", n, time.Since(now))
	return random
}

func generateFixedBytes(size uint64) []byte {
	return bytes.Repeat([]byte{0}, int(size))
}
