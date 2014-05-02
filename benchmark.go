package benchmark

import (
	"github.com/leifwalsh/olbermann"
	"log"
	"sync"
	"time"
)

// Basic stats for all benchmarks, some day should make each benchmark define its own.
type Stats struct {
	Inserts    uint64 `type:"counter" report:"iter,cum,total"`
	Deletes    uint64 `type:"counter" report:"iter,cum,total"`
	Updates    uint64 `type:"counter" report:"iter,cum,total"`
	Queries    uint64 `type:"counter" report:"iter,cum,total"`
	Operations uint64 `type:"counter" report:"iter,cum,total"`
	Errors     uint64 `type:"counter" report:"total"`
}

// An interface that defines work to be run on a thread.
type Work interface {
	// While the benchmark is running, Do is called repeatedly.
	// The function is responsible for sending results over the channel.
	Do(c chan<- interface{})
	// Cleanup any state needed before closing the benchmark.
	Close()
}

// Defines information about what a background thread's work.
//
type WorkInfo struct {
	// The work that is to being run throughout the benchmark
	Work Work
	// The maximum number of operations to run within WorkInfo.IntervalInSeconds
	// 0 means unlimited.
	OpsPerInterval uint64
	// Defines the period during for Work.OpsPerInterval. So, for example,
	// if OpsPerInterval is set to 100, and IntervalInSeconds is set to 1, then
	// this thread will do at most 100 operations per second.
	// 0 means there is no interval, and that Work may run as often as possible
	IntervalInSeconds uint64
	// The maximum number of operations this thread may run. This is used for benchmarks
	// that are designed to run a certain amount of work as opposed to run for a certain
	// amount of time. If this value is 0, and implies that Run was run with
	// a non-zero duration. If this value is > 0, that implies the benchmark is designed
	// to execute some finite task, and that Run was run with a duration of 0
	MaxOps uint64
}

// struct used to gate operations for a WorkInfo
type operationGater struct {
	t0      time.Time
	currOps uint64
}

// helper function to ensure we honor w.OpsPerInterval and w.IntervalInSeconds.
// Will sleep for the necessary time to ensure that the operations are properly gated
func (o *operationGater) gateOperations(w WorkInfo) {
	o.currOps++
	period := time.Duration(w.IntervalInSeconds) * time.Second
	// if we care about gating operations, and the number operations run has
	// surpassed the number of operations we want to run in an interval,
	// then possibly sleep to ensure gating, and reset the state
	if w.IntervalInSeconds > 0 && w.OpsPerInterval > 0 && o.currOps >= w.OpsPerInterval {
		lastTime := time.Now()
		if period > lastTime.Sub(o.t0) {
			time.Sleep(period - lastTime.Sub(o.t0))
		}
		o.currOps = 0
		o.t0 = time.Now()
	}
	return
}

// run a WorkInfo repeatedly until we get a message over the
// quitChannel telling us to exit
func runTimeBasedWorker(w WorkInfo, metrics chan<- interface{}, quitChannel chan int, done *sync.WaitGroup) {
	// this should never happen, as we've already called verifyWorks,
	// but it doesn't hurt
	if w.MaxOps > 0 {
		log.Fatal("calling runFiniteWorker with w.MaxOps ", w.MaxOps, " which is invalid. w.MaxOps must be <= 0")
	}
	defer done.Done()
	defer w.Work.Close()
	o := operationGater{t0: time.Now()}
	for {
		select {
		case <-quitChannel: // I hope this check is not too inefficient. If it is, we can batch the default case
			return
		default:
			w.Work.Do(metrics)
		}
		o.gateOperations(w)
	}
}

// run a WorkInfo for a finite number of operations. There is no way
// to get this function to exit early. It exits once the w.Work has
// been executed w.MaxOps times
func runFiniteWorker(w WorkInfo, metrics chan<- interface{}, done *sync.WaitGroup) {
	// this should never happen, as we've already called verifyWorks,
	// but it doesn't hurt
	if w.MaxOps <= 0 {
		log.Fatal("calling runFiniteWorker with w.MaxOps ", w.MaxOps, " which is invalid. w.MaxOps must be > 0")
	}
	defer done.Done()
	defer w.Work.Close()
	o := operationGater{t0: time.Now()}
	for numOps := uint64(0); numOps < w.MaxOps; numOps++ {
		w.Work.Do(metrics)
		o.gateOperations(w)
	}
}

// verify that either all the works are time based, meaning d > 0
// or that all the works are finite, meaning MaxOps > 0
func verifyWorks(works []WorkInfo, d time.Duration) {
	if d <= time.Duration(0) {
		for i := range works {
			if works[i].MaxOps <= 0 {
				log.Fatal("Have a benchmark time <= 0, ", d, ", but work ", i, " has MaxOps <= 0, ", works[i].MaxOps, ". It should be > 0")
			}
		}
	} else {
		for i := range works {
			if works[i].MaxOps > 0 {
				log.Fatal("Have a benchmark time > 0, ", d, ", but work ", i, " has MaxOps > 0, ", works[i].MaxOps, ". It should be <= 0")
			}
		}
	}
}

// runs a benchmark by having each member of works run its Work repeatedly
// in a background thread. The number of threads doing work is equal to len(works).
// So, for example, if iibench is running with 4 writer threads and two query threads, then
// works will have six elements, four threads for the inserts, and two threads for queries.
// If d > 0, the benchmark will run for the time defined by d. If d is 0, then the benchmark is designed
// to finish a finite amount of work (like loading 10M documents into a collection), and not designed
// to run for a certain amount of time. As a result, each element of works will have MaxOps > 0.
func Run(metricSample interface{}, works []WorkInfo, d time.Duration) {
	verifyWorks(works, d)
	numWorkers := len(works)
	log.Println("num workers ", numWorkers)
	workersDone := sync.WaitGroup{}
	// this channel is used to communicate results
	metrics := make(chan interface{}, 100)
	reporter := olbermann.Reporter{C: metrics}
	go reporter.Feed()
	if err := reporter.Start(metricSample, &olbermann.BasicDstatStyler); err != nil {
		log.Fatal(err)
	}
	defer reporter.Close()
	// probably a better way to do this
	quitWorkerChannels := make([]chan int, numWorkers)
	for i := 0; i < numWorkers; i++ {
		quitWorkerChannels[i] = make(chan int)
	}
	for i := 0; i < numWorkers; i++ {
		workersDone.Add(1)
		// MaxOps <= 0 means we will be running for a certain amount of time
		// and that there is no maximum
		if works[i].MaxOps <= 0 {
			go runTimeBasedWorker(works[i], metrics, quitWorkerChannels[i], &workersDone)
		} else {
			go runFiniteWorker(works[i], metrics, &workersDone)
		}
	}
	time.Sleep(d)
	for i := 0; i < numWorkers; i++ {
		if works[i].MaxOps <= 0 {
			quitWorkerChannels[i] <- 0
		}
	}
	workersDone.Wait()
}
