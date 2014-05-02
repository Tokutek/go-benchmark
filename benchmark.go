package benchmark

import (
	"log"
	"sync"
	"time"
)

// Current struct used to transfer results from a Work
// to a ResultManager. Note that he channel a Work
// has as an input parameter to Do() is of this type.
type Stats struct {
	Inserts    uint64
	Deletes    uint64
	Updates    uint64
	Queries    uint64
	Operations uint64
	Errors     uint64
}

// An interface that defines work to be run on a thread.
type Work interface {
	// While the benchmark is running, Do is called repeatedly.
	// The function is responsible for sending results over the channel.
	Do(c chan Stats)
	// Cleanup any state needed before closing the benchmark.
	Close()
}

// an interface responsible for reporting results of the benchmark as it is running
type ResultManager interface {
	// Method called once a second that is responsible for printing any
	// any results the benchmark writer sees fit
	PrintResults()
	// Called at the end of the benchmark to give the benchmark writer
	// an opportunity to print a summary of the entire run.
	PrintFinalResults()
	// Method responsible for aggregating results that an individual
	// Work sends over the channel provided in Work.Do
	RegisterIntermediateResult(r Stats)
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
func runTimeBasedWorker(w WorkInfo, c chan Stats, quitChannel chan int, done *sync.WaitGroup) {
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
			w.Work.Do(c)
		}
		o.gateOperations(w)
	}
}

// run a WorkInfo for a finite number of operations. There is no way
// to get this function to exit early. It exits once the w.Work has
// been executed w.MaxOps times
func runFiniteWorker(w WorkInfo, c chan Stats, done *sync.WaitGroup) {
	// this should never happen, as we've already called verifyWorks,
	// but it doesn't hurt
	if w.MaxOps <= 0 {
		log.Fatal("calling runFiniteWorker with w.MaxOps ", w.MaxOps, " which is invalid. w.MaxOps must be > 0")
	}
	defer done.Done()
	defer w.Work.Close()
	o := operationGater{t0: time.Now()}
	for numOps := uint64(0); numOps < w.MaxOps; numOps++ {
		w.Work.Do(c)
		o.gateOperations(w)
	}
}

// background thread responsible for accumulating results that Works send
func registerWrites(r ResultManager, c chan Stats, quitChannel chan int, done *sync.WaitGroup) {
	defer done.Done()
	for {
		select {
		case <-quitChannel: // I hope this check is not too inefficient. If it is, we can batch the default case
			return
		case x := <-c:
			r.RegisterIntermediateResult(x)
		}
	}
}

// background thread responsible for printing results once a second, until the benchmark ends,
// at which point will print final results.
func printWrites(r ResultManager, quitChannel chan int, done *sync.WaitGroup) {
	defer done.Done()
	for {
		select {
		case <-quitChannel:
			r.PrintFinalResults()
			return
		default:
			r.PrintResults()
			time.Sleep(1 * time.Second)
		}
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
func Run(res ResultManager, works []WorkInfo, d time.Duration) {
	verifyWorks(works, d)
	numWorkers := len(works)
	log.Println("num workers ", numWorkers)
	workersDone := sync.WaitGroup{}
	registerDone := sync.WaitGroup{}
	benchmarkDone := sync.WaitGroup{}
	// not sure if this batching is wise
	// this channel is used to communicate results
	resultsChannel := make(chan Stats, 100)
	// probably a better way to do this
	quitWorkerChannels := make([]chan int, numWorkers) // one for each work, one for registerWrites, and one for printWrites
	registerWritesChannel := make(chan int)
	printWritesChannel := make(chan int)
	for i := 0; i < numWorkers; i++ {
		quitWorkerChannels[i] = make(chan int)
	}
	for i := 0; i < numWorkers; i++ {
		workersDone.Add(1)
		// MaxOps <= 0 means we will be running for a certain amount of time
		// and that there is no maximum
		if works[i].MaxOps <= 0 {
			go runTimeBasedWorker(works[i], resultsChannel, quitWorkerChannels[i], &workersDone)
		} else {
			go runFiniteWorker(works[i], resultsChannel, &workersDone)
		}
	}
	registerDone.Add(1)
	go registerWrites(res, resultsChannel, registerWritesChannel, &registerDone)
	benchmarkDone.Add(1)
	go printWrites(res, printWritesChannel, &benchmarkDone)
	time.Sleep(d)
	for i := 0; i < numWorkers; i++ {
		if works[i].MaxOps <= 0 {
			quitWorkerChannels[i] <- 0
		}
	}
	workersDone.Wait()
	// quit registerWrites and printWrites
	registerWritesChannel <- 0
	registerDone.Wait()
	printWritesChannel <- 0
	benchmarkDone.Wait()
}
