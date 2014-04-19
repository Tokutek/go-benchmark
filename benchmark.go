package tokubenchmark

import (
	//"fmt"
	"log"
	"sync"
	"time"
)

// Current struct used to transfer results from a BenchmarkWorkItem
// to a BenchmarkResultManager. Note that he channel a BenchmarkWorkItem
// has as an input parameter to DoWork() is of this type.
type BenchmarkStats struct {
	Inserts    uint64
	Deletes    uint64
	Updates    uint64
	Queries    uint64
	Operations uint64
	Errors     uint64
}

// An interface that defines work to be run on a thread.
type BenchmarkWorkItem interface {
	DoWork(c chan BenchmarkStats)
	Close()
}

type BenchmarkResultManager interface {
	PrintResults()
	PrintFinalResults()
	RegisterIntermedieteResult(r BenchmarkStats)
}

type BenchmarkWorkInfo struct {
	WorkItem          BenchmarkWorkItem
	OpsPerInterval    uint64
	IntervalInSeconds uint64
	MaxOps            uint64
}

type operationGater struct {
	t0      time.Time
	currOps uint64
}

func (o *operationGater) gateOperations(w BenchmarkWorkInfo) {
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

func runTimeBasedWorker(w BenchmarkWorkInfo, c chan BenchmarkStats, quitChannel chan int, benchmarkDone *sync.WaitGroup) {
	// this should never happen, as we've already called verifyWorkItems,
	// but it doesn't hurt
	if w.MaxOps > 0 {
		log.Fatal("calling runFiniteWorker with w.MaxOps ", w.MaxOps, " which is invalid. w.MaxOps must be <= 0")
	}
	defer benchmarkDone.Done()
	defer w.WorkItem.Close()
	o := operationGater{t0: time.Now()}
	for {
		select {
		case <-quitChannel: // I hope this check is not too inefficient. If it is, we can batch the default case
			return
		default:
			w.WorkItem.DoWork(c)
		}
		o.gateOperations(w)
	}
}

func runFiniteWorker(w BenchmarkWorkInfo, c chan BenchmarkStats, benchmarkDone *sync.WaitGroup) {
	// this should never happen, as we've already called verifyWorkItems,
	// but it doesn't hurt
	if w.MaxOps <= 0 {
		log.Fatal("calling runFiniteWorker with w.MaxOps ", w.MaxOps, " which is invalid. w.MaxOps must be > 0")
	}
	defer benchmarkDone.Done()
	defer w.WorkItem.Close()
	o := operationGater{t0: time.Now()}
	for numOps := uint64(0); numOps < w.MaxOps; numOps++ {
		w.WorkItem.DoWork(c)
		o.gateOperations(w)
	}
}

func registerWrites(r BenchmarkResultManager, c chan BenchmarkStats, quitChannel chan int, benchmarkDone *sync.WaitGroup) {
	defer benchmarkDone.Done()
	for {
		select {
		case <-quitChannel: // I hope this check is not too inefficient. If it is, we can batch the default case
			return
		case x := <-c:
			r.RegisterIntermedieteResult(x)
		}
	}
}

func printWrites(r BenchmarkResultManager, quitChannel chan int, benchmarkDone *sync.WaitGroup) {
	defer benchmarkDone.Done()
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

// verify that either all the workItems are time based, meaning d > 0
// or that all the workItems are finite, meaning MaxOps > 0
func verifyWorkItems(workItems []BenchmarkWorkInfo, d time.Duration) {
	if d <= time.Duration(0) {
		for i := range workItems {
			if workItems[i].MaxOps <= 0 {
				log.Fatal("Have a benchmark time <= 0, ", d, ", but workItem ", i, " has MaxOps <= 0, ", workItems[i].MaxOps, ". It should be > 0")
			}
		}
	} else {
		for i := range workItems {
			if workItems[i].MaxOps > 0 {
				log.Fatal("Have a benchmark time > 0, ", d, ", but workItem ", i, " has MaxOps > 0, ", workItems[i].MaxOps, ". It should be <= 0")
			}
		}
	}
}

func RunBenchmark(res BenchmarkResultManager, workItems []BenchmarkWorkInfo, d time.Duration) {
	verifyWorkItems(workItems, d)
	numWorkers := len(workItems)
	log.Println("num workers ", numWorkers)
	workersDone := sync.WaitGroup{}
	registerDone := sync.WaitGroup{}
	benchmarkDone := sync.WaitGroup{}
	// not sure if this batching is wise
	// this channel is used to communicate results
	resultsChannel := make(chan BenchmarkStats, 100)
	// probably a better way to do this
	quitWorkerChannels := make([]chan int, numWorkers) // one for each workitem, one for registerWrites, and one for printWrites
	registerWritesChannel := make(chan int)
	printWritesChannel := make(chan int)
	for i := 0; i < numWorkers; i++ {
		quitWorkerChannels[i] = make(chan int)
	}
	for i := 0; i < numWorkers; i++ {
		workersDone.Add(1)
		// MaxOps <= 0 means we will be running for a certain amount of time
		// and that there is no maximum
		if workItems[i].MaxOps <= 0 {
			go runTimeBasedWorker(workItems[i], resultsChannel, quitWorkerChannels[i], &workersDone)
		} else {
			go runFiniteWorker(workItems[i], resultsChannel, &workersDone)
		}
	}
	registerDone.Add(1)
	go registerWrites(res, resultsChannel, registerWritesChannel, &registerDone)
	benchmarkDone.Add(1)
	go printWrites(res, printWritesChannel, &benchmarkDone)
	time.Sleep(d)
	for i := 0; i < numWorkers; i++ {
		if workItems[i].MaxOps <= 0 {
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
