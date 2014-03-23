package main

import (
	"errors"
	"flag"
	"fmt"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry-incubator/simulator/logger"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"
	"github.com/onsi/ginkgo/cleanup"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var executorID = flag.String("executorID", "executor-id", "the executor's ID")
var etcdCluster = flag.String("etcdCluster", "http://127.0.0.1:4001", "comma-separated list of etcd addresses (http://ip:port)")
var containerCreationSleepRange = flag.String("containerCreationSleepRange", "100,500", "min,max in milliseconds")
var runSleepRange = flag.String("runSleepRange", "5000,5001", "min,max in milliseconds")
var heartbeatInterval = flag.Duration("heartbeatInterval", 60*time.Second, "the interval, in seconds, between heartbeats for maintaining presence")
var convergenceInterval = flag.Duration("convergenceInterval", 30*time.Second, "the interval, in seconds, between convergences")
var timeToClaimRunOnce = flag.Duration("timeToClaimRunOnce", 30*time.Minute, "unclaimed run onces are marked as failed, after this time (in seconds)")
var maxMemory = flag.Int("availableMemory", 1000, "amount of available memory")

var lock *sync.Mutex
var currentMemory int
var stop chan bool
var tasks *sync.WaitGroup

var RAND *rand.Rand

func init() {
	RAND = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var MaintainPresenceError = errors.New("failed to maintain presence")

func main() {
	flag.Parse()
	cleanup.Register(func() {
		logger.Info("executor.shuttingdown")
		close(stop)
		tasks.Wait()
		logger.Info("executor.shutdown")
	})
	logger.Component = fmt.Sprintf("EXECUTOR %s", *executorID)

	lock = &sync.Mutex{}
	currentMemory = *maxMemory

	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)
	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("etcd.connect.fatal", err)
	}

	tasks = &sync.WaitGroup{}
	stop = make(chan bool)

	bbs := Bbs.New(etcdAdapter, timeprovider.NewTimeProvider())

	ready := make(chan bool, 1)

	err = maintainPresence(bbs, ready)
	if err != nil {
		logger.Fatal("executor.initializing-presence.failed", err)
	}

	go handleRunOnces(bbs)
	go convergeRunOnces(bbs)

	<-ready

	logger.Info("executor.up")

	select {}
}

func maintainPresence(bbs Bbs.ExecutorBBS, ready chan<- bool) error {
	p, statusChannel, err := bbs.MaintainExecutorPresence(*heartbeatInterval, *executorID)
	if err != nil {
		ready <- false
		return err
	}

	tasks.Add(1)

	go func() {
		for {
			select {
			case locked, ok := <-statusChannel:
				if locked && ready != nil {
					ready <- true
					ready = nil
				}

				if !locked && ok {
					tasks.Done()
					logger.Fatal("maintain.presence.fatal", err)
				}

				if !ok {
					tasks.Done()
					return
				}

			case <-stop:
				p.Remove()
				tasks.Done()
			}
		}
	}()

	return nil
}

func handleRunOnces(bbs Bbs.ExecutorBBS) {
	tasks.Add(1)

	for {
		logger.Info("watch.desired")
		runOnces, stopWatching, errors := bbs.WatchForDesiredRunOnce()

	INNER:
		for {
			select {
			case runOnce, ok := <-runOnces:
				if !ok {
					logger.Info("watch.desired.closed")
					break INNER
				}

				tasks.Add(1)
				go func() {
					handleRunOnce(bbs, runOnce)
					tasks.Done()
				}()
			case err, ok := <-errors:
				if ok && err != nil {
					logger.Error("watch.desired.error", err)
				}
				break INNER
			case <-stop:
				stopWatching <- true
				tasks.Done()
			}
		}
	}
}

func convergeRunOnces(bbs Bbs.ExecutorBBS) {
	statusChannel, releaseLock, err := bbs.MaintainConvergeLock(*convergenceInterval, *executorID)
	if err != nil {
		logger.Fatal("executor.converge-lock.acquire-failed", err)
	}

	tasks.Add(1)

	for {
		select {
		case locked, ok := <-statusChannel:
			if !ok {
				tasks.Done()
				return
			}

			if locked {
				t := time.Now()
				logger.Info("converging")
				bbs.ConvergeRunOnce(*timeToClaimRunOnce)
				logger.Info("converged", time.Since(t))
			} else {
				logger.Error("lost.convergence.lock")
			}
		case <-stop:
			releaseLock <- nil
		}
	}
}

func handleRunOnce(bbs Bbs.ExecutorBBS, runOnce *models.RunOnce) {
	//hesitate
	logger.Info("handling.runonce", runOnce.Guid)
	sleepForARandomInterval("sleep.claim", 0, 100)

	//reserve memory
	ok := reserveMemory(runOnce.MemoryMB)
	if !ok {
		logger.Info("reserve.memory.failed", runOnce.Guid)
		return
	}
	defer releaseMemory(runOnce.MemoryMB)

	//mark claimed
	logger.Info("claiming.runonce", runOnce.Guid)

	err := bbs.ClaimRunOnce(runOnce, *executorID)
	if err != nil {
		logger.Info("claim.runonce.failed", runOnce.Guid, err)
		return
	}

	logger.Info("claimed.runonce", runOnce.Guid)

	//create container

	sleepForContainerCreationInterval()

	//mark started

	logger.Info("starting.runonce", runOnce.Guid)

	err = bbs.StartRunOnce(runOnce, "container")
	if err != nil {
		logger.Error("start.runonce.failed", runOnce.Guid, err)
		return
	}

	logger.Info("started.runonce", runOnce.Guid)

	//run

	sleepForRunInterval()

	//mark completed

	logger.Info("completing.runonce", runOnce.Guid)

	err = bbs.CompleteRunOnce(runOnce, false, "", "")
	if err != nil {
		logger.Error("complete.runonce.failed", runOnce.Guid, err)
		return
	}

	logger.Info("completed.runonce", runOnce.Guid)
}

func reserveMemory(memory int) bool {
	lock.Lock()
	defer lock.Unlock()
	if currentMemory >= memory {
		currentMemory = currentMemory - memory
		return true
	}
	return false
}

func releaseMemory(memory int) {
	lock.Lock()
	defer lock.Unlock()
	currentMemory = currentMemory + memory
	if currentMemory > *maxMemory {
		logger.Error("bookkeeping.fail", "current memory exceeds original max memory... how?")
		currentMemory = *maxMemory
	}
}

func sleepForContainerCreationInterval() {
	containerCreationSleep := strings.Split(*containerCreationSleepRange, ",")
	minContainerCreationSleep, err := strconv.Atoi(containerCreationSleep[0])
	if err != nil {
		logger.Fatal("container.creation.sleep.min.parse.fatal", err)
	}
	maxContainerCreationSleep, err := strconv.Atoi(containerCreationSleep[1])
	if err != nil {
		logger.Fatal("container.creation.sleep.min.parse.fatal", err)
	}
	sleepForARandomInterval("sleep.create", minContainerCreationSleep, maxContainerCreationSleep)
}

func sleepForRunInterval() {
	runSleep := strings.Split(*runSleepRange, ",")
	minRunSleep, err := strconv.Atoi(runSleep[0])
	if err != nil {
		logger.Fatal("run.sleep.min.parse.fatal", err)
	}
	maxRunSleep, err := strconv.Atoi(runSleep[1])
	if err != nil {
		logger.Fatal("run.sleep.min.parse.fatal", err)
	}
	sleepForARandomInterval("sleep.run", minRunSleep, maxRunSleep)
}

func sleepForARandomInterval(reason string, minSleepTime, maxSleepTime int) {
	interval := RAND.Intn(maxSleepTime-minSleepTime) + minSleepTime
	logger.Info(reason, fmt.Sprintf("%dms", interval))
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
