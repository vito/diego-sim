package main

import (
	"encoding/json"
	"fmt"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry-incubator/simulator/logger"
	"github.com/onsi/ginkgo/cleanup"
	"io"
	"os"
	"path/filepath"
	"time"
)

type etcdData struct {
	Time              float64        `json:"time"`
	ReadTime          float64        `json:"read_time"`
	Pending           int            `json:"pending"`
	Claimed           int            `json:"claimed"`
	Running           int            `json:"running"`
	Completed         int            `json:"completed"`
	PresentExecutors  int            `json:"present_executors"`
	RunningByExecutor map[string]int `json:"running_by_executor"`
}

func (d *etcdData) toJson() []byte {
	data, err := json.Marshal(d)
	if err != nil {
		logger.Error("etcd.marshal.etcdData.failed", err)
	}
	return data
}

func (d *etcdData) String() string {
	return fmt.Sprintf("Executors: %d Pending: %d, Claimed: %d, Running: %d, Completed: %d", d.PresentExecutors, d.Pending, d.Claimed, d.Running, d.Completed)
}

func monitorETCD() {
	outputFile, err := os.Create(filepath.Join(outDir, "etcdstats.log"))
	if err != nil {
		logger.Fatal("etcd.log.creation.failure", err)
	}
	cleanup.Register(func() {
		outputFile.Sync()
	})

	go monitorRunOnces(outputFile)
}

func monitorRunOnces(out io.Writer) {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			t := time.Now()
			logger.Info("fetch.etcd.runonce.data")
			runOnceNodes, err := etcdAdapter.ListRecursively(Bbs.RunOnceSchemaRoot)
			if err != nil {
				logger.Info("fetch.etcd.runOnceNodes.error", err)
			}

			executorNode, err := etcdAdapter.ListRecursively(Bbs.ExecutorSchemaRoot)
			if err != nil {
				logger.Info("fetch.etcd.executorNode.error", err)
			}
			readTime := time.Since(t)

			d := etcdData{
				Time:              float64(time.Now().UnixNano()) / 1e9,
				RunningByExecutor: map[string]int{},
				PresentExecutors:  len(executorNode.ChildNodes),
				ReadTime:          float64(readTime) / 1e9,
			}

			for _, node := range runOnceNodes.ChildNodes {
				runOnce, err := models.NewRunOnceFromJSON(node.Value)
				if err != nil {
					logger.Error("etcd.decode.runonce", err)
					continue
				}

				switch runOnce.State {
				case models.RunOnceStatePending:
					d.Pending++
				case models.RunOnceStateClaimed:
					d.Claimed++
				case models.RunOnceStateRunning:
					d.Running++
					d.RunningByExecutor[runOnce.ExecutorID]++
				case models.RunOnceStateCompleted:
					d.Completed++
				}
			}

			logger.Info("fetched.etcd.runonce.data", time.Since(t), d.String())
			out.Write(d.toJson())
			out.Write([]byte("\n"))
		}
	}()
}
