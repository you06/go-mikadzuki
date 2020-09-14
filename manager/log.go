package manager

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/you06/go-mikadzuki/graph"
	"github.com/you06/go-mikadzuki/util"
)

const LOGTIME_FORMAT = "2006-01-02 15:04:05.00000"

var EMPTY_TIME = time.Time{}

func (m *Manager) DumpResult(g *graph.Graph, logs *ExecutionLog) {
	logPath := path.Join(m.cfg.Global.LogPath, util.START_TIME)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		fmt.Println("error create log dir", err)
		return
	}
	// write graph
	graphFile, err := os.Create(path.Join(logPath, "graph.txt"))
	if err != nil {
		fmt.Println("create graph log failed")
		return
	}
	graphWriter := bufio.NewWriter(graphFile)
	if _, err := graphWriter.WriteString(g.String()); err != nil {
		fmt.Println("write graph log failed")
		return
	}
	if err := graphWriter.Flush(); err != nil {
		fmt.Println("flush graph log failed")
		return
	}
	if err := graphFile.Close(); err != nil {
		fmt.Println("close graph log failed")
		return
	}
	// write execution log
	for i := 0; i < logs.thread; i++ {
		logFile, err := os.Create(path.Join(logPath, fmt.Sprintf("thread-%d.txt", i)))
		if err != nil {
			fmt.Printf("create thread-%d log failed\n", i)
			continue
		}
		logWriter := bufio.NewWriter(logFile)
		if _, err := logWriter.WriteString(logs.LogN(i)); err != nil {
			fmt.Printf("write thread-%d log failed\n", i)
			continue
		}
		if err := logWriter.Flush(); err != nil {
			fmt.Printf("flush thread-%d log failed\n", i)
			continue
		}
		if err := logFile.Close(); err != nil {
			fmt.Printf("close thread-%d log failed\n", i)
			continue
		}
	}
}

type ExecutionLog struct {
	thread    int
	action    int
	startTime [][]time.Time
	endTime   [][]time.Time
	tps       [][]graph.ActionTp
	sqls      [][]string
	status    [][]bool
}

func NewExecutionLog(thread, action int) *ExecutionLog {
	e := ExecutionLog{
		thread:    thread,
		action:    action,
		startTime: make([][]time.Time, thread),
		endTime:   make([][]time.Time, thread),
		tps:       make([][]graph.ActionTp, thread),
		sqls:      make([][]string, thread),
		status:    make([][]bool, thread),
	}
	for i := 0; i < thread; i++ {
		e.startTime[i] = make([]time.Time, action)
		e.endTime[i] = make([]time.Time, action)
		e.tps[i] = make([]graph.ActionTp, action)
		e.sqls[i] = make([]string, action)
		e.status[i] = make([]bool, action)
	}
	return &e
}

func (e *ExecutionLog) LogStart(tID, aID int, tp graph.ActionTp, sql string) {
	e.startTime[tID][aID] = time.Now()
	e.tps[tID][aID] = tp
	e.sqls[tID][aID] = sql
}

func (e *ExecutionLog) LogSuccess(tID, aID int) {
	e.endTime[tID][aID] = time.Now()
	e.status[tID][aID] = true
}

func (e *ExecutionLog) LogFail(tID, aID int) {
	e.endTime[tID][aID] = time.Now()
	e.status[tID][aID] = false
}

func (e *ExecutionLog) LogN(n int) string {
	var (
		b         strings.Builder
		startTime = e.startTime[n]
		endTime   = e.endTime[n]
		tps       = e.tps[n]
		sqls      = e.sqls[n]
		status    = e.status[n]
	)
	i := 0
	for {
		if i >= e.action || startTime[i] == EMPTY_TIME {
			break
		}
		fmt.Fprintf(&b, "[%s]", tps[i])
		if status[i] {
			fmt.Fprintf(&b, " [SUCCESS]")
		} else {
			fmt.Fprintf(&b, " [FAILED]")
		}
		fmt.Fprintf(&b, " [%s-%s] ", startTime[i].Format(LOGTIME_FORMAT), endTime[i].Format(LOGTIME_FORMAT))
		b.WriteString(sqls[i])
		b.WriteString("\n")
		i++
	}
	return b.String()
}
