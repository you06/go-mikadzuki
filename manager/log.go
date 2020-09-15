package manager

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/you06/go-mikadzuki/graph"
	"github.com/you06/go-mikadzuki/util"
)

const LOGTIME_FORMAT = "2006-01-02 15:04:05.00000"

var (
	EMPTY_TIME    = time.Time{}
	TIME_MAX      = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	threadPattern = regexp.MustCompile(`thread-(\d+)\.txt`)
	startPattern  = regexp.MustCompile(`.*\[(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{5}).*`)
)

func (m *Manager) DumpGraph(g *graph.Graph) {
	logPath := path.Join(m.cfg.Global.LogPath, util.START_TIME)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		fmt.Println("error create log dir", err)
		return
	}
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
}

func (m *Manager) DumpResult(logs *ExecutionLog) {
	logPath := path.Join(m.cfg.Global.LogPath, util.START_TIME)
	for i := 0; i < logs.thread; i++ {
		logFile, err := os.Create(path.Join(logPath, fmt.Sprintf("thread-%d.log", i)))
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
	errs      [][]error
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
		errs:      make([][]error, thread),
	}
	for i := 0; i < thread; i++ {
		e.startTime[i] = make([]time.Time, action)
		e.endTime[i] = make([]time.Time, action)
		e.tps[i] = make([]graph.ActionTp, action)
		e.sqls[i] = make([]string, action)
		e.status[i] = make([]bool, action)
		e.errs[i] = make([]error, action)
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

func (e *ExecutionLog) LogFail(tID, aID int, err error) {
	e.endTime[tID][aID] = time.Now()
	e.status[tID][aID] = false
	if err == nil {
		panic("err should not be nil when log a failed stmt")
	}
	e.errs[tID][aID] = err
}

func (e *ExecutionLog) LogN(n int) string {
	var (
		b         strings.Builder
		startTime = e.startTime[n]
		endTime   = e.endTime[n]
		tps       = e.tps[n]
		sqls      = e.sqls[n]
		status    = e.status[n]
		errs      = e.errs[n]
	)
	i := 0
	for {
		if i >= e.action || startTime[i] == EMPTY_TIME {
			break
		}
		if status[i] {
			fmt.Fprintf(&b, "[SUCCESS]")
		} else {
			if errs[i] != nil {
				fmt.Fprintf(&b, "[FAILED %s]", errs[i].Error())
			} else {
				fmt.Fprintf(&b, "[UNFINISHED]")
			}
		}
		fmt.Fprintf(&b, " [%s]", tps[i])
		fmt.Fprintf(&b, " [%s-%s] ", startTime[i].Format(LOGTIME_FORMAT), endTime[i].Format(LOGTIME_FORMAT))
		b.WriteString(sqls[i])
		b.WriteString("\n")
		i++
	}
	return b.String()
}

func ParseLog(logPath string) error {
	var files []string
	if err := filepath.Walk(logPath, func(path string, info os.FileInfo, err error) error {
		name := info.Name()
		if threadPattern.MatchString(name) {
			files = append(files, name)
		}
		return nil
	}); err != nil {
		return err
	}

	logs := make(map[int][]string)
	for _, file := range files {
		matches := threadPattern.FindStringSubmatch(file)
		thread, err := strconv.Atoi(matches[1])
		util.AssertNil(err)
		bs, err := ioutil.ReadFile(path.Join(logPath, file))
		if err != nil {
			return err
		}
		content := string(bs)
		logs[thread] = strings.Split(content, "\n")
	}

	combineFile, err := os.Create(path.Join(logPath, "combine.log"))
	if err != nil {
		return err
	}
	defer combineFile.Close()
	combineWriter := bufio.NewWriter(combineFile)

	// TODO: use min-heap
	for {
		least, leastThread := TIME_MAX, -1
		for i, log := range logs {
			startMatch := startPattern.FindStringSubmatch(log[0])
			if len(startMatch) == 0 {
				logs[i] = log[1:]
				if len(logs[i]) == 0 {
					delete(logs, i)
				}
				continue
			}
			startTime, err := time.Parse(LOGTIME_FORMAT, startMatch[1])
			util.AssertNil(err)
			if startTime.Before(least) {
				least = startTime
				leastThread = i
			}
		}
		if !least.Equal(TIME_MAX) {
			if _, err := combineWriter.WriteString(fmt.Sprintf("[THREAD %d] ", leastThread)); err != nil {
				return err
			}
			if _, err := combineWriter.WriteString(logs[leastThread][0] + "\n"); err != nil {
				return err
			}
			logs[leastThread] = logs[leastThread][1:]
			if len(logs[leastThread]) == 0 {
				delete(logs, leastThread)
			}
		} else {
			break
		}
	}

	return combineWriter.Flush()
}
