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
	threadPattern = regexp.MustCompile(`thread-(\d+)\.log`)
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
	thread int
	action int
	logs   [][]SQLLog
}

type SQLLog struct {
	startTime time.Time
	endTime   time.Time
	tp        graph.ActionTp
	sql       string
	status    bool
	err       error
}

func NewExecutionLog(thread, action int) *ExecutionLog {
	e := ExecutionLog{
		thread: thread,
		action: action,
		logs:   make([][]SQLLog, action),
	}
	return &e
}

func (e *ExecutionLog) LogStart(tID int, tp graph.ActionTp, sql string) int {
	e.logs[tID] = append(e.logs[tID], SQLLog{
		startTime: time.Now(),
		endTime:   EMPTY_TIME,
		tp:        tp,
		sql:       sql,
		status:    false,
		err:       nil,
	})
	return len(e.logs[tID]) - 1
}

func (e *ExecutionLog) LogSuccess(tID, aID int) {
	e.logs[tID][aID].endTime = time.Now()
	e.logs[tID][aID].status = true
}

func (e *ExecutionLog) LogFail(tID, aID int, err error) {
	e.logs[tID][aID].endTime = time.Now()
	e.logs[tID][aID].status = false
	if err == nil {
		panic("err should not be nil when log a failed stmt")
	}
	e.logs[tID][aID].err = err
}

func (e *ExecutionLog) LogN(n int) string {
	var (
		b    strings.Builder
		logs = e.logs[n]
	)
	for _, log := range logs {
		if log.status {
			fmt.Fprintf(&b, "[SUCCESS]")
		} else {
			if log.err != nil {
				fmt.Fprintf(&b, "[FAILED %s]", log.err.Error())
			} else {
				fmt.Fprintf(&b, "[UNFINISHED]")
			}
		}
		fmt.Fprintf(&b, " [%s]", log.tp)
		fmt.Fprintf(&b, " [%s-%s] ", log.startTime.Format(LOGTIME_FORMAT), log.endTime.Format(LOGTIME_FORMAT))
		b.WriteString(log.sql)
		b.WriteString("\n")
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
	defer func() {
		if err := combineFile.Close(); err != nil {
			fmt.Println(err)
		}
	}()
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
