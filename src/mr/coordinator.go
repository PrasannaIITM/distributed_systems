package mr

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAPPER_NOT_STARTED  = 0
	MAPPER_IN_PROGRESS  = 1
	MAPPER_DONE         = 2
	REDUCER_NOT_STARTED = 3
	REDUCER_IN_PROGRESS = 4
	REDUCER_DONE        = 5
	MAPPER_WORK         = "MAP"
	REDUCE_WORK         = "REDUCE"
	TIME_OUT_MAPPER     = 10
	TIME_OUT_REDUCER    = 10
)

type Coordinator struct {
	// Your definitions here.
	filesState             map[string]int
	files                  []string
	nMapper                int // total number of mapper worker
	nReducer               int
	intermediateFiles      []string
	finalIntFiles          []string
	maxReducers            int
	completedReducersCount int
	mu                     sync.Mutex
	assembleFileDone       bool
	jobDone                bool
	workerNum              int
	ReducerFileState       map[string]int
	InvalidMapperWorker    map[int]int
	InvalidReduceWorker    map[int]int
}

var DEBUG = 1

func debugPrintln(a ...interface{}) {
	if DEBUG == 1 {
		fmt.Println(a...)
	}
}

// Your code here -- RPC handlers for the worker to call.
func isMapTaskAvailable(c *Coordinator) (string, bool) {
	for _filename, state := range c.filesState {
		if state == MAPPER_NOT_STARTED {
			return _filename, true
		}
	}
	debugPrintln("No map task available!")
	return "", false
}

func isAllMapDone(c *Coordinator) bool {
	for name, state := range c.filesState {
		if state != MAPPER_DONE {
			debugPrintln("File ", name, "not mapped yet...in state: ", state)
			return false
		}
	}
	debugPrintln("All files mapped!")
	return true
}

func isReduceTaskAvailable(c *Coordinator) (string, bool) {
	for _fileName, state := range c.ReducerFileState {
		if state == REDUCER_NOT_STARTED {
			return _fileName, true
		}
	}
	debugPrintln("No reduce task available!")
	return "", false
}

func isAllReduceDone(c *Coordinator) bool {
	for name, state := range c.ReducerFileState {
		if state != REDUCER_DONE {
			debugPrintln("File ", name, "not reduced yet...in state: ", state)
			return false
		}
	}
	debugPrintln("All files reduced!")
	return true
}

func (c *Coordinator) RequestTask(req MrRequest, reply *MrReply) error {

	task := MrReply{nReduce: c.maxReducers}

	for {
		c.mu.Lock()
		fname, flag := isMapTaskAvailable(c)
		if flag {
			task.FileName = fname
			task.WorkType = MAPPER_WORK
			task.WorkId = c.workerNum + 1
			c.workerNum++
			c.filesState[task.FileName] = MAPPER_IN_PROGRESS

			// start a timer
			ctx, _ := context.WithTimeout(context.Background(), time.Duration(TIME_OUT_MAPPER)*time.Second)
			// start a go routing to handle timeout
			go func(n int, flName string) {
				<-ctx.Done()
				c.mu.Lock()
				defer c.mu.Unlock()

				if c.filesState[flName] != MAPPER_DONE {
					// it means the mapper worker has not completed the work in
					// time therefore => make the filestate of that file to
					// MAPPER_NOT_STARTED
					c.filesState[flName] = MAPPER_NOT_STARTED
					c.InvalidMapperWorker[n] = 1 // do not process MapperDone() RPC
					debugPrintln("Timeout for Mapper Worker ", n, " filename ", flName)
				}
			}(task.WorkId, task.FileName)
			debugPrintln("File ", fname, " is given to worker no ", task.WorkId)
			// done with assigning a mapper task, break the loop
			break
		} else {
			flag := isAllMapDone(c)
			if flag {
				if !c.assembleFileDone {
					dir := "mr-intermediate"
					err := os.Mkdir(dir, 0755)
					if err != nil {
						log.Fatalln(err)
					}
					//files are : /mr-intermediate/mapper-Y
					fileMap := make(map[string]*os.File)

					for i := 0; i < c.maxReducers; i++ {
						fName := "mapper" + "-" + strconv.Itoa(i)
						fName = filepath.Join(dir, fName)
						if _, err := os.Stat(fName); err == nil {
							fileMap[fName], err = os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
							if err != nil {
								log.Fatalln(err)
							}
						} else if os.IsNotExist(err) {
							fileMap[fName], err = os.Create(fName)
							if err != nil {
								log.Fatalln(err)
							}
						} else {
							debugPrintln("Unexpected Error occured in Creating File")
						}
					}

					for _, filePath := range c.intermediateFiles {
						// filePath can be of type /X/mapper-X-Y
						fileName := filepath.Base(filePath) // = mapper-7-4
						_dirName := filepath.Dir(filePath)

						wkn, err := strconv.Atoi(_dirName)
						if err != nil {
							log.Println(err)
						}

						// if this directory belongs to a invalidated worker
						// do not process the directory and ignore
						if _, ok := c.InvalidMapperWorker[wkn]; ok {
							continue
						}

						parts := strings.SplitN(fileName, "-", -1) // parts = [mapper,X,Y]
						fp, err := os.Open(filePath)               // open the file /7/mapper-7-4 in read mode
						if err != nil {
							log.Fatalln(err)
						}
						_, err = io.Copy(fileMap[filepath.Join(dir, "mapper"+"-"+parts[2])], fp)
						if err != nil {
							log.Fatalln(err)
						}
						fp.Close()
					}
					for fl, _ := range fileMap {
						c.finalIntFiles = append(c.finalIntFiles, fl)
					}
					for _, v := range fileMap {
						v.Close()
					}
					c.assembleFileDone = true
					// initialize the states of final intermediate files
					// set them to REDUCER_NOT_STARTED
					for _, flname := range c.finalIntFiles {
						c.ReducerFileState[flname] = REDUCER_NOT_STARTED
					}
				}
				// All map tasks are done final Intermediate files are also ready
				flname, flag := isReduceTaskAvailable(c)
				if flag {
					debugPrintln("Reduce task available")
					task.FileName = flname
					task.WorkType = REDUCE_WORK
					task.WorkId = c.workerNum + 1
					c.ReducerFileState[flname] = REDUCER_IN_PROGRESS
					c.workerNum++

					ctx, _ := context.WithTimeout(context.Background(), time.Duration(
						TIME_OUT_REDUCER)*time.Second)
					// start a go routing to handle timeout
					go func(n int, flName string) {
						<-ctx.Done()

						c.mu.Lock()
						defer c.mu.Unlock()

						if c.ReducerFileState[flName] != REDUCER_DONE {
							// it means the reducer worker has not completed the work in
							// time therefore => make the filestate of that file to
							// REDUCER_NOT_STARTED
							c.ReducerFileState[flName] = REDUCER_NOT_STARTED
							c.InvalidReduceWorker[n] = 1 // do not process ReduceDone() RPC
							debugPrintln("Timeout Reducer Worker ", n, " filename ", flName)
						}
					}(task.WorkId, task.FileName)
					debugPrintln("file ", flname, " is given to worker no ", task.WorkId)
					break
				} else {
					// continue
					flag := isAllReduceDone(c)
					if flag {
						debugPrintln("All Reduce Tasks are assigned")
						task.FileName = "None"
						task.WorkId = -1
						task.WorkType = "None"
						break
					} else {
						debugPrintln("Sleep in the wait of reduce task")
						c.mu.Unlock()
						time.Sleep(1 * time.Second)
						debugPrintln("Woke up after sleep for reduce work")
						continue
					}

				}

			} else {
				// or block
				debugPrintln("Sleep in the wait of map task")
				c.mu.Unlock()
				time.Sleep(10 * time.Second)
				debugPrintln("Woke up after sleep for map work")
				continue
			}

		}
	}
	c.mu.Unlock()
	*reply = task
	return nil
}

func (c *Coordinator) MapperDone(req *MapperRequest, reply *MrEmpty) error {
	for _, filename := range req.FileName {
		var fileExists bool
		for _, ele := range c.intermediateFiles {
			if ele == filename {
				fileExists = true
				break
			}
		}
		if !fileExists {
			c.mu.Lock()
			c.intermediateFiles = append(c.intermediateFiles, filename)
			c.mu.Unlock()
		}
	}

	c.mu.Lock()
	_, ok := c.InvalidMapperWorker[req.WorkId]
	if !ok {
		debugPrintln("Files received from ", req.WorkId, "were good!")
		c.filesState[req.OriginalFileAllocated] = MAPPER_DONE
	} else {
		debugPrintln("Intermediate files from worker = ", req.WorkId, " has been rejected for original file", req.OriginalFileAllocated)
	}
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) ReducerDone(req *ReducerRequest, reply *MrEmpty) error {
	c.mu.Lock()
	_, ok := c.InvalidReduceWorker[req.WorkId]
	if !ok {
		debugPrintln("Files received from ", req.WorkId, "were good!")
		c.ReducerFileState[req.OriginalFileAllocated] = REDUCER_DONE
		c.completedReducersCount += 1
	} else {
		debugPrintln("Output file from worker = ", req.WorkId, " has been rejected for original file", req.OriginalFileAllocated)
		// immediately delete the output file procedured by this invalidated
		// reduce worker
		err := os.Remove(req.FileName)
		if err != nil {
			log.Println(err)
		}
	}

	c.mu.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// if all reducers have finished then say master is done
	c.mu.Lock()
	if c.completedReducersCount == c.maxReducers {
		time.Sleep(time.Second)
		ret = true
		dirsSet := make(map[string]bool)
		for _, f := range c.intermediateFiles {
			if _, ok := dirsSet[filepath.Dir(f)]; !ok {
				dirsSet[filepath.Dir(f)] = true
			}
		}
		for _dir, _ := range dirsSet {
			err := os.RemoveAll(_dir)
			if err != nil {
				log.Println(err)
			}
		}
		err := os.RemoveAll("mr-intermediate")
		if err != nil {
			log.Println(err)
		}
	}
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.filesState = make(map[string]int)
	c.nMapper = 0
	c.intermediateFiles = make([]string, 0)
	c.finalIntFiles = make([]string, 0)

	c.nReducer = 0
	c.maxReducers = nReduce
	c.completedReducersCount = 0
	for _, fileName := range files {
		c.filesState[fileName] = 0
		c.files = append(c.files, fileName)
		c.nMapper++
	}
	// Your code here.

	c.assembleFileDone = false
	c.jobDone = false
	c.workerNum = 0
	c.ReducerFileState = make(map[string]int)
	c.InvalidMapperWorker = make(map[int]int)
	c.InvalidReduceWorker = make(map[int]int)

	c.server()
	return &c
}
