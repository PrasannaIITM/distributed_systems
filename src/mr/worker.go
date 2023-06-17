package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		var filename string
		var workId int
		var worktype string
		var nReduce int

		filename, workId, worktype, nReduce = CallRequestTask()
		fmt.Println(workId, "Requested new task from coordinator. Details:")
		fmt.Println("filename=", filename, "worktype=", worktype)
		fmt.Println("Nreduce=", nReduce)

		if worktype == "MAP" {
			fmt.Println(workId, "Got a map task from coordinator, filename =", filename)

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalln("Cannot open ", filename, "Error: ", err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalln("Cannot read ", filename, "Error: ", err)
			}
			file.Close()

			kva := mapf(filename, string(content))

			// create intermediate files workId/mapper-X-Y
			// X: workId, Y: reduce task number
			dir := strconv.Itoa(workId)
			err = os.Mkdir(dir, 0755)
			if err != nil {
				log.Fatalln(err)
			}
			fileMap := make(map[string]*os.File)
			for i := 0; i < 10; i++ {
				intFName := "mapper-" + strconv.Itoa(workId) + "-" + strconv.Itoa(i)
				intFName = filepath.Join(dir, intFName)

				_, err := os.Stat(intFName)
				if err == nil {
					fileMap[intFName], err = os.OpenFile(intFName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
					if err != nil {
						log.Fatalln(err)
					}
				} else if os.IsNotExist(err) {
					fileMap[intFName], err = os.Create(intFName)
					if err != nil {
						log.Fatalln(err)
					}
				} else {
					log.Fatalln("Unexpected error: ", err)
				}
			}

			var FileSet = make(map[string]bool)
			for _, kv := range kva {
				binNum := ihash(kv.Key) % 10
				intFName := "mapper-" + strconv.Itoa(workId) + "-" + strconv.Itoa(binNum)
				intFName = filepath.Join(dir, intFName)
				if _, ok := FileSet[intFName]; !ok {
					FileSet[intFName] = true
				}
				// fmt.Println(kv)
				enc := json.NewEncoder(fileMap[intFName])
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalln("Unable to encode to json.", err)
				}
			}

			for _, fp := range fileMap {
				fp.Close()
			}

			intFileList := make([]string, 0)
			for k, _ := range FileSet {
				intFileList = append(intFileList, k)
			}
			fmt.Println(workId, "Number of files sent to master = ", len(intFileList)) // output should be <= nReduce
			mRequest := MapperRequest{}                                                // 2 means mapper task is done
			mRequest.FileName = intFileList
			mRequest.OriginalFileAllocated = filename
			mRequest.WorkId = workId
			CallMapperDone(mRequest)
			fmt.Println("Mapper Task Done! :", workId)

		} else {
			fmt.Println("Got a reduce task from coordinator, filename = ", filename)

			intFName := filename
			intkva := []KeyValue{}
			fp, err := os.Open(intFName)
			if err != nil {
				log.Fatalln("HERE", err)
			}
			dec := json.NewDecoder(fp)
			for {
				var kv KeyValue
				if err = dec.Decode(&kv); err != nil {
					break
				}
				intkva = append(intkva, kv)
			}
			fp.Close()

			sort.Sort(ByKey(intkva))

			// write results to a temp file and atomically rename it later
			opFName := "mr-out-" + strconv.Itoa(workId)
			opFile, _ := os.Create(opFName)

			i := 0
			for i < len(intkva) {
				j := i + 1
				for j < len(intkva) && intkva[j].Key == intkva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intkva[k].Value)
				}
				output := reducef(intkva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(opFile, "%v %v\n", intkva[i].Key, output)

				i = j
			}

			opFile.Close()

			rReq := ReducerRequest{opFName, filename, workId}
			CallReducerDone(rReq)
			fmt.Println("Reducer Task Done! :", workId)

		}

	}
}

func CallRequestTask() (string, int, string, int) {

	// declare an argument structure.
	req := MrRequest{}

	// declare a reply structure.
	reply := MrReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &req, &reply)

	return reply.FileName, reply.WorkId, reply.WorkType, reply.nReduce
}

func CallMapperDone(req MapperRequest) {
	// declare a reply structure.
	reply := MrEmpty{}

	// send the RPC request, wait for the reply.
	call("Coordinator.MapperDone", &req, &reply)
}

func CallReducerDone(req ReducerRequest) {
	// declare a reply structure.
	reply := MrEmpty{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ReducerDone", &req, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
