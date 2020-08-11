package MapReduce

import(
	"hash/fnv"
	"os"
	"fmt"
	"encoding/json"
)

func doMap(
	jobName string ,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(file string,contents string )[]KeyValue,
	) {
	f, err := os.Open(inFile)
	defer f.Close()
	if err != nil {
		fmt.Print(err)
		fmt.Println("File open error")
		return
	}

	finfo, err := f.Stat()
	if err != nil {
		fmt.Print(err)
		fmt.Println("Failed To Obtain File Information")
		return
	}
	fcontent := make([]byte, finfo.Size())
	f.Read(fcontent)
	contents := string(fcontent)
	KeyValue := mapF(inFile, contents);
	fjson := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for i := range (fjson) {
		filename := reduceName(jobName, mapTaskNumber, i)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Print(err)
			fmt.Printf("Failed to create file %s", filename)
			return
		}
		fjson[i] = json.NewEncoder(file)
		files[i] = file

	}

	for _, kv := range KeyValue {
		index := ihash(kv.Key) % uint32(nReduce)
		fjson[index].Encode(&kv)

	}
	for _, file := range files {
		file.Close()
	}
}
	func ihash(s string) uint32{
		h := fnv.New32()
		h.Write([]byte(s))
		return h.Sum32()
	}


