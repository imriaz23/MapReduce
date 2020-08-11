package MapReduce

import(
	"os"
	"fmt"
	"encoding/json"
	"sort"

)

func doReduce(
	jobName string ,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string ,values [] string) string ,
	){
	keyvalue := make(map[string][]string)
	for i := 0; i < nMap ;i++{
		filename := reduceName(jobName,i,reduceTaskNumber)
		file ,err := os.Open(filename)
		if err != nil{
			fmt.Print(err)
			fmt.Printf("Failed to open file %s",filename)
			continue
		}
		fjson := json.NewDecoder(file)
		for{
			var kv KeyValue
			err_ := fjson.Decode(&kv)
			if err_ != nil{
				break
			}
			if _,ok := keyvalue[kv.Key];ok{
				keyvalue[kv.Key] = append(keyvalue[kv.Key],kv.value)
			} else{
				newvalue := []string{kv.value}
				keyvalue[kv.Key] = newvalue
			}
		}
		file.Close()
	}
	file, err := os.Create(mergeName(jobName,reduceTaskNumber))
	defer file.Close()
	if err != nil {
		fmt.Print(err)
		fmt . Printf ( "Failed to open merge file %s! \n " , mergeName ( jobName , reduceTaskNumber ))
		return
	}

	keys := make([]string,0,len(keyvalue))
	for k, _ := range keyvalue {
		keys = append(keys,k)
	}
	sort.Strings(keys)
	//Increment

	enc := json.NewEncoder(file)
	for _, k := range keys {
		enc.Encode(KeyValue{k,reduceF(k,keyvalue[k])})
	}
}
