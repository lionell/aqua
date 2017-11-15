package main

import (
	"github.com/lionell/aqua/jobs"
	"github.com/lionell/aqua/ops"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func main() {
	log.SetOutput(ioutil.Discard)
	//log.SetOutput(os.Stderr)

	h := []string{"test", "test1", "test2"}
	ds := jobs.NewRandomProducer(time.Millisecond * 15)
	ds = ops.Take(ds, 10)
	//ds, _ = ops.Where(ds, cond.NewFakeCondition(), h)
	//ds = ops.Distinct(ds)
	////ds = ops.Take(ds, 10)
	////ds = ops.Sort(ds, []data.Order{{0, data.DESC}, {1, data.ASC}})
	jobs.RunTabularWriter(os.Stdout, ds, h)

	log.Println("[Main]: Finished.")
}
