package rpcs

import (
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

type CbcollectRpc struct{}

// Unnamed receiver as the receiver is used to access fields of the struct, in this case none
// arg string to specify the location of the zip file
func (*CbcollectRpc) CbCollectLogs(arg *string, reply *string) error {
	dir := *arg
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Info("The directory for cbcollect does not exist")
		*arg = "/root/store/cbcollects_and_captures/"
		log.Info("writing to the default location (%s)", *arg)
	}
	timestamp := time.Now().Format("2006-01-02-150405")
	*arg = filepath.Join(dir, timestamp+"-cbcollect.zip")
	err := exec.Command("/opt/couchbase/bin/cbcollect_info", *arg).Run()
	if err != nil {
		return err
	}
	*reply = "ok"
	return nil
}
