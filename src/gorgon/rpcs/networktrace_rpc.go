package rpcs

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

// Field in RPC struct to capture PID of initialized tshark
type NetworkCaptureRpc struct {
	tsharkCmd *exec.Cmd
}

type NetworkCaptureConfig struct {
	TsharkTimeout time.Duration
	Directory     string
}

// first arg for directory for captured network dump
func (rpc *NetworkCaptureRpc) StartCapture(arg *NetworkCaptureConfig, reply *string) error {
	dir := arg.Directory
	timeout := strconv.Itoa(int(arg.TsharkTimeout.Seconds()))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Info("Provided directory doesn't exist, using default")
		dir = "/root/store/cbcollects_and_captures"
	}
	timestamp := time.Now().Format("2006-01-02-150405")
	outputFile := filepath.Join(dir, timestamp+"-capture.pcap")
	rpc.tsharkCmd = exec.Command("tshark", "-i", "any", "-s", "0", "-a", "duration:"+timeout, "-w", outputFile)
	err := rpc.tsharkCmd.Start()
	if err != nil {
		log.Error("Tshark start failed: %v", err)
		return err
	}
	log.Info("Network Capture started with pid (%v)", rpc.tsharkCmd.Process.Pid)
	*reply = "ok"
	return nil
}

func (rpc *NetworkCaptureRpc) StopCapture(arg *string, reply *string) error {
	if rpc.tsharkCmd != nil && rpc.tsharkCmd.Process != nil {
		err := rpc.tsharkCmd.Process.Kill()
		if err != nil {
			log.Error("Failed to kill tshark: %v", err)
			return err
		}
		log.Info("Successfully killed tshark with PID: %d", rpc.tsharkCmd.Process.Pid)

		// Wait for the process to actually exit
		_, err = rpc.tsharkCmd.Process.Wait()
		if err != nil {
			log.Warning("Process wait returned error: %v", err)
		}

		// Clear the cmd reference
		rpc.tsharkCmd = nil
	} else {
		log.Info("No tshark process to stop")
	}

	*reply = "ok"
	return nil
}
