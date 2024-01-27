package snapshot

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"time"
)

// This package contains the snapshot engine for standalone mode.
// Snapshots in cluster mode will be handled using the raft package in the raft layer.

type Manifest struct {
	LatestSnapshotMilliseconds int64
	LatestSnapshotHash         [16]byte
}

type Opts struct {
	Config                        utils.Config
	StartSnapshot                 func()
	FinishSnapshot                func()
	GetState                      func() map[string]interface{}
	SetLatestSnapshotMilliseconds func(msec int64)
	GetLatestSnapshotMilliseconds func() int64
}

type Engine struct {
	options Opts
}

func NewSnapshotEngine(opts Opts) *Engine {
	return &Engine{
		options: opts,
	}
}

func (engine *Engine) Start() {
	// TODO: Start goroutine for periodic snapshots
}

func (engine *Engine) TakeSnapshot() error {
	engine.options.StartSnapshot()
	defer engine.options.FinishSnapshot()

	// Extract current time
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)

	// Update manifest file to indicate the latest snapshot.
	// If manifest file does not exist, create it.
	// Manifest object will contain the following information:
	// 	1. Hash of the snapshot contents.
	// 	2. Unix time of the latest snapshot taken.
	// The information above will be used to determine whether a snapshot should be taken.
	// If the hash of the current state equals the hash in the manifest file, skip the snapshot.
	// Otherwise, take the snapshot and update the latest snapshot timestamp and hash in the manifest file.

	var firstSnapshot bool // Tracks whether the snapshot being attempted is the first one

	dirname := path.Join(engine.options.Config.DataDir, "snapshots")
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		log.Println(err)
		return err
	}
	// Open manifest file
	var mf *os.File
	mf, err := os.Open(path.Join(dirname, "manifest.bin"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Create file if it does not exist
			mf, err = os.Create(path.Join(dirname, "manifest.bin"))
			if err != nil {
				log.Println(err)
				return err
			}
			firstSnapshot = true
		} else {
			log.Println(err)
			return err
		}
	}

	md, err := io.ReadAll(mf)
	if err != nil {
		log.Println(err)
		return err
	}
	if err := mf.Close(); err != nil {
		log.Println(err)
		return err
	}

	manifest := new(Manifest)

	if !firstSnapshot {
		if err = json.Unmarshal(md, manifest); err != nil {
			log.Println(err)
			return err
		}
	}

	// Get current state
	snapshotObject := utils.SnapshotObject{
		State:                      engine.options.GetState(),
		LatestSnapshotMilliseconds: engine.options.GetLatestSnapshotMilliseconds(),
	}
	out, err := json.Marshal(snapshotObject)
	if err != nil {
		log.Println(err)
		return err
	}

	snapshotHash := md5.Sum(out)
	if snapshotHash == manifest.LatestSnapshotHash {
		return errors.New("nothing new to snapshot")
	}

	// Update the snapshotObject
	snapshotObject.LatestSnapshotMilliseconds = msec
	// Marshal the updated snapshotObject
	out, err = json.Marshal(snapshotObject)
	if err != nil {
		log.Println(err)
		return err
	}

	// os.Create will replace the old manifest file
	mf, err = os.Create(path.Join(dirname, "manifest.bin"))
	if err != nil {
		log.Println(err)
		return err
	}

	// Write the latest manifest data
	manifest = &Manifest{
		LatestSnapshotHash:         md5.Sum(out),
		LatestSnapshotMilliseconds: msec,
	}
	mo, err := json.Marshal(manifest)
	if err != nil {
		log.Println(err)
		return err
	}
	if _, err = mf.Write(mo); err != nil {
		log.Println(err)
		return err
	}
	if err = mf.Close(); err != nil {
		log.Println(err)
		return err
	}

	// Create snapshot directory
	dirname = path.Join(engine.options.Config.DataDir, "snapshots", fmt.Sprintf("%d", msec))
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return err
	}

	// Create snapshot file
	f, err := os.OpenFile(path.Join(dirname, "state.bin"), os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Println(err)
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	// Write state to file
	if _, err = f.Write(out); err != nil {
		return err
	}

	// Set the latest snapshot in unix milliseconds
	engine.options.SetLatestSnapshotMilliseconds(msec)

	return nil
}
