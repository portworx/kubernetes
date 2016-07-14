package common

import (
	"go.pedge.io/proto/time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/portworx/kvdb"
)

var (
	// BlockNotSupported is a default (null) block driver implementation.  This can be
	// used by drivers that do not want to (or care about) implementing the attach,
	// format and detach interfaces.
	BlockNotSupported    = &blockNotSupported{}
	SnapshotNotSupported = &snapshotNotSupported{}
	IONotSupported       = &ioNotSupported{}
)

// NewVolume returns a new api.Volume for a driver Create call.
func NewVolume(
	volumeID string,
	fsType api.FSType,
	volumeLocator *api.VolumeLocator,
	source *api.Source,
	volumeSpec *api.VolumeSpec,
) *api.Volume {
	return &api.Volume{
		Id:       volumeID,
		Locator:  volumeLocator,
		Ctime:    prototime.Now(),
		Spec:     volumeSpec,
		Source:   source,
		LastScan: prototime.Now(),
		Format:   fsType,
		State:    api.VolumeState_VOLUME_STATE_AVAILABLE,
		Status:   api.VolumeStatus_VOLUME_STATUS_UP,
	}
}

func NewDefaultStoreEnumerator(driver string, kvdb kvdb.Kvdb) volume.StoreEnumerator {
	return newDefaultStoreEnumerator(driver, kvdb)
}
