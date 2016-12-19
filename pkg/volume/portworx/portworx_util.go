/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package portworx

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/golang/glog"
	osdapi "github.com/libopenstorage/openstorage/api"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	osdvolume "github.com/libopenstorage/openstorage/volume"
	osdclient "github.com/libopenstorage/openstorage/api/client"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	osdMgmtPort           = "9001"
	osdDriverVersion      = "v1"
	pxdDriverName         = "pxd"
	pxdDevicePrefix       = "/dev/pxd/pxd"
	replLabel             = "repl"
	cosLabel              = "io_priority"
	sharedLabel           = "shared"
	fsLabel               = "fs"
	snapshotIntervalLabel = "snapshot_interval"
	aggregationLabel      = "aggregation_level"
	blocksizeLabel        = "block_size"
)

type PortworxVolumeUtil struct{
	portworxClient *osdclient.Client
}

func (util *PortworxVolumeUtil) DeleteVolume(d *portworxVolumeDeleter) error {
	hostName := d.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return err
	}

	err = client.Delete(d.volumeID)
	if err != nil {
		glog.Infof("Error in Volume Delete for (%v): %v", d.volName)
		return err
	}
	return nil
}

// CreateVolume creates a Portworx volume.
// Returns: volumeID, volumeSize, labels, error
func (util *PortworxVolumeUtil) CreateVolume(p *portworxVolumeProvisioner) (string, int, map[string]string, error) {
	hostName := p.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return "", 0, nil, err
	}

	capacity := p.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	// Portworx Volumes are specified in GB
	requestGB := int(volume.RoundUpSize(capacity.Value(), 1024*1024*1024))

	var labels map[string]string
	if p.options.CloudTags == nil {
		labels = make(map[string]string)
	} else {
		labels = *p.options.CloudTags
	}

	spec := osdapi.VolumeSpec{
		Size: uint64(requestGB*1024*1024*1024),
		// Default format is btrfs
		Format: osdapi.FSType_FS_TYPE_BTRFS,
	}

	for k, v := range p.options.Parameters {
		switch strings.ToLower(k) {
		case replLabel:
			repl, err := strconv.Atoi(v)
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid replication factor %q, must be [1..3]", v)
			}
			spec.HaLevel = int64(repl)
		case fsLabel:
			fsType, err := osdapi.FSTypeSimpleValueOf(v)
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid fs value %q, must be ext4|btrfs", v)
			}
			spec.Format = fsType
		case snapshotIntervalLabel:
			si, err := strconv.Atoi(v)
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid snapshot interval value %q, must be a number", v)
			}
			err = checkSnapInterval(si)
			if err != nil {
				return "", 0, nil, err
			}
			spec.SnapshotInterval = uint32(si)
		case cosLabel:
			cos, err := cosLevel(v)
			if err != nil {
				return "", 0, nil, err
			}
			spec.Cos = cos
		case sharedLabel:
			spec.Shared = true
		case blocksizeLabel:
			blocksize, err := strconv.Atoi(v)
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid block_size value %q, must be a number ", v)
			}
			spec.BlockSize = int64(blocksize)
		case aggregationLabel:
			agg, err := strconv.Atoi(v)
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid aggregation_level %q, must be a number ", v)
			}
			spec.AggregationLevel = uint32(agg)
		default:
			labels[strings.ToLower(k)] = v
		}
	}
	source := osdapi.Source{}
	locator := osdapi.VolumeLocator{
		Name:         p.options.PVName,
		VolumeLabels: labels,
	}
	volumeID, err := client.Create(&locator, &source, &spec)
	if err != nil {
		glog.Infof("Error in Volume Create : %v", err)
	}
	return volumeID, requestGB, nil, err
}

func (util *PortworxVolumeUtil) osdClient(hostName string) (osdvolume.VolumeDriver, error) {
	if util.portworxClient == nil {
		var clientUrl string
		if !strings.HasPrefix(hostName, "http://") {
			clientUrl = "http://" + hostName + ":" + osdMgmtPort
		} else {
			clientUrl = hostName + ":" + osdMgmtPort
		}
		
		driverClient, err := volumeclient.NewDriverClient(clientUrl, pxdDriverName, osdDriverVersion)
		if err != nil {
			return nil, err
		}
		util.portworxClient = driverClient
	}

	return volumeclient.VolumeDriver(util.portworxClient), nil
}

func (util *PortworxVolumeUtil) AttachVolume(m *portworxVolumeMounter) (string, error) {
	hostName := m.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return "", err
	}

	devicePath, err := client.Attach(m.volumeID)
	if err != nil {
		if err == osdvolume.ErrVolAttachedOnRemoteNode {
			// Volume is already attached to node.
			glog.V(2).Infof("Attach operation is unsuccessful. Volume %q is already attached to another node.", m.volumeID)
			return "", err
		}
		glog.V(2).Infof("AttachVolume on %v failed with error %v", m.volumeID, err)
		return "", err
	}
	return devicePath, nil
}

func (util *PortworxVolumeUtil) DetachVolume(u *portworxVolumeUnmounter, deviceName string) error {
	hostName := u.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return err
	}

	volumeID, err := getVolumeIDFromDeviceName(deviceName)
	if err != nil {
		return err
	}

	err = client.Detach(volumeID)
	if err != nil {
		glog.V(2).Infof("DetachVolume on %v failed with error %v", u.volumeID, err)
		return err
	}
	return nil
}

func (util *PortworxVolumeUtil) MountVolume(m *portworxVolumeMounter, mountPath string) error {
	hostName := m.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return err
	}

	err = client.Mount(m.volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("MountVolume on %v failed with error %v", m.volumeID, err)
		return err
	}
	return nil
}

func (util *PortworxVolumeUtil) UnmountVolume(u *portworxVolumeUnmounter, deviceName, mountPath string) error {
	hostName := u.plugin.host.GetHostName()
	client, err := util.osdClient(hostName)
	if err != nil {
		return err
	}

	volumeID, err := getVolumeIDFromDeviceName(deviceName)
	if err != nil {
		return err
	}

	err = client.Unmount(volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("UnmountVolume on mountPath: %v, VolName: %v failed with error %v", mountPath, u.volName, err)
		return err
	}
	return nil
}

func getVolumeIDFromDeviceName(deviceName string) (string, error) {
	if !strings.HasPrefix(deviceName, pxdDevicePrefix) {
		return "", fmt.Errorf("Invalid DeviceName for Portworx Volume: %v", deviceName)
	}
	return strings.TrimPrefix(deviceName, pxdDevicePrefix), nil
}

func checkSnapInterval(snapInterval int) error {
	if snapInterval == math.MaxUint32 || snapInterval < 0 ||
		(snapInterval > 0 && snapInterval < 60) {
		return fmt.Errorf(
			"Interval must be greater than or equal to 60 and less than %v"+
				" or 0 to disable scheduled snapshots",
			math.MaxUint32)
	}
	return nil
}

func cosLevel(cos string) (osdapi.CosType, error) {
	switch cos {
	case "high", "3":
		return osdapi.CosType_HIGH, nil
	case "medium", "2":
		return osdapi.CosType_MEDIUM, nil
	case "low", "1":
		return osdapi.CosType_LOW, nil
	}
	return osdapi.CosType_LOW,
		fmt.Errorf("Cos must be one of %q | %q | %q", "high", "medium", "low")
}
