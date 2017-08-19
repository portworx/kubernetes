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
	"os"
	"path"
	"time"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	checkSleepDuration = time.Second
)

type portworxVolumeAttacher struct {
	manager PortworxManager
	host    volume.VolumeHost
}

var _ volume.Attacher = &portworxVolumeAttacher{}

var _ volume.AttachableVolumePlugin = &portworxVolumePlugin{}

func (plugin *portworxVolumePlugin) NewAttacher() (volume.Attacher, error) {
	manager := &PortworxVolumeUtil{
		host: plugin.host,
	}
	return &portworxVolumeAttacher{
		manager: manager,
		host:    plugin.host,
	}, nil
}

func (plugin *portworxVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mount.GetMountRefs(mounter, deviceMountPath)
}

func (attacher *portworxVolumeAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	volumeID := volumeSource.VolumeID

	devicePath, err := attacher.manager.AttachVolume(volumeID, nodeName)
	if err != nil {
		glog.Errorf("Error attaching volume %q to node %q: %+v", volumeID, nodeName, err)
		return "", err
	}

	return devicePath, nil
}

func (attacher *portworxVolumeAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {

	glog.Warningf("Attacher.VolumesAreAttached called for node %q - Please use BulkVerifyVolumes for AWS", nodeName)
	volumeNodeMap := map[types.NodeName][]*volume.Spec{
		nodeName: specs,
	}
	nodeVolumesResult := make(map[*volume.Spec]bool)
	nodesVerificationMap, err := attacher.BulkVerifyVolumes(volumeNodeMap)
	if err != nil {
		glog.Errorf("Attacher.VolumesAreAttached - error checking volumes for node %q with %v", nodeName, err)
		return nodeVolumesResult, err
	}

	if result, ok := nodesVerificationMap[nodeName]; ok {
		return result, nil
	}
	return nodeVolumesResult, nil
}

func (attacher *portworxVolumeAttacher) BulkVerifyVolumes(volumesByNode map[types.NodeName][]*volume.Spec) (map[types.NodeName]map[*volume.Spec]bool, error) {
	volumesAttachedCheck := make(map[types.NodeName]map[*volume.Spec]bool)
	volumeIDsByNode := make(map[types.NodeName][]string)
	volumeSpecMap := make(map[string]*volume.Spec)

	for nodeName, volumeSpecs := range volumesByNode {
		for _, volumeSpec := range volumeSpecs {
			volumeSource, _, err := getVolumeSource(volumeSpec)

			if err != nil {
				glog.Errorf("Error getting volume (%q) source : %v", volumeSpec.Name(), err)
				continue
			}

			volumeIDsByNode[nodeName] = append(volumeIDsByNode[nodeName], volumeSource.VolumeID)

			volumeSpecCheck, exists := volumesAttachedCheck[nodeName]

			if !exists {
				volumeSpecCheck = make(map[*volume.Spec]bool)
			}
			volumeSpecCheck[volumeSpec] = true
			volumeSpecMap[volumeSource.VolumeID] = volumeSpec
			volumesAttachedCheck[nodeName] = volumeSpecCheck
		}
	}
	attachedResult, err := attacher.manager.VolumesAreAttached(volumeIDsByNode)

	if err != nil {
		glog.Errorf("Error checking if volumes are attached to nodes err = %v", err)
		return volumesAttachedCheck, err
	}

	for nodeName, nodeVolumes := range attachedResult {
		for volumeID, attached := range nodeVolumes {
			if !attached {
				spec := volumeSpecMap[volumeID]
				setNodeVolumeSpec(volumesAttachedCheck, spec, nodeName, false)
			}
		}
	}

	return volumesAttachedCheck, nil
}

func (attacher *portworxVolumeAttacher) WaitForAttach(spec *volume.Spec, devicePath string, timeout time.Duration) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	volumeID := volumeSource.VolumeID

	if devicePath == "" {
		return "", fmt.Errorf("WaitForAttach failed for Portworx Volume %q: devicePath is empty.", volumeID)
	}

	ticker := time.NewTicker(checkSleepDuration)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			glog.V(5).Infof("Checking Portworx Volume %q is attached.", volumeID)
			if devicePath != "" {
				path, err := verifyDevicePath(devicePath)
				if err != nil {
					// Log error, if any, and continue checking periodically. See issue #11321
					glog.Errorf("Error verifying Portworx Volume (%q) is attached: %v", volumeID, err)
				} else if path != "" {
					// A device path has successfully been created for the PD
					glog.Infof("Successfully found attached Portworx Volume %q.", volumeID)
					return path, nil
				}
			} else {
				glog.V(5).Infof("Portworx Volume (%q) is not attached yet", volumeID)
			}
		case <-timer.C:
			return "", fmt.Errorf("Could not find attached Portworx Volume %q. Timeout waiting for mount paths to be created.", volumeID)
		}
	}
}

func (attacher *portworxVolumeAttacher) GetDeviceMountPath(
	spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return makeGlobalVolumePath(attacher.host, volumeSource.VolumeID), nil
}

// FIXME: this method can be further pruned.
func (attacher *portworxVolumeAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string) error {
	mounter := attacher.host.GetMounter()
	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return err
	}

	if notMnt {
		err := attacher.manager.MountVolume(volumeSource.VolumeID, deviceMountPath)
		if err != nil {
			os.Remove(deviceMountPath)
			return err
		}
	}
	return nil
}

type portworxVolumeDetacher struct {
	mounter mount.Interface
	manager PortworxManager
}

var _ volume.Detacher = &portworxVolumeDetacher{}

func (plugin *portworxVolumePlugin) NewDetacher() (volume.Detacher, error) {
	manager := &PortworxVolumeUtil{
		host: plugin.host,
	}
	return &portworxVolumeDetacher{
		mounter: plugin.host.GetMounter(),
		manager: manager,
	}, nil
}

func (detacher *portworxVolumeDetacher) Detach(deviceMountPath string, nodeName types.NodeName) error {
	volumeID := path.Base(deviceMountPath)

	attached, err := detacher.manager.VolumeIsAttached(volumeID, nodeName)
	if err != nil {
		// Log error and continue with detach
		glog.Errorf(
			"Error checking if volume (%q) is already attached to current node (%q). Will continue and try detach anyway. err=%v",
			volumeID, nodeName, err)
	}

	if err == nil && !attached {
		// Volume is already detached from node.
		glog.Infof("detach operation was successful. volume %q is already detached from node %q.", volumeID, nodeName)
		return nil
	}

	if err := detacher.manager.DetachVolume(volumeID, nodeName); err != nil {
		glog.Errorf("Error detaching volumeID %q: %v", volumeID, err)
		return err
	}
	return nil
}

func (detacher *portworxVolumeDetacher) UnmountDevice(deviceMountPath string) error {
	volumeID := path.Base(deviceMountPath)
	return detacher.manager.UnmountVolume(volumeID, deviceMountPath)
}

func setNodeVolumeSpec(
	nodeVolumeMap map[types.NodeName]map[*volume.Spec]bool,
	volumeSpec *volume.Spec,
	nodeName types.NodeName,
	check bool) {

	volumeMap := nodeVolumeMap[nodeName]
	if volumeMap == nil {
		volumeMap = make(map[*volume.Spec]bool)
		nodeVolumeMap[nodeName] = volumeMap
	}
	volumeMap[volumeSpec] = check
}
