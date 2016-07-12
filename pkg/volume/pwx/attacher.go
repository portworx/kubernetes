// build +ignore
/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package pwx
/*
import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	checkSleepDuration  = time.Second
	errorSleepDuration  = 5 * time.Second
)

type pwxVolumeAttacher struct {
	host volume.VolumeHost
}

//var _ volume.Attacher = &pwxVolumeAttacher{}

//var _ volume.AttachableVolumePlugin = &pwxVolumePlugin{}

func (plugin *pwxVolumePlugin) NewAttacher() (volume.Attacher, error) {
	return &pwxVolumeAttacher{host: plugin.host}, nil
	//return nil
}

func (attacher *pwxVolumeAttacher) Attach(spec *volume.Spec, hostName string) error {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return err
	}

	volumeID := volumeSource.VolumeID

	pwxCloud, err := getCloudProvider(attacher.host.GetCloudProvider())
	if err != nil {
		return err
	}

	if _, err = pwxCloud.AttachVolume(volumeID, hostName); err != nil {
		glog.Errorf("Error attaching volume %q: %+v", volumeID, err)
		return err
	}
	return nil
}

func (attacher *pwxVolumeAttacher) WaitForAttach(spec *volume.Spec, timeout time.Duration) (string, error) {
	pwxCloud, err := getCloudProvider(attacher.host.GetCloudProvider())
	if err != nil {
		return "", err
	}

	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	volumeID := volumeSource.VolumeID

	devicePath := ""
	if d, err := pwxCloud.GetVolumePath(volumeID); err == nil {
		devicePath = d
	} else {
		glog.Errorf("GetVolumePath %q gets error %v", volumeID, err)
	}

	ticker := time.NewTicker(checkSleepDuration)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			glog.V(5).Infof("Checking PWX Volume %q is attached.", volumeID)
			if devicePath == "" {
				if d, err := pwxCloud.GetVolumePath(volumeID); err == nil {
					devicePath = d
				} else {
					glog.Errorf("GetDiskPath %q gets error %v", volumeID, err)
				}
			}
			if devicePath != "" {
				path, err := verifyDevicePath(devicePath)
				if err != nil {
					// Log error, if any, and continue checking periodically. See issue #11321
					glog.Errorf("Error verifying PWX Volume (%q) is attached: %v", volumeID, err)
				} else if path != "" {
					// A device path has successfully been created for the PD
					glog.Infof("Successfully found attached PWX Volume %q.", volumeID)
					return path, nil
				}
			} else {
				glog.V(5).Infof("PWX Volume (%q) is not attached yet", volumeID)
			}
		case <-timer.C:
			return "", fmt.Errorf("Could not find attached PWX Volume %q. Timeout waiting for mount paths to be created.", volumeID)
		}
	}
}

func (attacher *pwxVolumeAttacher) GetDeviceMountPath(
	spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return makeGlobalPDPath(attacher.host, volumeSource.VolumeID), nil
}

// FIXME: this method can be further pruned.
func (attacher *pwxVolumeAttacher) MountDevice(spec *volume.Spec, volumeID string, mountPath string) error {

	pwxCloud, err := getCloudProvider(attacher.host.GetCloudProvider())
	if err != nil {
		return err
	}

	mounter := attacher.host.GetMounter()
	notMnt, err := mounter.IsLikelyNotMountPoint(mountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(mountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	_, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return err
	}

	options := []string{}
	if readOnly {
		options = append(options, "ro")
	}
	if notMnt {
		pwxCloud.MountVolume(volumeID, mountPath)
		if err != nil {
			os.Remove(mountPath)
			return err
		}
	}
	return nil
}

type pwxVolumeDetacher struct {
	host volume.VolumeHost
}

var _ volume.Detacher = &pwxVolumeDetacher{}

func (plugin *pwxVolumePlugin) NewDetacher() (volume.Detacher, error) {
	return &pwxVolumeDetacher{
		host: plugin.host,
	}, nil
}

func (detacher *pwxVolumeDetacher) Detach(deviceMountPath string, hostName string) error {
	volumeID := path.Base(deviceMountPath)

	pwxCloud, err := getCloudProvider(detacher.host.GetCloudProvider())
	if err != nil {
		return err
	}

	if _, err = pwxCloud.DetachVolume(volumeID, hostName); err != nil {
		glog.Errorf("Error detaching volumeID %q: %v", volumeID, err)
		return err
	}
	return nil
}

func (detacher *pwxVolumeDetacher) WaitForDetach(devicePath string, timeout time.Duration) error {
	ticker := time.NewTicker(checkSleepDuration)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			// Do we need this??
			glog.V(5).Infof("Checking device %q is detached.", devicePath)
			if pathExists, err := pathExists(devicePath); err != nil {
				return fmt.Errorf("Error checking if device path exists: %v", err)
			} else if !pathExists {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("Timeout reached; PWX Volume %v is still attached", devicePath)
		}
	}
}

func (detacher *pwxVolumeDetacher) UnmountDevice(deviceMountPath string) error {
	volumeID := path.Base(deviceMountPath)
	pwxCloud, err := getCloudProvider(detacher.host.GetCloudProvider())
	if err != nil {
		return err
	}

	err = pwxCloud.UnmountVolume(volumeID, deviceMountPath)
	if err != nil {
		glog.Errorf("Unable to unmount PWX Volume %v", deviceMountPath)
		return err
	}
	return nil
}
*/
