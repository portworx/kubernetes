/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

import (
	"fmt"
	"os"
	"strings"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/cloudprovider"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/pwx"
	"k8s.io/kubernetes/pkg/volume"
	osdapi "github.com/libopenstorage/openstorage/api"
	osdclient "github.com/libopenstorage/openstorage/api/client"
	osdvolume "github.com/libopenstorage/openstorage/volume"

)

const (
	osdMgmtPort = "9007"
	osdDriverVersion = "v1"
	pxdDevicePrefix = "/dev/pxd/pxd"
)

type PWXDiskUtil struct{}

func (util *PWXDiskUtil) DeleteVolume(d *pwxVolumeDeleter) error {
	/*cloud, err := getCloudProvider(d.pwxVolume.plugin.host.GetCloudProvider())
	if err != nil {
		return err
	}

	deleted, err := cloud.DeleteVolume(d.volumeID)
	if err != nil {
		glog.V(2).Infof("Error deleting PWX volume %s: %v", d.volumeID, err)
		return err
	}
	if deleted {
		glog.V(2).Infof("Successfully deleted PWX volume %s", d.volumeID)
	} else {
		glog.V(2).Infof("Successfully deleted PWX volume %s (actually already deleted)", d.volumeID)
	}*/
	return nil
}

// CreateVolume creates a PWX volume.
// Returns: volumeID, volumeSize, labels, error
func (util *PWXDiskUtil) CreateVolume(p *pwxVolumeProvisioner) (string, int, map[string]string, error) {
	hostName := p.plugin.host.GetHostName()
	client, err := util.newOsdClient(hostName)
	if err != nil {
		return "", 0, nil, err
	}

	requestBytes := p.options.Capacity.Value()
	// PWX works in GB
	requestGB := int(volume.RoundUpSize(requestBytes, 1024*1024*1024))

	spec := osdapi.VolumeSpec{
		Size: uint64(requestGB),
	}
	source := osdapi.Source{}
	locator := osdapi.VolumeLocator{
		Name: p.options.PVName,
		VolumeLabels: *p.options.CloudTags,
	}
	volumeID, err := client.Create(&locator, &source, &spec)
	if err != nil {
		glog.Infof("Error in Volume Create : %v", err)
	}
	return volumeID, requestGB, *p.options.CloudTags, err

	/*cloud, err := getCloudProvider(p.pwxVolume.plugin.host.GetCloudProvider())
	if err != nil {
		return "", 0, nil, err
	}

	var labels map[string]string
	if p.options.CloudTags == nil {
		labels = make(map[string]string)
	} else {
		labels = *c.options.CloudTags
	}

	volumeName = volume.GenerateVolumeName(c.options.ClusterName, c.options.PVName, 255) // AWS tags can have 255 characters

	requestBytes := c.options.Capacity.Value()

	volumeOptions := &pwx.VolumeOptions{
		SizeInBytes:  requestBytes,
		VolumeLabels: labels,
	}

	name, err := cloud.CreateVolume(volumeOptions)
	if err != nil {
		glog.V(2).Infof("Error creating PWX volume: %v", err)
		return "", 0, nil, err
	}
	glog.V(2).Infof("Successfully created PWX volume %s", name)

	labels, err = cloud.GetVolumeLabels(name)
	if err != nil {
		// We don't really want to leak the volume here...
		glog.Errorf("error building labels for new PWXx volume %q: %v", name, err)
	}

	return name, requestBytes, labels, nil*/
	//return "", 0, nil, nil
}

func (util *PWXDiskUtil) newOsdClient(hostName string) (osdvolume.VolumeDriver, error) {
	var clientUrl string
	if !strings.HasPrefix(hostName, "http://") {
		clientUrl = "http://" + hostName + ":" + osdMgmtPort
	} else {
		clientUrl = hostName + ":" + osdMgmtPort
	}

	client, err := osdclient.NewClient(clientUrl, osdDriverVersion)
	if err != nil {
		return nil, err
	}

	return client.VolumeDriver(), nil
}

func (util *PWXDiskUtil) AttachVolume(m *pwxVolumeMounter) (string, error) {
	hostName := m.plugin.host.GetHostName()
	client, err := util.newOsdClient(hostName)
	if err != nil {
		return "", err
	}

	devicePath, err := client.Attach(m.volumeID)
	if err != nil {
		if err == osdvolume.ErrVolAttachedOnRemoteNode {
			// Volume is already attached to node.
			glog.Infof("Attach operation is unsuccessful. Volume %q is already attached to another node.", m.volumeID)
			return "", err
		}
		glog.V(2).Infof("AttachVolume on %v failed with error %v", m.volumeID, err)
		return "", err
	}
	return devicePath, err
}

func (util *PWXDiskUtil) DetachVolume(u *pwxVolumeUnmounter, deviceName string) error {
	hostName := u.plugin.host.GetHostName()
	client, err := util.newOsdClient(hostName)
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
	return err
}

func (util *PWXDiskUtil) MountVolume(m *pwxVolumeMounter, mountPath string) error {
	hostName := m.plugin.host.GetHostName()
	client, err := util.newOsdClient(hostName)
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

func (util *PWXDiskUtil) UnmountVolume(u *pwxVolumeUnmounter, deviceName, mountPath string) error {
	hostName := u.plugin.host.GetHostName()
	client, err := util.newOsdClient(hostName)
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
	return err
}

func getVolumeIDFromDeviceName(deviceName string) (string, error) {
	if !strings.HasPrefix(deviceName, pxdDevicePrefix) {
		return "", fmt.Errorf("Invalid DeviceName for PWX: %v", deviceName)
	}
	return strings.TrimPrefix(deviceName, pxdDevicePrefix), nil
}

// Returns list of all paths for given PWX volume mount
func getDiskByIdPaths(partition string, devicePath string) []string {
	devicePaths := []string{}
	if devicePath != "" {
		devicePaths = append(devicePaths, devicePath)
	}

	return devicePaths
}

// Returns the first path that exists, or empty string if none exist.
func verifyDevicePath(devicePath string) (string, error) {
	if pathExists, err := pathExists(devicePath); err != nil {
		return "", fmt.Errorf("Error checking if path exists: %v", err)
	} else if pathExists {
		return devicePath, nil
	}
	
	return "", nil
}

// Checks if the specified path exists
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

// Return cloud provider
func getCloudProvider(cloudProvider cloudprovider.Interface) (*pwx.PWXCloud, error) {
	pwxCloudProvider, ok := cloudProvider.(*pwx.PWXCloud)
	if !ok || pwxCloudProvider == nil {
		return nil, fmt.Errorf("Failed to get PWX Cloud Provider. plugin.host.GetCloudProvider returned %v instead", cloudProvider)
	}

	return pwxCloudProvider, nil
}
