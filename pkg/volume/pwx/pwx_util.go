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
	"path/filepath"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/pwx"
	"k8s.io/kubernetes/pkg/util/keymutex"
	"k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/pkg/volume"
)

type PWXDiskUtil struct{}

func (util *PWXDiskUtil) DeleteVolume(d *pwxVolumeDeleter) error {
	cloud, err := getCloudProvider(d.pwxVolume.plugin)
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
	}
	return nil
}

// CreateVolume creates an AWS EBS volume.
// Returns: volumeID, volumeSize, labels, error
func (util *PWXDiskUtil) CreateVolume(p *pwxVolumeProvisioner) (string, int, map[string]string, error) {
	cloud, err := getCloudProvider(p.pwxVolume.plugin)
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

	labels, err := cloud.GetVolumeLabels(name)
	if err != nil {
		// We don't really want to leak the volume here...
		glog.Errorf("error building labels for new PWXx volume %q: %v", name, err)
	}

	return name, requestBytes, labels, nil
}

// Return cloud provider
func getCloudProvider(plugin *pwxVolumePlugin) (*pwx.PWXCloud, error) {
	if plugin == nil {
		return nil, fmt.Errorf("Failed to get PWX Cloud Provider. plugin object is nil.")
	}
	if plugin.host == nil {
		return nil, fmt.Errorf("Failed to get PWX Cloud Provider. plugin.host object is nil.")
	}

	cloudProvider := plugin.host.GetCloudProvider()
	pwxCloudProvider, ok := cloudProvider.(*pwx.PWXCloud)
	if !ok || awsCloudProvider == nil {
		return nil, fmt.Errorf("Failed to get PWX Cloud Provider. plugin.host.GetCloudProvider returned %v instead", cloudProvider)
	}

	return pwxCloudProvider, nil
}
