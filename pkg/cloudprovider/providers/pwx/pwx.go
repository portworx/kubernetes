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

import (
	"errors"
	"fmt"
	"strings"

	osd "github.com/libopenstorage/openstorage/api"
	osdclient "github.com/libopenstorage/openstorage/api/client"
	osdvolume "github.com/libopenstorage/openstorage/api/client/volume"
)

const (
	ProviderName = "portworx"
	localHostName = "http://0.0.0.0"
)


type PWXCloud struct {
	cfg *PWXConfig
}

type PWXConfig struct {
	Global struct {
		KVDatastore   []string `gcfg: "datastore"`
		DriverName    string   `gcfg:"drivername"`
		MgmtPort      string   `gcfg:"mgmtPort"`
		DataPort      string   `gcfg:"dataPort"`
		DriverVersion string   `gcfg:"driverVersion"`
	}
}

type VolumeOptions struct {
	SizeInBytes  uint64
	Format       string
	VolumeLabels map[string]string
	Shared       bool
	BlockSize    int64
	Name         string
	Parent       string
	Seed         string
}

func readConfig(config io.Reader) (PWXConfig, error) {
	if config == nil {
		err := fmt.Errorf("no pwxx cloud provider config file given")
		return PWXConfig{}, err
	}

	var cfg PWXConfig
	err := gcfg.ReadInto(&cfg, config)
	return cfg, err
}

func newPWXCloud(cfg PWXConfig) (*PWXCloud, error) {
	id, err := readInstanceID(&cfg)
	if err != nil {
		return nil, err
	}

	pc := PWXCloud{
		cfg: &cfg,
	}
	return &pc, nil
}

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, func(config io.Reader) (cloudprovider.Interface, error) {
		cfg, err := readConfig(config)
		if err != nil {
			return nil, err
		}
		return newPWXClou(cfg)
	})
}

func (pc *PWXCloud) newOsdClient(hostName string) (*osdvolume.VolumeDriver, error) {
	var clientUrl string
	if !strings.HasPrefix(hostName, "http://") {
		clientUrl = "http://" + hostName + ":" + pc.MgmtPort
	} else {
		clientUrl = hostName + ":" + pc.MgmtPort
	}

	client, err := osdclient.NewClient(clientUrl, pc.DriverVersion)
	if err != nil {
		return nil, err
	}

	return client.VolumeDriver(), nil
}

func (pc *PWXCloud) ProviderName() string {
	return ProviderName
}

// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
// TODO: Do we want to support this ?
func (pc *PWXCloud) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

// LoadBalancer returns an implementation of LoadBalancer..
func (pc *PWXCloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	return nil, false
}

// Zones returns an implementation of Zones.
func (pc *PWXCloud) Zones() (cloudprovider.Zones, bool) {
	return nil, false
}

// Routes returns an implementation of Routes for vSphere.
func (pc *PWXCloud) Routes() (cloudprovider.Routes, bool) {
	return nil, false
}

// ScrubDNS filters DNS settings for pods.
// TODO: We might need this
func (pc *PWXCloud) ScrubDNS(nameservers, searches []string) (nsOut, srchOut []string) {
	return nameservers, searches
}

func (pc *PWXCloud) AttachVolume(volumeID string, hostName string) (string, error) {
	client, err := newOsdClient(hostName)
	if err != nil {
		return "", err
	}

	devicePath, err := client.Attach(volumeID)
	if err != nil {
		glog.V(2).Infof("AttachVolume on %v failed with error %v", volumeID, err)
		return "", err
	}
	return devicePath, err
}

func (px *PWXCloud) DetachVolume(volumeID string, hostName string) (string, error) {
	client, err := newOsdClient(hostName)
	if err != nil {
		return "", err
	}

	err = client.Detach(volumeID)
	if err != nil {
		glog.V(2).Infof("DetachVolume on %v failed with error %v", volumeID, err)
		return "", err
	}
	devicePath := "/dev/pxd/pxd" + volumeID
	return devicePath, err
}

func (px *PWXCloud) MountVolume(volumeID string, mountPath string) error {
	client, err := newOsdClient(localHostName)
	if err != nil {
		return "", err
	}

	err = client.Mount(volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("MountVolume on %v failed with error %v", volumeID, err)
		return "", err
	}

	return err
}

func (px *PWXCloud) UnmountVolume(volumeID string, mountPath string) error {
	client, err := newOsdClient(localHostName)
	if err != nil {
		return "", err
	}

	err = client.Unmount(volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("MountVolume on %v failed with error %v", volumeID, err)
		return "", err
	}

	return err
}
