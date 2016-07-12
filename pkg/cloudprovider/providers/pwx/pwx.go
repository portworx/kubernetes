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
	"fmt"
	"io"
	"strings"
	"errors"

	"github.com/golang/glog"
	"gopkg.in/gcfg.v1"

	osdclient "github.com/libopenstorage/openstorage/api/client"
	osdvolume "github.com/libopenstorage/openstorage/volume"
	"k8s.io/kubernetes/pkg/cloudprovider"
	"k8s.io/kubernetes/pkg/api"
)

const (
	ProviderName  = "portworx"
	localHostName = "http://0.0.0.0"
	defaultPxdPath = "/dev/pxd/pxd"
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
		err := fmt.Errorf("no pwx cloud provider config file given")
		return PWXConfig{}, err
	}

	var cfg PWXConfig
	err := gcfg.ReadInto(&cfg, config)
	return cfg, err
}

func newPWXCloud(cfg PWXConfig) (*PWXCloud, error) {
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
		return newPWXCloud(cfg)
	})
}

func (px *PWXCloud) newOsdClient(hostName string) (osdvolume.VolumeDriver, error) {
	var clientUrl string
	if !strings.HasPrefix(hostName, "http://") {
		clientUrl = "http://" + hostName + ":" + px.cfg.Global.MgmtPort
	} else {
		clientUrl = hostName + ":" + px.cfg.Global.MgmtPort
	}

	client, err := osdclient.NewClient(clientUrl, px.cfg.Global.DriverVersion)
	if err != nil {
		return nil, err
	}

	return client.VolumeDriver(), nil
}

func (px *PWXCloud) ProviderName() string {
	return ProviderName
}

type Instances struct {
	cfg *PWXConfig
}

// Instances returns an implementation of Instances for vSphere.
func (px *PWXCloud) Instances() (cloudprovider.Instances, bool) {
	return &Instances{px.cfg}, true
}

// List is an implementation of Instances.List.
func (i *Instances) List(filter string) ([]string, error) {
	return nil, nil
}

// NodeAddresses is an implementation of Instances.NodeAddresses.
func (i *Instances) NodeAddresses(name string) ([]api.NodeAddress, error) {
	return nil, nil
}

func (i *Instances) AddSSHKeyToAllInstances(user string, keyData []byte) error {
	return errors.New("unimplemented")
}

func (i *Instances) CurrentNodeName(hostname string) (string, error) {
	return "", nil
}

// ExternalID returns the cloud provider ID of the specified instance (deprecated).
func (i *Instances) ExternalID(name string) (string, error) {
	return "", nil
}

// InstanceID returns the cloud provider ID of the specified instance.
func (i *Instances) InstanceID(name string) (string, error) {
	return "", nil
}

func (i *Instances) InstanceType(name string) (string, error) {
	return "", nil
}

// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
// TODO: Do we want to support this ?
func (px *PWXCloud) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

// LoadBalancer returns an implementation of LoadBalancer..
func (px *PWXCloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	return nil, false
}

// Zones returns an implementation of Zones.
func (px *PWXCloud) Zones() (cloudprovider.Zones, bool) {
	return nil, false
}

// Routes returns an implementation of Routes for PWX.
func (px *PWXCloud) Routes() (cloudprovider.Routes, bool) {
	return nil, false
}

// ScrubDNS filters DNS settings for pods.
// TODO: We might need this
func (px *PWXCloud) ScrubDNS(nameservers, searches []string) (nsOut, srchOut []string) {
	return nameservers, searches
}

func (px *PWXCloud) GetVolumePath(volumeID string) (string, error) {
	return defaultPxdPath+volumeID, nil
}

func (px *PWXCloud) AttachVolume(volumeID string, hostName string) (string, error) {
	client, err := px.newOsdClient(hostName)
	if err != nil {
		return "", err
	}

	devicePath, err := client.Attach(volumeID)
	if err != nil {
		if err == osdvolume.ErrVolAttachedOnRemoteNode {
			// Volume is already attached to node.
			glog.Infof("Attach operation is unsuccessful. Volume %q is already attached to another node.", volumeID)
			return "", err
		}
		glog.V(2).Infof("AttachVolume on %v failed with error %v", volumeID, err)
		return "", err
	}
	return devicePath, err
}

func (px *PWXCloud) DetachVolume(volumeID string, hostName string) (string, error) {
	client, err := px.newOsdClient(hostName)
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
	client, err := px.newOsdClient(localHostName)
	if err != nil {
		return err
	}

	err = client.Mount(volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("MountVolume on %v failed with error %v", volumeID, err)
		return err
	}

	return err
}

func (px *PWXCloud) UnmountVolume(volumeID string, mountPath string) error {
	client, err := px.newOsdClient(localHostName)
	if err != nil {
		return err
	}

	err = client.Unmount(volumeID, mountPath)
	if err != nil {
		glog.V(2).Infof("MountVolume on %v failed with error %v", volumeID, err)
		return err
	}

	return err
}
