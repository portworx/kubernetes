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

package portworx

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	kstrings "k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
)

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&portworxVolumePlugin{nil}}
}

type portworxVolumePlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &portworxVolumePlugin{}
var _ volume.PersistentVolumePlugin = &portworxVolumePlugin{}
var _ volume.DeletableVolumePlugin = &portworxVolumePlugin{}
var _ volume.ProvisionableVolumePlugin = &portworxVolumePlugin{}

const (
	portworxVolumePluginName = "kubernetes.io/portworx-volume"
)

func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, kstrings.EscapeQualifiedNameForDisk(portworxVolumePluginName), volName)
}

func (plugin *portworxVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *portworxVolumePlugin) GetPluginName() string {
	return portworxVolumePluginName
}

func (plugin *portworxVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.VolumeID, nil
}

func (plugin *portworxVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.PortworxVolume != nil) ||
		(spec.Volume != nil && spec.Volume.PortworxVolume != nil)
}

func (plugin *portworxVolumePlugin) RequiresRemount() bool {
	return false
}

func (plugin *portworxVolumePlugin) GetAccessModes() []api.PersistentVolumeAccessMode {
	return []api.PersistentVolumeAccessMode{
		api.ReadWriteOnce,
	}
}

func (plugin *portworxVolumePlugin) NewMounter(spec *volume.Spec, pod *api.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod.UID, &PortworxDiskUtil{}, plugin.host.GetMounter())
}

func (plugin *portworxVolumePlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, manager portworxManager, mounter mount.Interface) (volume.Mounter, error) {
	pwx, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	volumeID := pwx.VolumeID
	fsType := pwx.FSType

	return &portworxVolumeMounter{
		portworxVolume: &portworxVolume{
			podUID:          podUID,
			volName:         spec.Name(),
			volumeID:        volumeID,
			manager:         manager,
			mounter:         mounter,
			plugin:          plugin,
			MetricsProvider: volume.NewMetricsStatFS(getPath(podUID, spec.Name(), plugin.host)),
		},
		fsType:      fsType,
		readOnly:    readOnly,
		diskMounter: &mount.SafeFormatAndMount{Interface: plugin.host.GetMounter(), Runner: exec.New()}}, nil
}

func (plugin *portworxVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, &PortworxDiskUtil{}, plugin.host.GetMounter())
}

func (plugin *portworxVolumePlugin) newUnmounterInternal(volName string, podUID types.UID, manager portworxManager, mounter mount.Interface) (volume.Unmounter, error) {
	return &portworxVolumeUnmounter{
		&portworxVolume{
			podUID:          podUID,
			volName:         volName,
			manager:         manager,
			mounter:         mounter,
			plugin:          plugin,
			MetricsProvider: volume.NewMetricsStatFS(getPath(podUID, volName, plugin.host)),
		}}, nil
}

func (plugin *portworxVolumePlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return plugin.newDeleterInternal(spec, &PortworxDiskUtil{})
}

func (plugin *portworxVolumePlugin) newDeleterInternal(spec *volume.Spec, manager portworxManager) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.PortworxVolume == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.PortworxVolume is nil")
	}
	return &portworxVolumeDeleter{
		portworxVolume: &portworxVolume{
			volName:  spec.Name(),
			volumeID: spec.PersistentVolume.Spec.PortworxVolume.VolumeID,
			manager:  manager,
			plugin:   plugin,
		}}, nil
}

func (plugin *portworxVolumePlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return plugin.newProvisionerInternal(options, &PortworxDiskUtil{})
}

func (plugin *portworxVolumePlugin) newProvisionerInternal(options volume.VolumeOptions, manager portworxManager) (volume.Provisioner, error) {
	return &portworxVolumeProvisioner{
		portworxVolume: &portworxVolume{
			manager: manager,
			plugin:  plugin,
		},
		options: options,
	}, nil
}

func (plugin *portworxVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	portworxVolume := &api.Volume{
		Name: volumeName,
		VolumeSource: api.VolumeSource{
			PortworxVolume: &api.PortworxVolumeSource{
				VolumeID: volumeName,
			},
		},
	}
	return volume.NewSpecFromVolume(portworxVolume), nil
}


func getVolumeSource(
	spec *volume.Spec) (*api.PortworxVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.PortworxVolume != nil {
		return spec.Volume.PortworxVolume, spec.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.PortworxVolume != nil {
		return spec.PersistentVolume.Spec.PortworxVolume, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a PWX volume type")
}

// Abstract interface to PD operations.
type portworxManager interface {
	// Creates a volume
	CreateVolume(provisioner *portworxVolumeProvisioner) (volumeID string, volumeSizeGB int, labels map[string]string, err error)
	// Deletes a volume
	DeleteVolume(deleter *portworxVolumeDeleter) error
	// Attach a volume
	AttachVolume(mounter *portworxVolumeMounter) (string, error)
	// Detach a volume
	DetachVolume(unmounter *portworxVolumeUnmounter, deviceName string) error
	// Mount a volume
	MountVolume(mounter *portworxVolumeMounter, mountDir string) error
	// Unmount a volume
	UnmountVolume(unmounter *portworxVolumeUnmounter, deviceName, mountDir string) error
}

// portworxVolume volumes are pwx block devices
// that are attached to the kubelet's host machine and exposed to the pod.
type portworxVolume struct {
	volName string
	podUID  types.UID
	// Unique id of the PD, used to find the disk resource in the provider.
	volumeID string
	// Utility interface that provides API calls to the provider to attach/detach disks.
	manager portworxManager
	// Mounter interface that provides system calls to mount the global path to the pod local path.
	mounter mount.Interface
	plugin  *portworxVolumePlugin
	volume.MetricsProvider
}

type portworxVolumeMounter struct {
	*portworxVolume
	// Filesystem type, optional.
	fsType string
	// Specifies whether the disk will be attached as read-only.
	readOnly bool
	// diskMounter provides the interface that is used to mount the actual block device.
	diskMounter *mount.SafeFormatAndMount
}

var _ volume.Mounter = &portworxVolumeMounter{}

func (b *portworxVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly: b.readOnly,
		Managed:  !b.readOnly,
		// true ?
		SupportsSELinux: true,
	}
}

// SetUp attaches the disk and bind mounts to the volume path.
func (b *portworxVolumeMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

// SetUpAt attaches the disk and bind mounts to the volume path.
func (b *portworxVolumeMounter) SetUpAt(dir string, fsGroup *int64) error {


	notMnt, err := b.mounter.IsLikelyNotMountPoint(dir)
	glog.V(4).Infof("PWX volume set up: %s %v %v", dir, !notMnt, err)
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("Cannot validate mountpoint: %s", dir)
		return err
	}
	if !notMnt {
		return nil
	}

	//globalPDPath := makeGlobalPDPath(b.plugin.host, b.volumeID)
	if _, err := b.manager.AttachVolume(b); err != nil {
		return err
	}

	glog.V(3).Infof("PWX volume %s attached", b.volumeID)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	// Perform a bind mount to the full path to allow duplicate mounts of the same PD.
	if err := b.manager.MountVolume(b, dir); err != nil {
		return err
	}

	return nil
}

func makeGlobalPDPath(host volume.VolumeHost, volumeID string) string {
	// Clean up the URI to be more fs-friendly
	name := volumeID
	name = strings.Replace(name, "://", "/", -1)
	return path.Join(host.GetPluginDir(portworxVolumePluginName), "mounts", name)
}

// Reverses the mapping done in makeGlobalPDPath
func getVolumeIDFromGlobalMount(host volume.VolumeHost, globalPath string) (string, error) {
	basePath := path.Join(host.GetPluginDir(portworxVolumePluginName), "mounts")
	rel, err := filepath.Rel(basePath, globalPath)
	if err != nil {
		return "", err
	}
	if strings.Contains(rel, "../") {
		return "", fmt.Errorf("Unexpected mount path: " + globalPath)
	}
	// Reverse the :// replacement done in makeGlobalPDPath
	volumeID := rel
	if strings.HasPrefix(volumeID, "pwx/") {
		volumeID = strings.Replace(volumeID, "pwx/", "pwx://", 1)
	}
	glog.V(2).Info("Mapping mount dir ", globalPath, " to volumeID ", volumeID)
	return volumeID, nil
}

func (pwx *portworxVolume) GetPath() string {
	return getPath(pwx.podUID, pwx.volName, pwx.plugin.host)
}

type portworxVolumeUnmounter struct {
	*portworxVolume
}

var _ volume.Unmounter = &portworxVolumeUnmounter{}

// Unmounts the bind mount, and detaches the disk only if the PD
// resource was the last reference to that disk on the kubelet.
func (c *portworxVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// Unmounts the bind mount, and detaches the disk only if the PD
// resource was the last reference to that disk on the kubelet.
func (c *portworxVolumeUnmounter) TearDownAt(dir string) error {
	notMnt, err := c.mounter.IsLikelyNotMountPoint(dir)
	if err != nil {
		glog.V(2).Info("Error checking if mountpoint ", dir, ": ", err)
		return err
	}
	if notMnt {
		glog.V(2).Info("Not mountpoint, deleting")
		return os.Remove(dir)
	}

	deviceName, _, err := mount.GetDeviceNameFromMount(c.mounter, dir)
	if err != nil {
		glog.Errorf("Failed to get reference count and device name for volume: %s", dir)
		return err
	}
	
	// Unmount the bind-mount inside this pod
	if err := c.manager.UnmountVolume(c, deviceName, dir); err != nil {
		return err
	}

	if err := c.manager.DetachVolume(c, deviceName); err != nil {
		return err
	}
	return nil
}

type portworxVolumeDeleter struct {
	*portworxVolume
}

var _ volume.Deleter = &portworxVolumeDeleter{}

func (d *portworxVolumeDeleter) GetPath() string {
	return getPath(d.podUID, d.volName, d.plugin.host)
}

func (d *portworxVolumeDeleter) Delete() error {
	return d.manager.DeleteVolume(d)
}

type portworxVolumeProvisioner struct {
	*portworxVolume
	options   volume.VolumeOptions
	namespace string
}

var _ volume.Provisioner = &portworxVolumeProvisioner{}

func (c *portworxVolumeProvisioner) Provision() (*api.PersistentVolume, error) {
	glog.Infof("In pwx Provision()")
	volumeID, sizeGB, labels, err := c.manager.CreateVolume(c)
	if err != nil {
		return nil, err
	}

	glog.Infof("Labels : %v", labels)
	pv := &api.PersistentVolume{
		ObjectMeta: api.ObjectMeta{
			Name:   c.options.PVName,
			Labels: map[string]string{},
			Annotations: map[string]string{
				"kubernetes.io/createdby": "portworx-volume-dynamic-provisioner",
			},
		},
		Spec: api.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: c.options.PersistentVolumeReclaimPolicy,
			AccessModes:                   c.options.PVC.Spec.AccessModes,
			Capacity: api.ResourceList{
				api.ResourceName(api.ResourceStorage): resource.MustParse(fmt.Sprintf("%dGi", sizeGB)),
			},
			PersistentVolumeSource: api.PersistentVolumeSource{
				PortworxVolume: &api.PortworxVolumeSource{
					VolumeID:  volumeID,
				},
			},
		},
	}

	if len(labels) != 0 {
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		for k, v := range labels {
			pv.Labels[k] = v
		}
	}

	if len(c.options.PVC.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = c.plugin.GetAccessModes()
	}

	return pv, nil
}
