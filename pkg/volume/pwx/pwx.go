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
	return []volume.VolumePlugin{&pwxVolumePlugin{nil}}
}

type pwxVolumePlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &pwxVolumePlugin{}
var _ volume.PersistentVolumePlugin = &pwxVolumePlugin{}
var _ volume.DeletableVolumePlugin = &pwxVolumePlugin{}
var _ volume.ProvisionableVolumePlugin = &pwxVolumePlugin{}

const (
	pwxVolumePluginName = "kubernetes.io/pwx-volume"
)

func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, kstrings.EscapeQualifiedNameForDisk(pwxVolumePluginName), volName)
}

func (plugin *pwxVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *pwxVolumePlugin) GetPluginName() string {
	return pwxVolumePluginName
}

func (plugin *pwxVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.VolumeID, nil
}

func (plugin *pwxVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.PWXVolume != nil) ||
		(spec.Volume != nil && spec.Volume.PWXVolume != nil)
}

func (plugin *pwxVolumePlugin) RequiresRemount() bool {
	return false
}

func (plugin *pwxVolumePlugin) GetAccessModes() []api.PersistentVolumeAccessMode {
	return []api.PersistentVolumeAccessMode{
		api.ReadWriteOnce,
		// pwx : Are we sure we want this ?
		api.ReadWriteMany,
	}
}

func (plugin *pwxVolumePlugin) NewMounter(spec *volume.Spec, pod *api.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod.UID, &PWXDiskUtil{}, plugin.host.GetMounter())
}

func (plugin *pwxVolumePlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, manager pwxManager, mounter mount.Interface) (volume.Mounter, error) {
	pwx, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	volumeID := pwx.VolumeID
	fsType := pwx.FSType

	return &pwxVolumeMounter{
		pwxVolume: &pwxVolume{
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

func (plugin *pwxVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, &PWXDiskUtil{}, plugin.host.GetMounter())
}

func (plugin *pwxVolumePlugin) newUnmounterInternal(volName string, podUID types.UID, manager pwxManager, mounter mount.Interface) (volume.Unmounter, error) {
	return &pwxVolumeUnmounter{
		&pwxVolume{
			podUID:          podUID,
			volName:         volName,
			manager:         manager,
			mounter:         mounter,
			plugin:          plugin,
			MetricsProvider: volume.NewMetricsStatFS(getPath(podUID, volName, plugin.host)),
		}}, nil
}

func (plugin *pwxVolumePlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return plugin.newDeleterInternal(spec, &PWXDiskUtil{})
}

func (plugin *pwxVolumePlugin) newDeleterInternal(spec *volume.Spec, manager pwxManager) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.PWXVolume == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.PWXVolume is nil")
	}
	return &pwxVolumeDeleter{
		pwxVolume: &pwxVolume{
			volName:  spec.Name(),
			volumeID: spec.PersistentVolume.Spec.PWXVolume.VolumeID,
			manager:  manager,
			plugin:   plugin,
		}}, nil
}

func (plugin *pwxVolumePlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	if len(options.AccessModes) == 0 {
		options.AccessModes = plugin.GetAccessModes()
	}
	return plugin.newProvisionerInternal(options, &PWXDiskUtil{})
}

func (plugin *pwxVolumePlugin) newProvisionerInternal(options volume.VolumeOptions, manager pwxManager) (volume.Provisioner, error) {
	return &pwxVolumeProvisioner{
		pwxVolume: &pwxVolume{
			manager: manager,
			plugin:  plugin,
		},
		options: options,
	}, nil
}

func getVolumeSource(
	spec *volume.Spec) (*api.PWXVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.PWXVolume != nil {
		return spec.Volume.PWXVolume, spec.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.PWXVolume != nil {
		return spec.PersistentVolume.Spec.PWXVolume, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a PWX volume type")
}

// Abstract interface to PD operations.
type pwxManager interface {
	// Creates a volume
	CreateVolume(provisioner *pwxVolumeProvisioner) (volumeID string, volumeSizeGB int, labels map[string]string, err error)
	// Deletes a volume
	DeleteVolume(deleter *pwxVolumeDeleter) error
	// Attach a volume
	AttachVolume(mounter *pwxVolumeMounter) (string, error)
	// Detach a volume
	DetachVolume(unmounter *pwxVolumeUnmounter, deviceName string) error
	// Mount a volume
	MountVolume(mounter *pwxVolumeMounter, mountDir string) error
	// Unmount a volume
	UnmountVolume(unmounter *pwxVolumeUnmounter, deviceName, mountDir string) error
}

// pwxVolume volumes are pwx block devices
// that are attached to the kubelet's host machine and exposed to the pod.
type pwxVolume struct {
	volName string
	podUID  types.UID
	// Unique id of the PD, used to find the disk resource in the provider.
	volumeID string
	// Utility interface that provides API calls to the provider to attach/detach disks.
	manager pwxManager
	// Mounter interface that provides system calls to mount the global path to the pod local path.
	mounter mount.Interface
	plugin  *pwxVolumePlugin
	volume.MetricsProvider
}

type pwxVolumeMounter struct {
	*pwxVolume
	// Filesystem type, optional.
	fsType string
	// Specifies whether the disk will be attached as read-only.
	readOnly bool
	// diskMounter provides the interface that is used to mount the actual block device.
	diskMounter *mount.SafeFormatAndMount
}

var _ volume.Mounter = &pwxVolumeMounter{}

func (b *pwxVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly: b.readOnly,
		Managed:  !b.readOnly,
		// true ?
		SupportsSELinux: true,
	}
}

// SetUp attaches the disk and bind mounts to the volume path.
func (b *pwxVolumeMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

// SetUpAt attaches the disk and bind mounts to the volume path.
func (b *pwxVolumeMounter) SetUpAt(dir string, fsGroup *int64) error {


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

	/*err = b.mounter.Mount(globalPDPath, dir, "", options)
	if err != nil {
		notMnt, mntErr := b.mounter.IsLikelyNotMountPoint(dir)
		if mntErr != nil {
			glog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
			return err
		}
		if !notMnt {
			if mntErr = b.mounter.Unmount(dir); mntErr != nil {
				glog.Errorf("Failed to unmount: %v", mntErr)
				return err
			}
			notMnt, mntErr := b.mounter.IsLikelyNotMountPoint(dir)
			if mntErr != nil {
				glog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
				return err
			}
			if !notMnt {
				// This is very odd, we don't expect it.  We'll try again next sync loop.
				glog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", dir)
				return err
			}
		}
		os.Remove(dir)
		return err
	}

	if !b.readOnly {
		volume.SetVolumeOwnership(b, fsGroup)
	}*/

	return nil
}

func makeGlobalPDPath(host volume.VolumeHost, volumeID string) string {
	// Clean up the URI to be more fs-friendly
	name := volumeID
	name = strings.Replace(name, "://", "/", -1)
	return path.Join(host.GetPluginDir(pwxVolumePluginName), "mounts", name)
}

// Reverses the mapping done in makeGlobalPDPath
func getVolumeIDFromGlobalMount(host volume.VolumeHost, globalPath string) (string, error) {
	basePath := path.Join(host.GetPluginDir(pwxVolumePluginName), "mounts")
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

func (pwx *pwxVolume) GetPath() string {
	return getPath(pwx.podUID, pwx.volName, pwx.plugin.host)
}

type pwxVolumeUnmounter struct {
	*pwxVolume
}

var _ volume.Unmounter = &pwxVolumeUnmounter{}

// Unmounts the bind mount, and detaches the disk only if the PD
// resource was the last reference to that disk on the kubelet.
func (c *pwxVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// Unmounts the bind mount, and detaches the disk only if the PD
// resource was the last reference to that disk on the kubelet.
func (c *pwxVolumeUnmounter) TearDownAt(dir string) error {
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
	/*notMnt, mntErr := c.mounter.IsLikelyNotMountPoint(dir)
	if mntErr != nil {
		glog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
		return err
	}
	if notMnt {
		if err := os.Remove(dir); err != nil {
			glog.V(2).Info("Error removing mountpoint ", dir, ": ", err)
			return err
		}
	}*/
	return nil
}

type pwxVolumeDeleter struct {
	*pwxVolume
}

var _ volume.Deleter = &pwxVolumeDeleter{}

func (d *pwxVolumeDeleter) GetPath() string {
	return getPath(d.podUID, d.volName, d.plugin.host)
}

func (d *pwxVolumeDeleter) Delete() error {
	return d.manager.DeleteVolume(d)
}

type pwxVolumeProvisioner struct {
	*pwxVolume
	options   volume.VolumeOptions
	namespace string
}

var _ volume.Provisioner = &pwxVolumeProvisioner{}

func (c *pwxVolumeProvisioner) Provision() (*api.PersistentVolume, error) {
	glog.Infof("In pwx Provision()")
	volumeID, sizeGB, labels, err := c.manager.CreateVolume(c)
	if err != nil {
		return nil, err
	}

	pv := &api.PersistentVolume{
		ObjectMeta: api.ObjectMeta{
			Name:   c.options.PVName,
			Labels: map[string]string{},
			Annotations: map[string]string{
				"kubernetes.io/createdby": "pwx-volume-dynamic-provisioner",
			},
		},
		Spec: api.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: c.options.PersistentVolumeReclaimPolicy,
			AccessModes:                   c.options.AccessModes,
			Capacity: api.ResourceList{
				api.ResourceName(api.ResourceStorage): resource.MustParse(fmt.Sprintf("%dGi", sizeGB)),
			},
			PersistentVolumeSource: api.PersistentVolumeSource{
				PWXVolume: &api.PWXVolumeSource{
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

	return pv, nil
}
