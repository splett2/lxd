package device

import (
	"fmt"
	"strings"
	"strconv"

	deviceConfig "github.com/lxc/lxd/lxd/device/config"
	"github.com/lxc/lxd/lxd/instance/instancetype"
	"github.com/lxc/lxd/shared"
	"github.com/farjump/go-libudev"
)

// unixHotplugIsOurDevice indicates whether the USB device event qualifies as part of our device.
// This function is not defined against the unixHotplug struct type so that it can be used in event
// callbacks without needing to keep a reference to the unixHotplug device struct.
func unixHotplugIsOurDevice(config deviceConfig.Device, unixHotplug *UnixHotplugEvent) bool {
	// Check if event matches criteria for this device, if not return.
	if (config["vendorid"] != "" && config["vendorid"] != unixHotplug.Vendor) || (config["productid"] != "" && config["productid"] != unixHotplug.Product) {
		return false
	}

	return true
}

type unixHotplug struct {
	deviceCommon
}

// isRequired indicates whether the device config requires this device to start OK.
func (d *unixHotplug) isRequired() bool {
	// Defaults to not required.
	if shared.IsTrue(d.config["required"]) {
		return true
	}

	return false
}

// validateConfig checks the supplied config for correctness.
func (d *unixHotplug) validateConfig() error {
	if d.instance.Type() != instancetype.Container {
		return ErrUnsupportedDevType
	}

	rules := map[string]func(string) error{
		"vendorid":  shared.IsDeviceID,
		"productid": shared.IsDeviceID,
		"uid":       unixValidUserID,
		"gid":       unixValidUserID,
		"mode":      unixValidOctalFileMode,
		"required":  shared.IsBool,
	}

	err := d.config.Validate(rules)
	if err != nil {
		return err
	}

	return nil
}

// Register is run after the device is started or when LXD starts.
func (d *unixHotplug) Register() error {
	// Extract variables needed to run the event hook so that the reference to this device
	// struct is not needed to be kept in memory.
	devicesPath := d.instance.DevicesPath()
	deviceConfig := d.config
	deviceName := d.name
	state := d.state

	// Handler for when a UnixHotplug event occurs.
	f := func(e UnixHotplugEvent) (*RunConfig, error) {
		if !unixHotplugIsOurDevice(deviceConfig, &e) {
			return nil, nil
		}

		runConf := RunConfig{}

		if e.Action == "add" {
			// TODO: what if the device is a block device?
			err := unixDeviceSetupCharNum(state, devicesPath, "unix", deviceName, deviceConfig, e.Major, e.Minor, e.Path, false, &runConf)
			if err != nil {
				return nil, err
			}
		} else if e.Action == "remove" {
			relativeTargetPath := strings.TrimPrefix(e.Path, "/")
			err := unixDeviceRemove(devicesPath, "unix", deviceName, relativeTargetPath, &runConf)
			if err != nil {
				return nil, err
			}

			// Add a post hook function to remove the specific USB device file after unmount.
			runConf.PostHooks = []func() error{func() error {
				err := unixDeviceDeleteFiles(state, devicesPath, "unix", deviceName, relativeTargetPath)
				if err != nil {
					return fmt.Errorf("Failed to delete files for device '%s': %v", deviceName, err)
				}

				return nil
			}}
		}

		runConf.Uevents = append(runConf.Uevents, e.UeventParts)

		return &runConf, nil
	}

	unixHotplugRegisterHandler(d.instance, d.name, f)

	return nil
}

// Start is run when the device is added to the instance
func (d *unixHotplug) Start() (*RunConfig, error) {
	runConf := RunConfig{}
	runConf.PostHooks = []func() error{d.Register}

	//find device if it exists
	u := udev.Udev{}
	e := u.NewEnumerate()

	if d.config["vendorid"] != "" {
		e.AddMatchProperty("ID_VENDOR_ID", d.config["vendorid"])
	}
	if d.config["productid"] != "" {
		e.AddMatchProperty("ID_MODEL_ID", d.config["productid"])
	}
	e.AddMatchIsInitialized()

	devices, _ := e.Devices()
	device := devices[0]
	if device != nil {
		fmt.Printf("found dev with\n vendorid: %s\n, productid: %s\n, subsystem: %s\n, devnode: %s\n, major: %s\n, minor: %s\n", d.config["vendorid"], d.config["productid"], device.Subsystem(), device.Devnode(), device.SysattrValue("MAJOR"), device.SysattrValue("MINOR"))
	}
	if d.isRequired() && device == nil {
		return nil, fmt.Errorf("Required Unix Hotplug device not found")
	}
	if device == nil {
		// TODO what is the action we're supposed to take in this case? 
		fmt.Printf("device not found with vendorid: %s, productid: %s\n", d.config["vendorid"], d.config["productid"])
		return &runConf, nil
	}
	if device.Subsystem() != "block" && device.Subsystem() != "char" {

		if d.isRequired(){
			return nil, fmt.Errorf("Required Unix Hotplug device not found, found device but has unsupported subsystem")
		} else {
			fmt.Printf("Device found has unsupported subsystem with vendorid: %s, productid: %s\n", d.config["vendorid"], d.config["productid"])
			return &runConf, nil
		}
	}

	i, err := strconv.ParseUint(device.SysattrValue("MAJOR"), 10, 32)
	if err != nil {
		return nil, err
	}
	major := uint32(i)
	i, err := strconv.ParseUint(device.SysattrValue("MINOR"), 10, 32)
	if err != nil {
		return nil, err
	}
	minor := uint32(i)

	// TODO now setup device
	// TODO figure out if Devnode is the path that we want or not
	if device.Subsystem() == "char" {
		err := unixDeviceSetupCharNum(d.state, d.instance.DevicesPath(), "unix", d.name, d.config, major, minor, device.Devnode(), false, &runConf)
	} else if device.Subsystem() == "block" {
		err := unixDeviceSetupBlockNum(d.state, d.instance.DevicesPath(), "unix", d.name, d.config, major, minor, device.Devnode(), false, &runConf)
	}
	
	if err != nil {
		return nil, err
	}

	fmt.Printf("finished Start() for hotplug device\n")

	return &runConf, nil
}

// Stop is run when the device is removed from the instance
func (d *unixHotplug) Stop() (*RunConfig, error) {
	unixHotplugUnregisterHandler(d.instance, d.name)

	runConf := RunConfig{
		PostHooks: []func() error{d.postStop},
	}

	err := unixDeviceRemove(d.instance.DevicesPath(), "unix", d.name, "", &runConf)
	if err != nil {
		return nil, err
	}

	return &runConf, nil
}

// postStop is run after the device is removed from the instance
func (d *unixHotplug) postStop() error {
	err := unixDeviceDeleteFiles(d.state, d.instance.DevicesPath(), "unix", d.name, "")
	if err != nil {
		return fmt.Errorf("Failed to delete files for device '%s': %v", d.name, err)
	}

	return nil
}
