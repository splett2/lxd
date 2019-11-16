package device

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/lxc/lxd/lxd/state"
	log "github.com/lxc/lxd/shared/log15"
	"github.com/lxc/lxd/shared/logger"
)

// UnixHotplugEvent represents the properties of a Unix hotplug device uevent.
type UnixHotplugEvent struct {
	Action string

	Vendor  string
	Product string

	Path        string   //TODO make sure this is correct
	Major       uint32   //TODO do we need this?
	Minor       uint32   //TODO do we need this?
	UeventParts []string //TODO do we need this?
	UeventLen   int		 //TODO do we need this?
	//TODO subsystem?
}

// unixHotplugHandlers stores the event handler callbacks for Unix hotplug events.
var unixHotplugHandlers = map[string]func(UnixHotplugEvent) (*RunConfig, error){}

// unixHotplugMutex controls access to the unixHotplugHandlers map.
var unixHotplugMutex sync.Mutex

// unixHotplugRegisterHandler registers a handler function to be called whenever a Unix hotplug device event occurs.
func unixHotplugRegisterHandler(instance Instance, deviceName string, handler func(UnixHotplugEvent) (*RunConfig, error)) {
	unixHotplugMutex.Lock()
	defer unixHotplugMutex.Unlock()

	// Null delimited string of project name, instance name and device name.
	key := fmt.Sprintf("%s\000%s\000%s", instance.Project(), instance.Name(), deviceName)
	unixHotplugHandlers[key] = handler
}

// unixHotplugUnregisterHandler removes a registered Unix hotplug handler function for a device.
func unixHotplugUnregisterHandler(instance Instance, deviceName string) {
	unixHotplugMutex.Lock()
	defer unixHotplugMutex.Unlock()

	// Null delimited string of project name, instance name and device name.
	key := fmt.Sprintf("%s\000%s\000%s", instance.Project(), instance.Name(), deviceName)
	delete(unixHotplugHandlers, key)
}

// unixHotplugRunHandlers executes any handlers registered for Unix hotplug events.
func unixHotplugRunHandlers(state *state.State, event *UnixHotplugEvent) {
	unixHotplugMutex.Lock()
	defer unixHotplugMutex.Unlock()

	for key, hook := range unixHotplugHandlers {
		keyParts := strings.SplitN(key, "\000", 3)
		projectName := keyParts[0]
		instanceName := keyParts[1]
		deviceName := keyParts[2]

		if hook == nil {
			delete(unixHotplugHandlers, key)
			continue
		}

		runConf, err := hook(*event)
		if err != nil {
			logger.Error("Unix hotplug event hook failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
			continue
		}

		// If runConf supplied, load instance and call its Unix hotplug event handler function so
		// any instance specific device actions can occur.
		if runConf != nil {
			instance, err := InstanceLoadByProjectAndName(state, projectName, instanceName)
			if err != nil {
				logger.Error("Unix hotplug event loading instance failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
				continue
			}

			err = instance.DeviceEventHandler(runConf)
			if err != nil {
				logger.Error("Unix hotplug event instance handler failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
				continue
			}
		}
	}
}

// unixHotplugNewEvent instantiates a new UnixHotplugEvent struct.
func unixHotplugNewEvent(action string, vendor string, product string, major string, minor string, busnum string, devnum string, devname string, ueventParts []string, ueventLen int) (UnixHotplugEvent, error) {
	majorInt, err := strconv.ParseUint(major, 10, 32)
	if err != nil {
		return UnixHotplugEvent{}, err
	}

	minorInt, err := strconv.ParseUint(minor, 10, 32)
	if err != nil {
		return UnixHotplugEvent{}, err
	}

	path := devname
	if devname == "" {
		busnumInt, err := strconv.Atoi(busnum)
		if err != nil {
			return UnixHotplugEvent{}, err
		}

		devnumInt, err := strconv.Atoi(devnum)
		if err != nil {
			return UnixHotplugEvent{}, err
		}
		path = fmt.Sprintf("/dev/bus/usb/%03d/%03d", busnumInt, devnumInt) //TODO how do we get the correct path?
	} else {
		if !filepath.IsAbs(devname) {
			path = fmt.Sprintf("/dev/%s", devname)
		}
	}

	return UnixHotplugEvent{
		action,
		vendor,
		product,
		path,
		uint32(majorInt),
		uint32(minorInt),
		ueventParts,
		ueventLen,
	}, nil
}
