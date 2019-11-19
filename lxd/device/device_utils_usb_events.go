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
	"github.com/farjump/go-libudev"
)

// USBEvent represents the properties of a USB device uevent.
type USBEvent struct {
	Action string

	Vendor  string
	Product string

	Path        string
	Major       uint32
	Minor       uint32
	UeventParts []string
	UeventLen   int
}

// usbHandlers stores the event handler callbacks for USB events.
var usbHandlers = map[string]func(USBEvent) (*RunConfig, error){}

// usbMutex controls access to the usbHandlers map.
var usbMutex sync.Mutex

// usbRegisterHandler registers a handler function to be called whenever a USB device event occurs.
func usbRegisterHandler(instance Instance, deviceName string, handler func(USBEvent) (*RunConfig, error)) {
	usbMutex.Lock()
	defer usbMutex.Unlock()

	// Null delimited string of project name, instance name and device name.
	key := fmt.Sprintf("%s\000%s\000%s", instance.Project(), instance.Name(), deviceName)
	usbHandlers[key] = handler
}

// usbUnregisterHandler removes a registered USB handler function for a device.
func usbUnregisterHandler(instance Instance, deviceName string) {
	usbMutex.Lock()
	defer usbMutex.Unlock()

	// Null delimited string of project name, instance name and device name.
	key := fmt.Sprintf("%s\000%s\000%s", instance.Project(), instance.Name(), deviceName)
	delete(usbHandlers, key)
}

// USBRunHandlers executes any handlers registered for USB events.
func USBRunHandlers(state *state.State, event *USBEvent) {
	usbMutex.Lock()
	defer usbMutex.Unlock()

	fmt.Printf("running in USBHandler\n")
	// Create Udev and Enumerate
	u := Udev{}
	e := u.NewEnumerate()

	// Add some FilterAddMatchSubsystemDevtype
	e.AddMatchSubsystem("block")
	e.AddMatchIsInitialized()
	devices, _ := e.Devices()
	for i := range devices {
	    device := devices[i]
	    fmt.Println(device.Syspath())
	}

	u2 := Udev{}
	e2 := u2.NewEnumerate()

	// Add some FilterAddMatchSubsystemDevtype
	e2.AddMatchSubsystem("char")
	e2.AddMatchIsInitialized()
	devices2, _ := e2.Devices()
	for i := range devices2 {
	    device := devices2[i]
	    fmt.Println(device2.Syspath())
	}


	for key, hook := range usbHandlers {
		keyParts := strings.SplitN(key, "\000", 3)
		projectName := keyParts[0]
		instanceName := keyParts[1]
		deviceName := keyParts[2]

		if hook == nil {
			delete(usbHandlers, key)
			continue
		}

		runConf, err := hook(*event)
		if err != nil {
			logger.Error("USB event hook failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
			continue
		}

		// If runConf supplied, load instance and call its USB event handler function so
		// any instance specific device actions can occur.
		if runConf != nil {
			instance, err := InstanceLoadByProjectAndName(state, projectName, instanceName)
			if err != nil {
				logger.Error("USB event loading instance failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
				continue
			}

			err = instance.DeviceEventHandler(runConf)
			if err != nil {
				logger.Error("USB event instance handler failed", log.Ctx{"err": err, "project": projectName, "instance": instanceName, "device": deviceName})
				continue
			}
		}
	}
}

// USBNewEvent instantiates a new USBEvent struct.
func USBNewEvent(action string, vendor string, product string, major string, minor string, busnum string, devnum string, devname string, ueventParts []string, ueventLen int) (USBEvent, error) {
	majorInt, err := strconv.ParseUint(major, 10, 32)
	if err != nil {
		return USBEvent{}, err
	}

	minorInt, err := strconv.ParseUint(minor, 10, 32)
	if err != nil {
		return USBEvent{}, err
	}

	path := devname
	if devname == "" {
		busnumInt, err := strconv.Atoi(busnum)
		if err != nil {
			return USBEvent{}, err
		}

		devnumInt, err := strconv.Atoi(devnum)
		if err != nil {
			return USBEvent{}, err
		}
		path = fmt.Sprintf("/dev/bus/usb/%03d/%03d", busnumInt, devnumInt)
	} else {
		if !filepath.IsAbs(devname) {
			path = fmt.Sprintf("/dev/%s", devname)
		}
	}

	return USBEvent{
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
