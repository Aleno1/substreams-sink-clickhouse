// Code generated by go-enum DO NOT EDIT.
// Version:
// Revision:
// Build Date:
// Built By:

package db

import (
	"fmt"
	"strings"
)

const (
	// OnModuleHashMismatchIgnore is a OnModuleHashMismatch of type Ignore.
	OnModuleHashMismatchIgnore OnModuleHashMismatch = iota
	// OnModuleHashMismatchWarn is a OnModuleHashMismatch of type Warn.
	OnModuleHashMismatchWarn
	// OnModuleHashMismatchError is a OnModuleHashMismatch of type Error.
	OnModuleHashMismatchError
)

var ErrInvalidOnModuleHashMismatch = fmt.Errorf("not a valid OnModuleHashMismatch, try [%s]", strings.Join(_OnModuleHashMismatchNames, ", "))

const _OnModuleHashMismatchName = "IgnoreWarnError"

var _OnModuleHashMismatchNames = []string{
	_OnModuleHashMismatchName[0:6],
	_OnModuleHashMismatchName[6:10],
	_OnModuleHashMismatchName[10:15],
}

// OnModuleHashMismatchNames returns a list of possible string values of OnModuleHashMismatch.
func OnModuleHashMismatchNames() []string {
	tmp := make([]string, len(_OnModuleHashMismatchNames))
	copy(tmp, _OnModuleHashMismatchNames)
	return tmp
}

var _OnModuleHashMismatchMap = map[OnModuleHashMismatch]string{
	OnModuleHashMismatchIgnore: _OnModuleHashMismatchName[0:6],
	OnModuleHashMismatchWarn:   _OnModuleHashMismatchName[6:10],
	OnModuleHashMismatchError:  _OnModuleHashMismatchName[10:15],
}

// String implements the Stringer interface.
func (x OnModuleHashMismatch) String() string {
	if str, ok := _OnModuleHashMismatchMap[x]; ok {
		return str
	}
	return fmt.Sprintf("OnModuleHashMismatch(%d)", x)
}

// IsValid provides a quick way to determine if the typed value is
// part of the allowed enumerated values
func (x OnModuleHashMismatch) IsValid() bool {
	_, ok := _OnModuleHashMismatchMap[x]
	return ok
}

var _OnModuleHashMismatchValue = map[string]OnModuleHashMismatch{
	_OnModuleHashMismatchName[0:6]:                    OnModuleHashMismatchIgnore,
	strings.ToLower(_OnModuleHashMismatchName[0:6]):   OnModuleHashMismatchIgnore,
	_OnModuleHashMismatchName[6:10]:                   OnModuleHashMismatchWarn,
	strings.ToLower(_OnModuleHashMismatchName[6:10]):  OnModuleHashMismatchWarn,
	_OnModuleHashMismatchName[10:15]:                  OnModuleHashMismatchError,
	strings.ToLower(_OnModuleHashMismatchName[10:15]): OnModuleHashMismatchError,
}

// ParseOnModuleHashMismatch attempts to convert a string to a OnModuleHashMismatch.
func ParseOnModuleHashMismatch(name string) (OnModuleHashMismatch, error) {
	if x, ok := _OnModuleHashMismatchValue[name]; ok {
		return x, nil
	}
	// Case insensitive parse, do a separate lookup to prevent unnecessary cost of lowercasing a string if we don't need to.
	if x, ok := _OnModuleHashMismatchValue[strings.ToLower(name)]; ok {
		return x, nil
	}
	return OnModuleHashMismatch(0), fmt.Errorf("%s is %w", name, ErrInvalidOnModuleHashMismatch)
}

// MarshalText implements the text marshaller method.
func (x OnModuleHashMismatch) MarshalText() ([]byte, error) {
	return []byte(x.String()), nil
}

// UnmarshalText implements the text unmarshaller method.
func (x *OnModuleHashMismatch) UnmarshalText(text []byte) error {
	name := string(text)
	tmp, err := ParseOnModuleHashMismatch(name)
	if err != nil {
		return err
	}
	*x = tmp
	return nil
}
