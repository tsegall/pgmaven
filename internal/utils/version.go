package utils

import "strconv"

const MajorVersion int = 0
const MinorVersion int = 17
const PatchVersion int = 0

func GetVersionString() string {
	return strconv.Itoa(MajorVersion) + "." + strconv.Itoa(MinorVersion) + "." + strconv.Itoa(MajorVersion)
}
