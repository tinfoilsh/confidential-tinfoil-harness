package remoteimage

import (
	"bytes"
	"encoding/binary"
	"image"

	"github.com/disintegration/imaging"
)

const (
	jpegMarkerPrefix = 0xff
	jpegSOI          = 0xd8
	jpegEOI          = 0xd9
	jpegSOS          = 0xda
	jpegAPP1         = 0xe1
	jpegTEM          = 0x01
	jpegRestartFirst = 0xd0
	jpegRestartLast  = 0xd7
	exifSignature    = "Exif\x00\x00"
	tiffHeaderSize   = 8
	tiffMagic        = 42
	tiffEntrySize    = 12
	tiffOrientation  = 0x0112
	tiffShort        = 3
)

const (
	orientationNormal uint16 = iota + 1
	orientationFlipH
	orientationRotate180
	orientationFlipV
	orientationTranspose
	orientationRotate270
	orientationTransverse
	orientationRotate90
)

// jpegOrientation reads only IFD0's inline orientation value. Every TIFF offset
// is bounded by its APP1 segment; other directories and metadata are not read.
func jpegOrientation(data []byte) uint16 {
	if !bytes.HasPrefix(data, []byte{jpegMarkerPrefix, jpegSOI}) {
		return 0
	}
	data = data[2:]
	for len(data) >= 2 && data[0] == jpegMarkerPrefix {
		data = data[1:]
		if data[0] == jpegMarkerPrefix {
			continue
		}
		marker := data[0]
		data = data[1:]
		switch {
		case marker == jpegSOS || marker == jpegEOI || marker == 0:
			return 0
		case marker == jpegTEM || marker >= jpegRestartFirst && marker <= jpegRestartLast:
			continue
		}
		if len(data) < 2 {
			return 0
		}
		size := int(binary.BigEndian.Uint16(data))
		if size < 2 || size > len(data) {
			return 0
		}
		payload := data[2:size]
		data = data[size:]
		if marker == jpegAPP1 && bytes.HasPrefix(payload, []byte(exifSignature)) {
			if orientation := tiffDisplayOrientation(payload[len(exifSignature):]); orientation != 0 {
				return orientation
			}
		}
	}
	return 0
}

func tiffDisplayOrientation(data []byte) uint16 {
	if len(data) < tiffHeaderSize {
		return 0
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 0
	}
	if order.Uint16(data[2:4]) != tiffMagic {
		return 0
	}
	offset := order.Uint32(data[4:8])
	if offset < tiffHeaderSize || uint64(offset) > uint64(len(data)-2) {
		return 0
	}
	data = data[offset:]
	entries := int(order.Uint16(data))
	data = data[2:]
	if entries > len(data)/tiffEntrySize {
		return 0
	}
	for range entries {
		entry := data[:tiffEntrySize]
		data = data[tiffEntrySize:]
		if order.Uint16(entry[:2]) == tiffOrientation && order.Uint16(entry[2:4]) == tiffShort && order.Uint32(entry[4:8]) == 1 {
			return order.Uint16(entry[8:10])
		}
	}
	return 0
}

func orientJPEG(img image.Image, data []byte) image.Image {
	switch jpegOrientation(data) {
	case orientationFlipH:
		return imaging.FlipH(img)
	case orientationRotate180:
		return imaging.Rotate180(img)
	case orientationFlipV:
		return imaging.FlipV(img)
	case orientationTranspose:
		return imaging.Transpose(img)
	case orientationRotate270:
		return imaging.Rotate270(img)
	case orientationTransverse:
		return imaging.Transverse(img)
	case orientationRotate90:
		return imaging.Rotate90(img)
	default:
		return img
	}
}
