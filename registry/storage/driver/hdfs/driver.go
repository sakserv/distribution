package hdfs

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/colinmarc/hdfs"
	"strconv"
)

// Set the version
const CurrentVersion Version = "0.1"

// Default values for the driverParameters if not set by the user.
const (
	driverName			= "hdfs"
	driverDisplayName		= "HDFS Storage Driver"
	defaultHdfsRootDirectory	= "/tmp/hdfs-registry"
	defaultHdfsNamenode		= ""
	defaultHdfsUser			= "hdfs"
	defaultDirectoryUmask		= 0755
)

//
// Implement factory.StorageDriverFactory, register the driver, and validate
// user input.
//

// driverParameters is a struct that encapsulates all of the driver parameters after all values have been set
type driverParameters struct {
	hdfsRootDirectory string
	hdfsNameNode string
	hdfsUser string
	directoryUmask int
}

type driver struct {
	hdfsRootDirectory string
	hdfsNameNode string
	hdfsUser string
	directoryUmask int
	hdfsClient *hdfs.Client
}

// hdfsDriverFactory implements the factory.StorageDriverFactory interface
type hdfsDriverFactory struct{}

// Registry the driver
func init() {
	factory.Register(driverName, &hdfsDriverFactory{})
}

// Create StorageDriver from parameters
func (factory *hdfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - hdfsrootdirectory
// - hdfsuser
// - directoryumask
// Required Parameters:
// - hdfsnamenode
func FromParameters(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	// Load the defaults
	var hdfsRootDirectory = defaultHdfsRootDirectory
	var hdfsNamenode = defaultHdfsNamenode
	var hdfsUser = defaultHdfsUser
	var directoryUmask = defaultDirectoryUmask

	// Validate input
	if parameters != nil {
		// Get hdfsRootdirectory
		hdfsRootDir, ok := parameters["hdfsrootdirectory"]
		if ok {
			hdfsRootDirectory = fmt.Sprint(hdfsRootDir)
		}

		// Get hdfsNamenode
		nameNode, ok := parameters["hdfsnamenode"]
		if ok {
			hdfsNamenode = fmt.Sprint(nameNode)
		}

		// Get hdfsUser
		hUser, ok := parameters["hdfsuser"]
		if ok {
			hdfsUser = fmt.Sprint(hUser)
		}

		// Get directoryUmask
		dUmask, ok := parameters["directoryumask"]
		if ok {
			directoryUmask = dUmask.(int)
		}
	}

	// Populate params
	params := driverParameters{
		hdfsRootDirectory:	hdfsRootDirectory,
		hdfsNameNode:		hdfsNamenode,
		hdfsUser:		hdfsUser,
		directoryUmask:		directoryUmask,
	}

	return New(params)
}

// New constructs a new driver
func New(params driverParameters) (storagedriver.StorageDriver, error) {

	// Setup the connection to hdfs
	client, err := hdfs.NewForUser(params.hdfsNameNode, params.hdfsUser)
	if err != nil {
		log.Fatal(err)
	}

	// Populate the driver
	d := &driver{
		hdfsRootDirectory:	params.hdfsRootDirectory,
		hdfsNameNode:		params.hdfsNameNode,
		hdfsUser:		params.hdfsUser,
		directoryUmask:		params.directoryUmask,
		hdfsClient:		client,
	}

	// Return the StorageDriver
	return &base.Base{
		StorageDriver: d,
	}, nil
}

//
// Implement the storagedriver.StorageDriver interface
//

// Return the display name
func (d *driver) Name() string {
	return driverDisplayName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(context context.Context, path string) ([]byte, error) {
	fullPath := d.fullPath(path)
	p, err := d.hdfsClient.ReadFile(fullPath)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: d.fullPath(path)}
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(context context.Context, path string, contents[]byte) error {
	fullPath := d.fullPath(path)
	d.makeParentDir(fullPath)

	// Get the FileWriter
	writer, err := d.Writer(context, fullPath, false)
	if err != nil {
		log.Print(err)
	}

	// Write the contents
	_, err = writer.Write(contents)
	if err != nil {
		log.Print(err)
	}
	writer.Close()
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(context context.Context, path string, offset int64) (io.ReadCloser, error) {
	fullPath := d.fullPath(path)

	// Open the file
	reader, err := d.hdfsClient.Open(fullPath)
	if(err != nil) {
		log.Print(err)
	}

	// Seek to the supplied offset
	seekPos, err := reader.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		reader.Close()
		return nil, err
	} else if seekPos < int64(offset) {
		reader.Close()
		return nil, storagedriver.InvalidOffsetError{Path: fullPath, Offset: offset}
	}

	return reader, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(context context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fullPath := d.fullPath(path)
	d.makeParentDir(fullPath)

	reader, err := d.hdfsClient.Open(fullPath)
	if err != nil {
		hdfsWriter, _ := d.hdfsClient.Create(fullPath)
		return newFileWriter(hdfsWriter, fullPath, 0), nil
	} else {
		if !append {
			d.hdfsClient.Remove(fullPath)
			hdfsWriter, _ := d.hdfsClient.Create(fullPath)
			return newFileWriter(hdfsWriter, fullPath, 0), nil
		} else {
			hdfsWriter, _ := d.hdfsClient.Append(fullPath)
			return newFileWriter(hdfsWriter, fullPath, reader.Stat().Size()), nil
		}
	}
}


// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(context context.Context, path string) (storagedriver.FileInfo, error) {
	fi, err := d.hdfsClient.Stat(d.fullPath(path))
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: d.fullPath(path)}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: storagedriver.FileInfoFields{
		Path:    d.fullPath(path),
		Size:    int64(fi.Size()),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}}, nil
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *driver) List(context context.Context, subPath string) ([]string, error) {
	fileInfos, err := d.hdfsClient.ReadDir(d.fullPath(subPath))
	if err != nil {
		return make([]string, 0), nil
	}

	fileNames := make([]string, len(fileInfos))
	for index, fileInfo := range fileInfos {
		fileNames[index] = d.fullPath(subPath) + "/" + fileInfo.Name()
	}
	return fileNames, nil
}


// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *driver) Move(context context.Context, sourcePath string, destPathstring string) error {
	d.makeParentDir(d.fullPath(destPathstring))
	return d.hdfsClient.Rename(d.fullPath(sourcePath), d.fullPath(destPathstring))
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(context context.Context, path string) error {
	//return nil
	return d.hdfsClient.Remove(d.fullPath(path))
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
func (d *driver) URLFor(context context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

//
// Implement the storagedriver.FileWriter interface
//
type fileWriter struct {
	hdfsWriter		*hdfs.FileWriter
	filePath		string
	isClosed		bool
	writeSize		int64
	startingFileSize 	int64
}

func newFileWriter(hdfsWriter *hdfs.FileWriter, filePath string, startingFileSize int64) *fileWriter {
	return &fileWriter{
		hdfsWriter: hdfsWriter,
		filePath: filePath,
		startingFileSize: startingFileSize,
	}
}

func (w *fileWriter) Write(p []byte) (int, error) {
	w.Size()
	if _, err := w.hdfsWriter.Write(p); err != nil {
		log.Print(err)
	}
	w.isClosed = false
	w.writeSize += int64(len(p))
	return len(p), nil
}

// Close the client connection
func (w *fileWriter) Close() error {
	w.Size()
	if w.hdfsWriter != nil {
		if !w.isClosed {
			w.isClosed = true
			if err := w.hdfsWriter.Close(); err != nil {
				log.Print(err)
			}

		}
	}
	return nil
}

// Size returns the number of bytes written to this FileWriter.
func (w *fileWriter) Size() int64 {
	if w.startingFileSize > 0 {
		w.writeSize += w.startingFileSize
		w.startingFileSize = 0
	}
	return w.writeSize
}

// Cancel removes any written content from this FileWriter.
func (w *fileWriter) Cancel() error {
	return nil
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (w *fileWriter) Commit() error {
	return nil
}

//
// Utils
//

// fullPath returns the full path to the file
func (d *driver) fullPath(subPath string) string {
	if strings.HasPrefix(subPath, d.hdfsRootDirectory) {
		return subPath
	}
	return path.Join(d.hdfsRootDirectory, subPath)
}

// creates the parent directory with the default umask
func (d *driver) makeParentDir(subPath string) error {
	if err := d.hdfsClient.MkdirAll(path.Dir(d.fullPath(subPath)), os.FileMode(d.directoryUmask)); err != nil {
		return err
	}
	return nil
}
