package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
)

var (
	blocksize = flag.Int("blocksize", 32, "")

	dryrun = flag.Bool("dryrun", false, "")

	// Display usage if true.
	showHelp = flag.Bool("help", false, "")
)

const helpMessage = `
raveler-import sends a series of label slabs to a DVID server. 

Usage: raveler-import [options] <config file>

	    -blocksize      =number   Number of Z slices should be combined to form each label slab (default 32)

	    -dryrun         (flag)    Don't actually POST data
	-h, -help           (flag)    Show help message

The configuration file should be JSON that gives the slabs to be imported and their Z range.  Example:

{
	"URI": "http://emdata2.int.janelia.org:7000/api/653/M10_LO/raw/0_1_2/18534_10786_32/",
	"SizeX": 18534,
	"SizeY": 10786,
	"Thickness": 32,
	"Directories": [
		{
			"Path": "/groups/flyem/data/dvid-data/FIB-19/M10",
			"BegZ": 10058,
			"EndZ": 13182,
			"Template": "bodies-z%05d-18534x10786x32.gz"
		},
		{
			"Path": "/groups/flyem/data/dvid-data/FIB-19/LO",
			"BegZ": 13183,
			"EndZ": 17557,
			"Template": "bodies-z%05d-18534x10786x32.gz"
		}
	]
}
`

var usage = func() {
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

type Config struct {
	URI         string
	SizeX       int
	SizeY       int
	Thickness   int
	BegZ        int
	EndZ        int
	Directories []SlabDir
}

type SlabDir struct {
	Path     string
	BegZ     int
	EndZ     int
	Template string
}

func readConfig(filename string) Config {
	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		fmt.Printf("Could not open configuration JSON file: %s\n", err.Error())
		os.Exit(1)
	}

	jsonBytes, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Printf("Could not parse configuration JSON file: %s\n", err.Error())
		os.Exit(1)
	}
	var config Config
	if err := json.Unmarshal(jsonBytes, &config); err != nil {
		fmt.Printf("Error reading configuration JSON file: %s\n", err.Error())
		os.Exit(1)
	}
	return config
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if *showHelp || len(args) != 1 {
		flag.Usage()
		os.Exit(0)
	}

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	// Load the configuration file
	config := readConfig(args[0])
	if len(config.Directories) < 1 {
		fmt.Printf("ERROR: Found no directories in configuration file.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if config.Thickness != *blocksize {
		fmt.Printf("Currently, destination block size (%d) must equal the thickness of the import slabs (%d)!\n", *blocksize, config.Thickness)
		os.Exit(1)
	}

	// Make sure that each directory has non-overlapping Z ranges, goes from smallest to largest, and blocksize is slab size.
	maxZ := 0
	for _, dir := range config.Directories {
		if dir.BegZ <= maxZ {
			fmt.Printf("Directory %q is not in order.  BegZ %d is <= than previous directory Z %d\n", dir.Path, dir.BegZ, maxZ)
			os.Exit(1)
		}
		if dir.EndZ <= dir.BegZ {
			fmt.Printf("Directory %q has bad Z range: %d -> %d\n", dir.Path, dir.BegZ, dir.EndZ)
		}
		maxZ = dir.EndZ
	}

	// Process each directory, label slab by label slab.
	bytebuf := make([]byte, config.SizeX*config.SizeY*config.Thickness*8)
	for slabBegZ := config.BegZ; slabBegZ <= config.EndZ; slabBegZ += *blocksize {
		if err := processSlab(config, bytebuf, slabBegZ); err != nil {
			fmt.Printf("Error processing slab @ %d: %s\n", slabBegZ, err.Error())
			os.Exit(1)
		}
	}
}

func processSlab(config Config, bytebuf []byte, slabBegZ int) error {
	for i := range bytebuf {
		bytebuf[i] = 0
	}

	sliceBytes := config.SizeX * config.SizeY * 8
	slabEndZ := slabBegZ + *blocksize - 1
	url := fmt.Sprintf("%s/0_0_%d", config.URI, slabBegZ)

	// Iterate through all directories and fill in byte buffer when intersecting.
	zfilled := 0
	for _, dir := range config.Directories {
		begZ := slabBegZ
		endZ := slabEndZ
		if begZ > dir.EndZ || endZ < dir.BegZ {
			continue
		}
		if begZ < dir.BegZ {
			begZ = dir.BegZ
		}
		if endZ > dir.EndZ {
			endZ = dir.EndZ
		}

		// Get the file
		filename := fmt.Sprintf(dir.Template, slabBegZ)
		fmt.Printf("Getting data for Z %d -> %d from %s ...\n", begZ, endZ, filename)

		var f *os.File
		var err error
		if f, err = os.Open(filename); err != nil {
			return err
		}
		defer f.Close()

		// If there is a slab for exactly this range, just send it.
		if begZ == slabBegZ && endZ == slabEndZ {
			fmt.Printf("Sending data via POST to %s ...\n", url)
			if !*dryrun {
				r, err := http.Post(url, "application/octet-stream", f)
				if err != nil {
					return err
				}
				if r.StatusCode != http.StatusOK {
					return fmt.Errorf("Received bad status from POST on %q: %d\n", url, r.StatusCode)
				}
			}
			return nil
		}

		// Else we have to read, uncompress, store into bytebuf.
		gr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		data, err := ioutil.ReadAll(gr)
		if err != nil {
			return err
		}
		if err = gr.Close(); err != nil {
			return err
		}
		if len(data) != sliceBytes**blocksize {
			return fmt.Errorf("Expected %d bytes from uncompressed gzip file, got %d instead.\n", sliceBytes**blocksize, len(data))
		}

		for volZ := begZ; volZ <= endZ; volZ++ {
			zfilled++
			fmt.Printf("Transferring slice %d over to buffer (%d filled)...\n", volZ, zfilled)
			z := (volZ - slabBegZ) * sliceBytes
			copy(bytebuf[z:z+sliceBytes], data[z:z+sliceBytes])
		}

		// If we've filled up the bytebuf, send it.
		if zfilled == *blocksize {
			fmt.Printf("Filled bytebuf, sending data.\n")
			if !*dryrun {
				r, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(bytebuf))
				if err != nil {
					return err
				}
				if r.StatusCode != http.StatusOK {
					return fmt.Errorf("Received bad status from POST on %q: %d\n", url, r.StatusCode)
				}
			}
			return nil
		}
	}
	return nil
}
