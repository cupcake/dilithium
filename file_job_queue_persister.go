package dilithium

import (
	"log"
	"io"
	"os"
	"encoding/binary"
	"strings"
	"strconv"
	)

const recordLengthBytes = 4 // Use 32-bit numbers for writing the length in the journal.
// journal files are named journal-X where X is an integer
const journalFilenamePrefix = "journal-"
// TODO this should be set through some config
const journalDirpath = "journals/"

const lastDoneFilename = "last_done"

// Provides a persistent (journaled) job queue.
// Its goal is that each job in the queue is seen by one reader once.
// Its guarantee (in the face of failures) is that each job in the queue
// is seen by at least one reader at least once.

// Readers must eventually call a matching Done after calling Get so that
// garbage can be collected. If jobs are held (in between Get and Done)
// long enough that many other Gets or Pushes have occurred in the meantime,
// performance may suffer.

// It is not threadsafe. The user must provide some form of synchronization on top.
// The recommendation is to have one goroutine that owns it and has jobs passed in
// and out via channels. JobQueue does this..

type FileJobQueuePersister struct {
	name string // Name of the file that backs the queue
	head uint64 // id of the front of the front of the queue (last written)
	tail uint64 // id of the back of the queue (lowest id that has not been Done-ed)
	// window of outstanding elements. window[i] accounts for the element of id
	// (i + tail). true means that it has been doneted.
	window []bool
	journals []journalFile
	writeJournal, readJournal int
}

type Job interface {
	Id() uint64
	SetId(uint64)
	Serialize() []byte
	Deserialize([]byte)
}

// Represents one journal file that we are reading from and/or
// writing to.
type journalFile struct {
	// Filename (not including directories) of the file
	basename string
	// The id of the highest record contained. Used to know when we can delete.
	lastRecord uint64
	number int
	readFile *os.File // File handle that we use to read queued jobs
	writeFile *os.File // File handle that we use to write new queued jobs
}

func isJournal(name string) bool {
	return strings.HasPrefix(name, journalFilenamePrefix)
}

func journalNumber(basename string) int {
	// TODO error handle
	num, _ := strconv.Atoi(basename[len(journalFilenamePrefix):])
	return num
}

func (q *FileJobQueuePersister) journalDir() string {
	// TODO at some point should error check to make sure name doesn't have a /
	return journalDirpath + q.name + "/"
}

// Opens and reads journal and returns the highest id of any record in the file
// Also returns an error if there are problems reading the file.
func (q *FileJobQueuePersister) latestRecord(journal journalFile) (uint64, error) {
	// TODO, I don't actually have the exact type of Job that we're using.
	// For now I'm just assuming it's type job (defined in the tests), but we should
	// either make that not an abstract interface and say that your job will simply
	// have an id and a slice of bytes (probably best) or have some way to communicate
	// the exact type to the constructor.
	max := uint64(0)
	file, err := os.Open(q.journalDir() + journal.basename)
	for err != nil {
		job, e := q.nextInFile(file)
		if e == nil {
			if max < job.Id() {
				max = job.Id()
			} else {
				log.Println(job, "followed higher id", max)
			}
		} else {
			err = e
		}
	}
	return max, err
}

func (q *FileJobQueuePersister) readFile() *os.File {
	return q.journals[q.readJournal].readFile
}

func (q *FileJobQueuePersister) writeFile() *os.File {
	return q.journals[q.writeJournal].writeFile
}



func NewFileJobQueuePersister(name string) *FileJobQueuePersister {
	q := new(FileJobQueuePersister)
	q.name = name
	// TODO better erorr handling
	os.Mkdir(journalDirpath, 0777)
	dirname := journalDirpath + name
	os.Mkdir(dirname, 0777)
	dirFile, _ := os.Open(dirname)
	fis, _ := dirFile.Readdir(0)
	journals := make(map[int]journalFile)
	for _, fi := range fis {
		// skip any subdirectories (there shouldn't be any)
		if !fi.IsDir() {
			if isJournal(fi.Name()) {
				journals[journalNumber(fi.Name())] = journalFile{basename: fi.Name()}
			} else if fi.Name() == lastDoneFilename {
				// TODO read and set q.tail to the content
				q.tail = 0
			}
		}
	}
	
	nextJournalNum := 0
	if len(journals) > 0 {
		// TODO
	}
	i := 0 // number of existing open journals
	q.journals = make([] journalFile, 1)
	q.writeJournal = i
	q.readJournal = i
	q.journals[i].number = nextJournalNum
	q.journals[i].basename = journalFilenamePrefix + strconv.Itoa(q.journals[i].number)
	q.journals[i].writeFile, _ = os.Create(q.journalDir() + q.journals[i].basename)
	q.journals[i].readFile, _ = os.Open(q.journalDir() + q.journals[i].basename)
	
		/*writeFile, err := os.Create(q.filename)
		if err == nil {
			// TODO
			readFile, _ := os.Open(q.filename)
			q.writeFile = writeFile
			q.readFile = readFile
		 }
		if err != nil {
			log.Println("Failure opening journal:", err)
			return nil
		} else {
			return q
		}*/
	return q
}

func (q *FileJobQueuePersister) nextInFile(readFrom *os.File) (*job, error) {
	j := &job{}

	// read from file
	lengthBuffer := make([]byte, recordLengthBytes)
	bytesRead, err := q.readFile().Read(lengthBuffer)
	if bytesRead < recordLengthBytes || err != nil {
		if err == io.EOF {
			// Don't pass back EOF
			err = nil
		}
		return nil, err
	}
	recordLength := binary.BigEndian.Uint32(lengthBuffer)
	jobBuffer := make([]byte, recordLength)
	bytesRead, err = q.readFile().Read(jobBuffer)
	if uint32(bytesRead) < recordLength || err != nil {
		return nil, err
	}

	j.Deserialize(jobBuffer)
	return j, nil
}

func (q *FileJobQueuePersister) Get() Job {
	j, _ := q.nextInFile(q.readFile())
	if j == nil {
		return nil
	}

	// Extend window to track that this job has not been done-ed.
	q.window = append(q.window, false)

	return j
}

func writeLen(f *os.File, length uint32) bool {
	bytes := make([]byte, recordLengthBytes)
	binary.BigEndian.PutUint32(bytes, length)
	return writeBytes(f, bytes)
}

func writeBytes(f *os.File, bytes []byte) bool {
	written, err := f.Write(bytes)
	return err == nil && written == len(bytes)
}


func (q *FileJobQueuePersister) Push(job Job) bool {
	// Give it the next id
	job.SetId(q.head)
	q.head++

	bytes := job.Serialize()
	success := writeLen(q.writeFile(), uint32(len(bytes)))
	if success {
		success := writeBytes(q.writeFile(), bytes)
		return success
	}
	return success
}

// Returns the index of job in window, or -1 if out of range
func (q *FileJobQueuePersister) windowIndex(job Job) int {
	ind := int(job.Id() - q.tail)
	if ind < 0 || ind > len(q.window) {
		ind = -1
	}
	return ind
}

func firstFalseIndex(slice []bool) int {
	for i, v := range slice {
		if !v {
			return i
		}
	}
	return len(slice)
}

func (q *FileJobQueuePersister) updateDurablyWritten() {
	// TODO write q.tail to some file.
}

func (q *FileJobQueuePersister) Done(job Job) {
	ind := q.windowIndex(job)
	if ind < 0 {
		// TODO log or return 0 or something
		return
	}
	// Try to slide the window forward
	newTail := firstFalseIndex(q.window)
	if newTail > 0 {
		// Slide window forward.
		q.window = q.window[:newTail]
		q.tail += uint64(newTail)

		q.updateDurablyWritten()
	}
}
