package persistence

import (
	"bytes"
	"encoding/base32"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const DEBUG = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf(format, a...)
	}
	return
}

func ReadTransactionSuccess(dir string) bool {
	var success bool
	if err := ReadFile(dir, "transaction_success", &success); err != nil {
		success = false
	}
	return success
}

func SyncTempfile(dir string, success bool) error {
	DPrintf("Syncing tempfile dir %v", dir)
	makeDir(dir)
	files, _ := ioutil.ReadDir(dir)
	for _, file := range files {
		if file.IsDir() {
			if err := SyncTempfile(dir+"/"+file.Name(), success); err != nil {
				return err
			}
		} else {
			if strings.HasPrefix(file.Name(), "temp-") {
				tempname := dir + "/" + file.Name()
				fullname := dir + "/" + file.Name()[5:]
				DPrintf("sync temp file, filename %v", tempname)
				if success {
					if err := os.Rename(tempname, fullname); err != nil {
						return err
					}
				} else {
					os.Remove(tempname)
				}
			}
		}
	}
	return nil
}

func ReadFile(dir string, name string, content interface{}) error {
	makeDir(dir)
	if _, err := os.Stat(dir + "/" + name); err != nil {
		return err
	}
	if value, err := ioutil.ReadFile(dir + "/" + name); err == nil {
		if err := decode(string(value), content); err != nil {
			panic(err)
		}
	} else {
		return err
	}
	return nil
}

func WriteFile(dir string, name string, content interface{}) error {
	makeDir(dir)
	value, err := encode(content)
	if err != nil {
		panic(err)
	}
	fullname := dir + "/" + name
	if err := ioutil.WriteFile(fullname, []byte(value), 0666); err != nil {
		return err
	}
	return nil
}

func WriteTempFile(dir string, name string, content interface{}) error {
	if content == nil {
		return nil
	}
	tempname := "temp-" + name
	if err := WriteFile(dir, tempname, content); err != nil {
		return err
	}
	return nil
}

func makeDir(dir string) {
	if _, err := os.Stat(dir); err != nil {
		if err := os.MkdirAll(dir, 0777); err != nil {
			panic(err)
		}
	}
}

func encode(v interface{}) (string, error) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	return string(w.Bytes()), nil
}

func decode(buf string, v interface{}) error {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	if err := d.Decode(v); err != nil {
		return err
	}
	return nil
}

func encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func decodeKey(filename string) string {
	key, err := base32.StdEncoding.DecodeString(filename)
	if err != nil {
		panic(err)
	}
	return string(key)
}
