package examples

import (
	"io"
	"net/http"
	"os"
)

const dataUrl = "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet"
const dataPath = "./yellow_tripdata_2022-01.parquet"

func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}

func downloadTestData() error {
	if fileExists(dataPath) {
		return nil
	}

	resp, err := http.Get(dataUrl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	file, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func PathToTestData() string {
	downloadTestData()
	return dataPath
}
