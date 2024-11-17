package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func export() {
	// 指定資料夾路徑
	folderPath := "/Users/haruki/golang-mvcc" // 請將此路徑更改為你要遍歷的資料夾

	// 輸出檔案名稱
	outputFileName := "output.txt"

	// 開啟或建立輸出檔案
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("無法建立輸出檔案: %v\n", err)
		return
	}
	defer outputFile.Close()

	// 遍歷資料夾
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 檢查是否為 .go 檔案
		if !info.IsDir() && filepath.Ext(path) == ".go" {
			// 讀取檔案內容
			content, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			// 寫入路徑和內容到輸出檔案
			_, err = outputFile.WriteString(fmt.Sprintf("File: %s\n", path))
			if err != nil {
				return err
			}
			_, err = outputFile.WriteString(string(content) + "\n\n")
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Printf("遍歷資料夾時發生錯誤: %v\n", err)
	} else {
		fmt.Printf("所有 .go 檔案已成功輸出到 %s\n", outputFileName)
	}
}

func main() {
	export()
}
