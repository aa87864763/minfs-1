package main

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		showUsage()
		os.Exit(1)
	}

	filename := os.Args[1]
	sizeStr := os.Args[2]

	// 解析文件大小
	size, err := parseSize(sizeStr)
	if err != nil {
		fmt.Printf("Error parsing size: %v\n", err)
		showUsage()
		os.Exit(1)
	}

	// 获取生成模式，默认为随机模式
	mode := "random"
	if len(os.Args) > 3 {
		mode = strings.ToLower(os.Args[3])
	}

	fmt.Printf("Generating file: %s\n", filename)
	fmt.Printf("Size: %s (%d bytes)\n", sizeStr, size)
	fmt.Printf("Mode: %s\n", mode)

	// 生成文件
	err = generateFile(filename, size, mode)
	if err != nil {
		fmt.Printf("Error generating file: %v\n", err)
		os.Exit(1)
	}

	// 计算并显示MD5
	hash, err := calculateMD5(filename)
	if err != nil {
		fmt.Printf("Warning: failed to calculate MD5: %v\n", err)
	} else {
		fmt.Printf("MD5: %x\n", hash)
	}

	fmt.Printf("Successfully generated file: %s\n", filename)
}

func showUsage() {
	fmt.Println("File Generator - Create test files of arbitrary size")
	fmt.Println()
	fmt.Println("Usage: go run main.go <filename> <size> [mode]")
	fmt.Println()
	fmt.Println("Size formats:")
	fmt.Println("  123        - bytes")
	fmt.Println("  123B       - bytes")
	fmt.Println("  123K/123KB - kilobytes")
	fmt.Println("  123M/123MB - megabytes")
	fmt.Println("  123G/123GB - gigabytes")
	fmt.Println()
	fmt.Println("Modes:")
	fmt.Println("  random     - random binary data (default)")
	fmt.Println("  text       - readable text content")
	fmt.Println("  zeros      - all zeros")
	fmt.Println("  pattern    - repeating pattern")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  go run main.go test.txt 1MB random")
	fmt.Println("  go run main.go large.bin 100MB text")
	fmt.Println("  go run main.go huge.dat 1GB zeros")
	fmt.Println("  go run main.go pattern.txt 10K pattern")
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	
	// 去掉末尾的B
	if strings.HasSuffix(sizeStr, "B") {
		sizeStr = sizeStr[:len(sizeStr)-1]
	}
	
	var multiplier int64 = 1
	var numberStr string
	
	if strings.HasSuffix(sizeStr, "G") {
		multiplier = 1024 * 1024 * 1024
		numberStr = sizeStr[:len(sizeStr)-1]
	} else if strings.HasSuffix(sizeStr, "M") {
		multiplier = 1024 * 1024
		numberStr = sizeStr[:len(sizeStr)-1]
	} else if strings.HasSuffix(sizeStr, "K") {
		multiplier = 1024
		numberStr = sizeStr[:len(sizeStr)-1]
	} else {
		numberStr = sizeStr
	}
	
	number, err := strconv.ParseInt(numberStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}
	
	if number < 0 {
		return 0, fmt.Errorf("size cannot be negative")
	}
	
	return number * multiplier, nil
}

func generateFile(filename string, size int64, mode string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	switch mode {
	case "random":
		return generateRandomFile(file, size)
	case "text":
		return generateTextFile(file, size)
	case "zeros":
		return generateZerosFile(file, size)
	case "pattern":
		return generatePatternFile(file, size)
	default:
		return fmt.Errorf("unknown mode: %s", mode)
	}
}

func generateRandomFile(file *os.File, size int64) error {
	const chunkSize = 64 * 1024 // 64KB chunks
	buffer := make([]byte, chunkSize)
	
	written := int64(0)
	for written < size {
		remaining := size - written
		currentChunk := chunkSize
		if remaining < int64(chunkSize) {
			currentChunk = int(remaining)
			buffer = buffer[:currentChunk]
		}
		
		_, err := rand.Read(buffer)
		if err != nil {
			return fmt.Errorf("failed to generate random data: %v", err)
		}
		
		n, err := file.Write(buffer)
		if err != nil {
			return fmt.Errorf("failed to write data: %v", err)
		}
		
		written += int64(n)
		
		// 显示进度
		if written%(1024*1024) == 0 || written == size {
			fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", 
				float64(written)/float64(size)*100, written, size)
		}
	}
	
	fmt.Println() // 换行
	return nil
}

func generateTextFile(file *os.File, size int64) error {
	text := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
		"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris. " +
		"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum. " +
		"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia. "
	
	textBytes := []byte(text)
	textLen := int64(len(textBytes))
	
	written := int64(0)
	for written < size {
		remaining := size - written
		if remaining < textLen {
			textBytes = textBytes[:remaining]
		}
		
		n, err := file.Write(textBytes)
		if err != nil {
			return fmt.Errorf("failed to write text: %v", err)
		}
		
		written += int64(n)
		
		// 显示进度
		if written%(1024*1024) == 0 || written == size {
			fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", 
				float64(written)/float64(size)*100, written, size)
		}
	}
	
	fmt.Println() // 换行
	return nil
}

func generateZerosFile(file *os.File, size int64) error {
	const chunkSize = 64 * 1024 // 64KB chunks
	buffer := make([]byte, chunkSize)
	// buffer已经被初始化为全0
	
	written := int64(0)
	for written < size {
		remaining := size - written
		currentChunk := chunkSize
		if remaining < int64(chunkSize) {
			currentChunk = int(remaining)
			buffer = buffer[:currentChunk]
		}
		
		n, err := file.Write(buffer)
		if err != nil {
			return fmt.Errorf("failed to write zeros: %v", err)
		}
		
		written += int64(n)
		
		// 显示进度
		if written%(1024*1024) == 0 || written == size {
			fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", 
				float64(written)/float64(size)*100, written, size)
		}
	}
	
	fmt.Println() // 换行
	return nil
}

func generatePatternFile(file *os.File, size int64) error {
	pattern := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	patternLen := int64(len(pattern))
	
	written := int64(0)
	for written < size {
		remaining := size - written
		if remaining < patternLen {
			pattern = pattern[:remaining]
		}
		
		n, err := file.Write(pattern)
		if err != nil {
			return fmt.Errorf("failed to write pattern: %v", err)
		}
		
		written += int64(n)
		
		// 显示进度
		if written%(1024*1024) == 0 || written == size {
			fmt.Printf("\rProgress: %.1f%% (%d/%d bytes)", 
				float64(written)/float64(size)*100, written, size)
		}
	}
	
	fmt.Println() // 换行
	return nil
}

func calculateMD5(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}