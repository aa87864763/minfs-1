package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"client/pkg/client"
)

var globalClient *client.MinifsClient

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <metaserver_address>")
		fmt.Println("Example: go run main.go localhost:8080")
		os.Exit(1)
	}

	metaServerAddr := os.Args[1]
	
	// 创建client实例
	c, err := client.NewMinifsClient(metaServerAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	globalClient = c

	fmt.Printf("=== MiniFS Client Connected to %s ===\n", metaServerAddr)
	fmt.Println("Type 'help' for available commands")
	
	// 启动命令行界面
	startCommandLine()
}

func startCommandLine() {
	scanner := bufio.NewScanner(os.Stdin)
	
	for {
		fmt.Print("minifs> ")
		if !scanner.Scan() {
			break
		}
		
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		
		command := strings.ToLower(parts[0])
		args := parts[1:]
		
		executeCommand(command, args)
	}
}

func executeCommand(command string, args []string) {
	switch command {
	case "help", "h":
		showHelp()
		
	case "exit", "quit", "q":
		fmt.Println("Goodbye!")
		os.Exit(0)
		
	case "cluster":
		cmdCluster(args)
		
	case "create":
		cmdCreate(args)
		
	case "mkdir":
		cmdMkdir(args)
		
	case "ls":
		cmdLs(args)
		
	case "stat":
		cmdStat(args)
		
	case "rm":
		cmdRm(args)
		
	case "write":
		cmdWrite(args)
		
	case "read":
		cmdRead(args)
		
	case "put":
		cmdPut(args)
		
	case "get":
		cmdGet(args)
		
	default:
		fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
	}
}

func showHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  help                      - Show this help message")
	fmt.Println("  exit/quit/q              - Exit the client")
	fmt.Println("")
	fmt.Println("File Operations:")
	fmt.Println("  create <path>            - Create a file")
	fmt.Println("  mkdir <path>             - Create a directory") 
	fmt.Println("  ls <path>                - List directory contents")
	fmt.Println("  stat <path>              - Show file/directory status")
	fmt.Println("  rm <path> [recursive]    - Delete file/directory")
	fmt.Println("")
	fmt.Println("Data Operations:")
	fmt.Println("  write <path> <text>      - Write text to file")
	fmt.Println("  read <path>              - Read file content")
	fmt.Println("  put <local_file> <remote_path> - Upload local file")
	fmt.Println("  get <remote_path> <local_file> - Download file")
	fmt.Println("")
	fmt.Println("Cluster:")
	fmt.Println("  cluster                  - Show cluster information")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  mkdir /test")
	fmt.Println("  create /test/hello.txt")
	fmt.Println("  write /test/hello.txt \"Hello MiniFS!\"")
	fmt.Println("  read /test/hello.txt")
	fmt.Println("  stat /test/hello.txt")
	fmt.Println("  ls /test")
	fmt.Println("  rm /test/hello.txt")
	fmt.Println("  rm /test recursive")
}

func cmdCluster(args []string) {
	_, err := globalClient.GetClusterInfo()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdCreate(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: create <path>")
		return
	}
	
	err := globalClient.Create(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdMkdir(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: mkdir <path>")
		return
	}
	
	err := globalClient.CreateDirectory(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdLs(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: ls <path>")
		return
	}
	
	_, err := globalClient.ListStatus(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdStat(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: stat <path>")
		return
	}
	
	_, err := globalClient.GetStatus(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdRm(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: rm <path> [recursive]")
		return
	}
	
	recursive := false
	if len(args) > 1 && strings.ToLower(args[1]) == "recursive" {
		recursive = true
	}
	
	err := globalClient.Delete(args[0], recursive)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdWrite(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: write <path> <text>")
		return
	}
	
	path := args[0]
	text := strings.Join(args[1:], " ")
	
	// 去除引号
	if strings.HasPrefix(text, "\"") && strings.HasSuffix(text, "\"") {
		text = text[1 : len(text)-1]
	}
	
	err := globalClient.WriteFile(path, []byte(text))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdRead(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: read <path>")
		return
	}
	
	data, err := globalClient.ReadFile(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Printf("Content of %s:\n", args[0])
	fmt.Printf("--- BEGIN ---\n")
	fmt.Printf("%s", string(data))
	fmt.Printf("\n--- END ---\n")
}

func cmdPut(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: put <local_file> <remote_path>")
		return
	}
	
	localFile := args[0]
	remotePath := args[1]
	
	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		fmt.Printf("Error reading local file: %v\n", err)
		return
	}
	
	err = globalClient.WriteFile(remotePath, data)
	if err != nil {
		fmt.Printf("Error uploading file: %v\n", err)
		return
	}
	
	fmt.Printf("Successfully uploaded %s to %s (%d bytes)\n", localFile, remotePath, len(data))
}

func cmdGet(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: get <remote_path> <local_file>")
		return
	}
	
	remotePath := args[0]
	localFile := args[1]
	
	data, err := globalClient.ReadFile(remotePath)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return
	}
	
	err = ioutil.WriteFile(localFile, data, 0644)
	if err != nil {
		fmt.Printf("Error writing local file: %v\n", err)
		return
	}
	
	fmt.Printf("Successfully downloaded %s to %s (%d bytes)\n", remotePath, localFile, len(data))
}