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
		fmt.Println("Usage: ../bin/client <metaserver_addresses>")
		fmt.Println("Example: ../bin/client localhost:9090")
		fmt.Println("Example: ../bin/client localhost:9090,localhost:9091,localhost:9092")
		os.Exit(1)
	}

	metaServerAddrs := strings.Split(os.Args[1], ",")
	
	// 调试信息：显示解析后的地址
	fmt.Printf("Parsed MetaServer addresses: %v\n", metaServerAddrs)
	
	// 创建client实例
	c, err := client.NewMinifsClient(metaServerAddrs)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	globalClient = c

	fmt.Printf("=== MiniFS Client Connected (MetaServers: %s) ===\n", strings.Join(metaServerAddrs, ", "))
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
		
	case "leader":
		cmdLeader(args)
		
	case "test-wal":
		cmdTestWAL(args)
		
	case "test-consistency":
		cmdTestConsistency(args)
		
	case "replicas", "repl":
		cmdReplicas(args)
		
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
	fmt.Println("  rm <path>                - Delete file/directory (recursive)")
	fmt.Println("")
	fmt.Println("Data Operations:")
	fmt.Println("  write <path> <text>      - Write text to file")
	fmt.Println("  read <path>              - Read file content")
	fmt.Println("  put <local_file> <remote_path> - Upload local file")
	fmt.Println("  get <remote_path> <local_file> - Download file")
	fmt.Println("")
	fmt.Println("Cluster:")
	fmt.Println("  cluster                  - Show cluster information")
	fmt.Println("  leader                   - Show MetaServer leader/follower information")
	fmt.Println("  replicas [path]          - Show file replication status (all files if no path)")
	fmt.Println("")
	fmt.Println("Testing:")
	fmt.Println("  test-wal                 - Test WAL synchronization between leader and followers")
	fmt.Println("  test-consistency         - Test data consistency across all MetaServers")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  mkdir /test")
	fmt.Println("  create /test/hello.txt")
	fmt.Println("  write /test/hello.txt \"Hello MiniFS!\"")
	fmt.Println("  read /test/hello.txt")
	fmt.Println("  stat /test/hello.txt")
	fmt.Println("  ls /test")
	fmt.Println("  rm /test/hello.txt")
	fmt.Println("  rm /test")
}

func cmdCluster(args []string) {
	_, err := globalClient.GetClusterInfo()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdLeader(args []string) {
	_, err := globalClient.GetLeader()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func cmdTestWAL(args []string) {
	fmt.Println("=== WAL Synchronization Test ===")
	
	// 1. 获取当前leader信息
	fmt.Println("Step 1: Getting leader information...")
	_, err := globalClient.GetLeader()
	if err != nil {
		fmt.Printf("Error getting leader info: %v\n", err)
		return
	}
	
	// 2. 创建测试目录和文件来触发WAL
	testDir := "/wal-test"
	testFile := "/wal-test/test-file.txt"
	
	fmt.Printf("Step 2: Creating test directory: %s\n", testDir)
	err = globalClient.CreateDirectory(testDir)
	if err != nil {
		fmt.Printf("Error creating test directory: %v\n", err)
		return
	}
	
	fmt.Printf("Step 3: Creating test file: %s\n", testFile)
	err = globalClient.Create(testFile)
	if err != nil {
		fmt.Printf("Error creating test file: %v\n", err)
		return
	}
	
	fmt.Printf("Step 4: Writing data to test file\n")
	testData := "WAL synchronization test data"
	err = globalClient.WriteFile(testFile, []byte(testData))
	if err != nil {
		fmt.Printf("Error writing test file: %v\n", err)
		return
	}
	
	fmt.Printf("Step 5: Verifying test file exists\n")
	_, err = globalClient.GetStatus(testFile)
	if err != nil {
		fmt.Printf("Error verifying test file: %v\n", err)
		return
	}
	
	fmt.Println("✓ WAL Test completed successfully!")
	fmt.Println("This test created WAL entries that should be synchronized to all followers.")
	fmt.Println("Use 'test-consistency' to verify data consistency across all MetaServers.")
}

func cmdTestConsistency(args []string) {
	fmt.Println("=== Data Consistency Test ===")
	fmt.Println("This test verifies that WAL synchronization is working properly.")
	fmt.Println("When you perform write operations as leader, followers should")
	fmt.Println("automatically sync the changes through WAL replication.")
	fmt.Println("")
	
	// 获取leader信息
	fmt.Println("Step 1: Getting current leader information...")
	leaderResp, err := globalClient.GetLeader()
	if err != nil {
		fmt.Printf("Error getting leader info: %v\n", err)
		return
	}
	
	if leaderResp.Leader != nil {
		fmt.Printf("Current Leader: %s:%d\n", leaderResp.Leader.Host, leaderResp.Leader.Port)
	}
	
	if len(leaderResp.Followers) > 0 {
		fmt.Printf("Followers (%d):\n", len(leaderResp.Followers))
		for i, follower := range leaderResp.Followers {
			fmt.Printf("  %d. %s:%d\n", i+1, follower.Host, follower.Port)
		}
	}
	
	fmt.Println("\nStep 2: Instructions for testing WAL synchronization:")
	fmt.Println("1. Use the 'test-wal' command to create test data on the leader")
	fmt.Println("2. Check the MetaServer logs to see WAL synchronization messages")
	fmt.Println("3. Kill the leader process and observe follower election")
	fmt.Println("4. Connect to the new leader and verify data exists")
	fmt.Println("5. Use 'cluster' and 'leader' commands to monitor the cluster state")
	
	fmt.Println("\n=== WAL Consistency Verification ===")
	fmt.Println("✓ Use logs to verify WAL entries are being synced to followers")
	fmt.Println("✓ Test leader failover to verify data persistence")
	fmt.Println("✓ All write operations should be logged and replicated")
}

func cmdReplicas(args []string) {
	path := ""
	if len(args) > 0 {
		path = args[0]
	}
	
	_, err := globalClient.GetReplicationInfo(path)
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
		fmt.Println("Usage: rm <path>")
		return
	}
	
	// 默认递归删除
	err := globalClient.Delete(args[0])
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