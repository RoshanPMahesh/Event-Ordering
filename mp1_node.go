// TODO: Multiple exactly same transactions - remove from received when priority agreed upon - msg id (lamport) for consecutive msgs
// TODO: ISIS Algorithm tie-breaking (assigning priority ids to nodes)
// TODO: Deal with failures (what happens to the message and its proposed priority)
// TODO: Invalid transactions - if the transaction is invalid is it still "processed", i.e. we print account information?
// TODO: Multiple transactions in a row at the beginning - didn't register the 2nd one
// TODO: Missing 1-2 transactions at the beginning
// TODO: Validate that if process 1's transaction is priority 3 and the other processes priorities are priority 2, then the final priority is 3
    // If there's an error, then there's an issue with the sent[key/message] and how it's being iterated in diffmessages

    package main

import (
	"bufio"
	"fmt"
	"strconv"

	//"io"
	"net"
	"os"

	"strings"
	"sync"

	//"reflect"

	"container/heap"
	"container/list"
	"sort"
	"time"
    // "math"

    // "encoding/csv" // Comment out for graphs
)

type Node struct {
    name string
    hostname string
    port_num string
    connection net.Conn
    // host bool
    // pq_index int
}

type pair struct {
    val1 int
    val2 int
}

var me string = ""
var total_nodes int = 1
var transaction_id int = 0

var pq_index map[string]int = make(map[string]int)
var configs []Node = make([]Node, 0)
var listening []net.Conn = make([]net.Conn, 0)

var connections map[string]net.Conn = make(map[string]net.Conn)

var received map[string]int = make(map[string]int)
var pq PriorityQueue
var priority int = 0
var sent map[string]*list.List = make(map[string]*list.List)

var accounts map[string]int = make(map[string]int) // non-negative integer balance

var NodesLock sync.RWMutex
var sending sync.RWMutex
var receivedLock sync.RWMutex
var priorityLock sync.RWMutex
var totalLock sync.RWMutex

// Comment out for graphs
/* // logging for graphs
var start_time = 0.0
var time_file *os.File = nil
var time_writer *csv.Writer = nil
var time_err error = nil
*/

func bmulticast(message string) {
    
    split := strings.Split(message, "\t")
    // var our_message bool
    if (len(split) == 3 && split[0] == me) { // sending out transaction
        receivedLock.Lock()
        received[message] = 1 // self-deliver
        receivedLock.Unlock()

        var text Message
        NodesLock.Lock()
        text.msg = message
        text.sender = pq_index[me]
        priorityLock.Lock()
        text.priority = priority+1
        priorityLock.Unlock()
        text.deliverable = false
        text.init_time = float64(time.Now().Unix())
        NodesLock.Unlock()

        priorityLock.Lock()
        heap.Push(&pq, text)
        priority += 1
        priorityLock.Unlock()
        
        sending.Lock()
        sent[message] = list.New()
        priorityLock.Lock()
        var order pair
        order.val1 = priority
        order.val2 = pq_index[me]
        // sent[message].PushBack(float64(priority) + (float64(pq_index[me]) / 10.0)) // set proposed priority - the priority for node the event was generated on 
        sent[message].PushBack(order)
        priorityLock.Unlock()
        sending.Unlock()

        for idx:=1; idx < len(configs); idx++ {
            fmt.Fprintf(configs[idx].connection, message) 
        }

        return
    } 

    for idx:=1; idx < len(configs); idx++ {
        fmt.Fprintf(configs[idx].connection, message) 
    }

    if len(split) == 6 && split[3] == me { // sending out final priority
        receivedLock.Lock()
        // is the received code below needed? the message is the full string
        // with len of 4. the received list in the other if statement has a
        // diff message
        received[message] = 1 // self-deliver
        receivedLock.Unlock()
        transaction := split[5]
        t_id := split[4]
        transaction = me + "\t" + t_id + "\t" + transaction
        p, _ := strconv.Atoi(split[1])
        process_num, _ := strconv.Atoi(split[2])

        priorityLock.Lock()
        for idx:=0; idx < len(pq); idx++ {
            if pq[idx].msg == transaction && pq[idx].deliverable == false {
                pq[idx].priority = p
                pq[idx].sender = process_num
                pq[idx].deliverable = true
                heap.Fix(&pq, idx)
                break
            }
        }
        checkqueue()
        priorityLock.Unlock()

        priorityLock.Lock()
        if p > priority {
            priority = p
        }
        priorityLock.Unlock()
        
    }
}

func rmulticast(connection net.Conn, message string) {
    receivedLock.RLock()
    _, err := received[message]
    receivedLock.RUnlock()
    if err == false { // false if key is not present in the map
        // fmt.Fprintf(os.Stdout, message + " false in rmulticast\n")
        //fmt.Fprintf(os.Stderr, message)

        //diffmessLock.Lock()
        msg_type := diffmessages(connection, message)
        //diffmessLock.Unlock()

        // what does this do?
        if msg_type != 3 { // if not a message meant only for us
            receivedLock.Lock()
            received[message] = 1
            receivedLock.Unlock()

            bmulticast(message)
            
        }
    }
    
}

func checkqueue() {
    //fmt.Println(len(pq))
    //priorityLock.Lock()

    // for {
    //     if pq.Top() == nil {
    //         break
    //     }

    //     top := pq.Top().(Message)
    //     fmt.Fprintf(os.Stdout, strconv.Itoa(top.priority) + "\t" + strconv.Itoa(top.sender) + "\t" + top.msg + "\n")
    //     if top.deliverable == false {
    //         split := strings.Split(top.msg, "\t")
    //         if split[0] == me {
    //             break
    //         }
    //         current_time := float64(time.Now().Unix())
    //         if current_time - top.init_time > 15 {
    //             pq.Pop() // sender node has failed, do not deliver message
    //             continue
    //         }
    //         break
    //     }

    //     transaction_message := pq.Pop().(Message).msg
    //     passing_message := string(transaction_message)
    //     processtransaction(passing_message)
    // }


    // for idx:=0; idx < len(pq); idx++ {
    //     if pq[idx].deliverable == false {
    //         break // cannot deliver this message or any after it
    //     }
    //     message := pq[idx].msg
    //     transaction := strings.Split(message, "\t")[1]
    //     heap.Pop(&pq)
    //     idx-- // because we popped - is there any need since len(pq) would subtract by 1
    //     processtransaction(transaction)
    // }

    for idx:=0; idx < len(pq); idx++ {
        if pq[idx].deliverable == false {
            split := strings.Split(pq[idx].msg, "\t")
            if split[0] == me {
                return // I'm alive so wait for me to send final priority
            }
            current_time := float64(time.Now().Unix())
            if current_time - pq[idx].init_time > 15 {
                heap.Pop(&pq) // sender node has failed, do not deliver message
                idx--
                continue
            }
            break // cannot deliver this message or any after it
        }
        // text := pq[idx]
        message := pq[idx].msg
        // transaction := strings.Split(message, "\t")[2]
        heap.Pop(&pq)
        idx-- // because we popped - is there any need since len(pq) would subtract by 1
        processtransaction(message)
    }
}

// final/(none) priority proposing_node_num node_name transaction_id transaction
func diffmessages(connection net.Conn, message string) int {
    split := strings.Split(message, "\t")
    if len(split) == 6 {
        // received the final priority
        //fmt.Fprintf(os.Stderr, message)
        transaction := split[5]
        t_id := split[4]
        transaction = split[3] + "\t" + t_id + "\t" + transaction
        // p, _ := strconv.ParseFloat(split[1], 64)

        // num_int, num_frac := math.Modf(p)
        // final_priority := int(num_int)
        // final_tiebreaker := int(num_frac * 10)

        p, _ := strconv.Atoi(split[1])
        process_num, _ := strconv.Atoi(split[2])

        priorityLock.Lock()
        for idx:=0; idx < len(pq); idx++ {
            if pq[idx].msg == transaction && pq[idx].deliverable == false {
                pq[idx].priority = p
                pq[idx].sender = process_num
                pq[idx].deliverable = true
                heap.Fix(&pq, idx)
                break
            }
        }
        checkqueue() 
        priorityLock.Unlock()

        priorityLock.Lock()
        if p > priority {
            priority = p
        }
        priorityLock.Unlock()
        
        return 4
    } else if len(split) == 5 { // receive proposed priorities from other nodes, send final priority value
        // priority proposing_node_num node_name transaction_id transaction
        transaction := split[4]
        t_id := split[3]
        node_name := split[2]
        key := node_name + "\t" + t_id + "\t" + transaction
        // p, _ := strconv.ParseFloat(split[0], 64) //priority.node# in float64 format
        p, _ := strconv.Atoi(split[0])
        process_num, _ := strconv.Atoi(split[1])

        sending.Lock()
        
        var order pair
        order.val1 = p
        order.val2 = process_num
        sent[key].PushBack(order)
        // sent[key].PushBack(float64(p) + (float64(process_num) / 10.0))

        totalLock.Lock()
        if sent[key].Len() == total_nodes { // received all priorities
            l := sent[key]
            max := l.Front().Value.(pair)
            for e := l.Front(); e != nil; e = e.Next() {
                cmp := e.Value.(pair)
                // fmt.Fprintf(os.Stdout, strconv.Itoa(cmp.val1) + " " + strconv.Itoa(cmp.val2) + "\n")
                if cmp.val1 > max.val1 { // type cast back to int
                    max = cmp
                } else if cmp.val1 == max.val1 {
                    if cmp.val2 > max.val2 {
                        max = cmp
                    }
                }
            }
            // num_int, num_frac := math.Modf(max)
            // final_priority := int(num_int)
            // final_tiebreaker := int(num_frac * 10)
            // fmt.Fprintf(os.Stdout, strconv.Itoa(max.val1) + "\t" + strconv.Itoa(max.val2) + "\n")

            // time.Sleep(5 * time.Second) // testing node failure
            bmulticast("final\t" + strconv.Itoa(max.val1) + "\t" + strconv.Itoa(max.val2) + "\t" + key)
        }
        totalLock.Unlock()
        sending.Unlock()

        return 3

    } else if len(split) == 3 { // received message from another node, send proposed priority
        // node_name transaction_id transaction

        // curr_nodes := strings.TrimLeft(configs[0].name, "node")
        // nodes_num, _ := strconv.Atoi(curr_nodes)
        // attachingSenders := float64(nodes_num) * 0.1

        var text Message
        NodesLock.Lock()
        text.msg = message
        text.sender = pq_index[me]
        priorityLock.Lock()
        text.priority = priority+1
        priorityLock.Unlock()
        text.deliverable = false
        text.init_time = float64(time.Now().Unix())
        NodesLock.Unlock()

        priorityLock.Lock()
        heap.Push(&pq, text)
        priority += 1
        // p := float64(priority) + attachingSenders

        //fmt.Fprintf(os.Stderr, "Sending proposed priority to " + split[0] + " " + strconv.FormatFloat(p, 'f', 1, 64) + "\n")
        // strconv.FormatFloat(p, 'f', 1, 64)
        fmt.Fprintf(connections[split[0]], strconv.Itoa(priority) + "\t" + strconv.Itoa(pq_index[me]) + "\t" + message) // send proposed priority to node that originally multicast
        priorityLock.Unlock()
        
        return 2
    }
    return 1
}

func initiateConnections() {
    //defer waiting.Done()
    for idx:=1; idx < len(configs); idx++ {
        // if configs[idx].connection != nil {
        //     continue
        // }
        var err error = nil
        configs[idx].connection, err = net.Dial("tcp", configs[idx].hostname + ":" + configs[idx].port_num)
        for err != nil {
            configs[idx].connection, err = net.Dial("tcp", configs[idx].hostname + ":" + configs[idx].port_num)
            //fmt.Println("STUCK HERE")
        }
        
        // sending this message for debugging purposes
        // fmt.Fprintf(configs[idx].connection, "from this node: " + configs[0].name + " to this node: " + configs[idx].name + "\n")   
    }
}

func receiveConnections() {
    server, err := net.Listen("tcp", ":" + configs[0].port_num)
    if err != nil {
        fmt.Fprintf(os.Stderr, "No connection from other nodes")
    }

    defer server.Close()

    for idx:=0; idx < len(listening); idx++ { // listen for connections
        connection, err := server.Accept()
        if err != nil {
            fmt.Fprintf(os.Stderr, "Client didn't connect")
        }

        listening[idx] = connection

        // reader := bufio.NewReader(connection)
        // outputs, err := reader.ReadString('\n')
        // if err != nil {
        //     fmt.Fprintf(os.Stderr, "Didn't connect properly")
        // }

        // fmt.Fprintf(os.Stdout, outputs) // debugging purposes
        // reader := bufio.NewReader(connection)
	    // bytes_read, err := reader.ReadString('\n')
        // fmt.Fprintf(os.Stderr, bytes_read)
    }
}

func listen(connection net.Conn) {
    reader := bufio.NewReader(connection)
	for {
		bytes_read, err := reader.ReadString('\n')
		if err != nil {
            // node failed
            totalLock.Lock()
            total_nodes -= 1
            totalLock.Unlock()
			return
		}
        rmulticast(connection, bytes_read) // process message in parallel
    }
}

func main() {
    args := os.Args
	if len(args) != 3 {
        fmt.Fprintf(os.Stderr, "Too many args")
		return
	}

	me = args[1]
	config_file := args[2]

    // reads the entire file
    file, err := os.ReadFile(config_file)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Could not open config file")
    }

    // splits the string containing entire file into an array, each element contains a line in config file
    node_nums := strings.Split(string(file), "\n")
    total_nodes, _ = strconv.Atoi(node_nums[0])  // takes the number of nodes, puts it to an int type

    // multicast to self, do not attempt to make connection with self
   
    configs = make([]Node, total_nodes)  // makes a splice -> for some reason arrays weren't working since you can't use a variable to dictate number of elements
    listening = make([]net.Conn, total_nodes-1)

    NodesLock = sync.RWMutex{}
    sending = sync.RWMutex{}
    receivedLock = sync.RWMutex{}
    priorityLock = sync.RWMutex{}
    totalLock = sync.RWMutex{}
    
    idx := 1
    for i := 1; i <= total_nodes; i++ {
        var config Node
    
        split_str := strings.Split(node_nums[i], " ")
        
        config.name = split_str[0]
        config.hostname = split_str[1]
        config.port_num = split_str[2]
       // config.connection = nil
        // if i == 1 {
        //     config.host = true
        // } else {
        //     config.host = false
        // }

        if config.name == me {
            configs[0] = config
        } else {
            configs[idx] = config
            idx += 1
        }

        pq_index[config.name] = i
    }

    var waiting sync.WaitGroup

    waiting.Add(2)

    go func() {
        defer waiting.Done()
        initiateConnections()
    }()

    // go initiateConnections()

    // waiting.Add(1)

    go func() {
        defer waiting.Done()
        receiveConnections()
    }()

    waiting.Wait()

    // by now, all nodes should be in our multicast group
    // we should be in the multicast group of all nodes

    // time.Sleep(5 * time.Second)

    heap.Init(&pq)

    me = configs[0].name

    for idx:=0; idx < len(configs); idx++ {
        connections[configs[idx].name] = configs[idx].connection
    }

    // Comment out for graphs
    /* time_file, time_err = os.Create("transaction_timings_" + me + ".csv")
	if time_err != nil {
		fmt.Fprintf(os.Stderr, "File not created")
	}
	defer time_file.Close()

	time_writer = csv.NewWriter(time_file)
	defer time_writer.Flush()
    */

    
    for idx:=0; idx < len(listening); idx++ {
        go listen(listening[idx])
    }

    // time.Sleep(5 * time.Second) // time to set up listen

    // Comment out for graphs
    // current_time := time.Now()
    // time_in_seconds := float64(current_time.Unix())
    // time_in_nanoseconds := float64(current_time.Nanosecond()) / 1000000000.0
    // start_time = float64(time_in_seconds) + time_in_nanoseconds

    // read from stdin
    event := bufio.NewReader(os.Stdin)
    for {
		transaction, _ := event.ReadString('\n')
		// fmt.Fprintf(os.Stdout, transaction)

        // Comment out for graphs
        /* // store transaction generation time
        current_time := time.Now()
        time_in_seconds := float64(current_time.Unix())
        time_in_nanoseconds := float64(current_time.Nanosecond()) / 1000000000.0
        total_time := float64(time_in_seconds) + time_in_nanoseconds
        diff_time := total_time - start_time // time since node started
        
        // if diff_time <= 205 { // only write if first 100 seconds
        transaction_val := strings.TrimSuffix(transaction, "\n")
        csv_write := []string{configs[0].name + "\t" + strconv.Itoa(transaction_id) + "\t" + transaction_val, strconv.FormatFloat(diff_time, 'f', 7, 64)}
        time_writer.Write(csv_write)
        time_writer.Flush()
        // }
        */

		bmulticast(configs[0].name + "\t" + strconv.Itoa(transaction_id) + "\t" + transaction)
        transaction_id += 1
	}
    //fmt.Println(configs[0].host)
    //fmt.Println(configs[1].host)
    //fmt.Println(configs[2].host)
}

func deposit(acc string, amt int) {
    _, err := accounts[acc]
    if err == false { // create account
        accounts[acc] = amt
    } else {
        accounts[acc] += amt
    }
}

func withdraw(acc string, amt int) bool {
    _, err := accounts[acc]
    if err == false || (accounts[acc] - amt) < 0 { // invalid transaction
        return false
    } else {
        accounts[acc] -= amt
        return true
    }
}

func transfer(acc1 string, acc2 string, amt int) {
    if withdraw(acc1, amt) {
        deposit(acc2, amt)
    }

    //fmt.Println("DO WE GET HERE")
    // i think we need to do something still if the transaction is invalid
}

// TODO: remove \n at the end of transaction
func processtransaction(transaction string) {
    // transaction = node# \t (Deposit or transfer) -> length of 2 when splitting on /t 
    //fmt.Println(transaction + "here!!!")  
    split := strings.Split(transaction, "\t")
    true_transaction := split[2]
    individual_parts := strings.Split(true_transaction, " ")
    action := individual_parts[0]

    if action == "DEPOSIT" {
        acc := individual_parts[1]
        num_string := strings.TrimSuffix(individual_parts[2], "\n")
        amt, _ := strconv.Atoi(num_string)
        deposit(acc, amt)
    } else {
        acc1 := individual_parts[1]
        acc2 := individual_parts[3]
        num_string := strings.TrimSuffix(individual_parts[4], "\n")
        amt, _ := strconv.Atoi(num_string)
        transfer(acc1, acc2, amt)
    }

    // Comment out for graphs
    /* // store transaction processing time
    current_time := time.Now()
    time_in_seconds := float64(current_time.Unix())
    time_in_nanoseconds := float64(current_time.Nanosecond()) / 1000000000.0
    total_time := float64(time_in_seconds) + time_in_nanoseconds
    diff_time := total_time - start_time // time since node started
    // if diff_time <= 205 { // only write if first 100 seconds
    transaction_val := strings.TrimSuffix(transaction, "\n")
    csv_write := []string{"Process" + "\t" + transaction_val, strconv.FormatFloat(diff_time, 'f', 7, 64)}
    time_writer.Write(csv_write)
    time_writer.Flush()
    // }
    */

    printaccounts()
}

func printaccounts() {
    print := "BALANCES"

    accs := make([]string, 0, len(accounts))
    for acc := range accounts {
        accs = append(accs, acc)
    }
    sort.Strings(accs)

    for _, account_name := range accs {
        print += " " + account_name + ":" + strconv.Itoa(accounts[account_name])
    }

    fmt.Fprintf(os.Stdout, print + "\n")
}