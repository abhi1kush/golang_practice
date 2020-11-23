package main

import (
    "crypto/sha1"
    "fmt"
    "sync"
    "os"
    "io"
)

const BUFFER_SIZE = 10000
const MAX_CONCURRENCY =  20

type Payload struct {
    job_id int
    data []byte
}

type HashResult struct {
    job_id int
    hash []byte
}

func worker(input <-chan Payload, output chan<- HashResult, wg *sync.WaitGroup) {
    defer wg.Done()
    in := <-input
    hash := sha1.New()
    hash.Write(in.data)
    hash_out := hash.Sum(nil)
    hash_result := HashResult{
        job_id : in.job_id,
        hash : hash_out,
    }
    output <- hash_result
}

func main() {
    var input_chan = make(chan Payload, MAX_CONCURRENCY)
    var output_chan = make(chan HashResult, MAX_CONCURRENCY)
    var wg sync.WaitGroup

    file, err := os.Open("input_file")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()

    fileinfo, err := file.Stat()
    if err != nil {
        fmt.Println(err)
        return
    }

    filesize := int(fileinfo.Size())
    // Divide file in 1000 blocks.
    parts := int(filesize / 1000)
    buffersize := min(BUFFER_SIZE, parts)
    buffer := make([]byte, buffersize)

    id := 0
    for {
        _, err := file.Read(buffer)
        if err != nil {
            if err != io.EOF {
                fmt.Println(err)
            }
            break
        }

        input := Payload{
            job_id : id,
            data : buffer,
        }

        // This will limit the number of concurrent go rutines 
        // by blocking after reaching the channel capacity
        input_chan <- input

        wg.Add(1)
        go worker(input_chan, output_chan, &wg)
        output := <-output_chan
        fmt.Printf("job_id %d, hash %x \n", output.job_id, output.hash)
        id++
    }
    wg.Wait()
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

