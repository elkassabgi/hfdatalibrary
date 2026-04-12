// pcap_extract — Extract trade reports from IEX HIST TOPS pcap/pcapng files.
//
// IEX HIST files use pcapng (Next Generation) format, identified by magic
// number 0x0a0d0d0a. The packet data inside Enhanced Packet Blocks contains
// raw IEX-TP segments (no Ethernet/IP/UDP encapsulation).
//
// This program parses the pcapng framing, extracts IEX-TP segments,
// finds TOPS Trade Report messages (type 'T' = 0x54), filters to a
// specified ticker universe, and outputs a CSV of trades.
//
// Output CSV columns: symbol,timestamp_ns,price,size,trade_id
//
// Usage:
//   pcap_extract -input FILE.pcap.gz -tickers tickers.json -output trades.csv
//
// Author: Ahmed Elkassabgi / HF Data Library
// License: CC BY 4.0

package main

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	// pcapng block types
	sectionHeaderBlock   = 0x0A0D0D0A
	interfaceDescBlock   = 0x00000001
	enhancedPacketBlock  = 0x00000006
	simplePacketBlock    = 0x00000003

	// Network header sizes (to skip in pcapng Enhanced Packet Blocks)
	ethernetHeaderSize = 14
	ipHeaderMinSize    = 20
	udpHeaderSize      = 8

	// IEX-TP
	iexTPHeaderSize = 40
	topsProtocolID  = 0x8003
	tradeReportType = 0x54 // 'T'
	tradeReportSize = 38

	// Classic pcap
	pcapMagicMicro = 0xA1B2C3D4
	pcapMagicNano  = 0xA1B23C4D
)

func trimSymbol(sym [8]byte) string {
	n := 8
	for n > 0 && (sym[n-1] == ' ' || sym[n-1] == 0) {
		n--
	}
	return string(sym[:n])
}

// parseIEXTPSegment parses one IEX-TP segment and extracts trade reports.
func parseIEXTPSegment(data []byte, universe map[string]bool, out *os.File,
	totalMessages, totalTrades, filteredTrades *uint64) {

	if len(data) < iexTPHeaderSize {
		return
	}

	version := data[0]
	protocolID := binary.LittleEndian.Uint16(data[2:4])
	msgCount := binary.LittleEndian.Uint16(data[14:16])

	if version != 1 || protocolID != topsProtocolID || msgCount == 0 {
		return
	}

	payloadLen := binary.LittleEndian.Uint16(data[12:14])
	offset := iexTPHeaderSize
	end := iexTPHeaderSize + int(payloadLen)
	if end > len(data) {
		end = len(data)
	}

	for i := 0; i < int(msgCount) && offset < end; i++ {
		if offset+2 > end {
			break
		}
		msgLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if msgLen == 0 || offset+msgLen > end {
			break
		}

		*totalMessages++

		if msgLen >= tradeReportSize && data[offset] == tradeReportType {
			*totalTrades++

			timestampNs := int64(binary.LittleEndian.Uint64(data[offset+2 : offset+10]))
			var sym [8]byte
			copy(sym[:], data[offset+10:offset+18])
			size := binary.LittleEndian.Uint32(data[offset+18 : offset+22])
			priceFixed := int64(binary.LittleEndian.Uint64(data[offset+22 : offset+30]))
			tradeID := int64(binary.LittleEndian.Uint64(data[offset+30 : offset+38]))

			symbol := trimSymbol(sym)
			if len(universe) == 0 || universe[symbol] {
				*filteredTrades++
				price := float64(priceFixed) / 10000.0
				fmt.Fprintf(out, "%s,%d,%.4f,%d,%d\n",
					symbol, timestampNs, price, size, tradeID)
			}
		}

		offset += msgLen
	}
}

// processPcapng reads a pcapng stream and processes IEX-TP segments from Enhanced Packet Blocks.
func processPcapng(reader io.Reader, universe map[string]bool, out *os.File) (
	totalPackets, totalMessages, totalTrades, filteredTrades uint64, err error) {

	startTime := time.Now()
	lastReport := startTime

	for {
		// Read block type (4 bytes) and block total length (4 bytes)
		var blockType uint32
		var blockLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &blockType); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return totalPackets, totalMessages, totalTrades, filteredTrades, nil
			}
			return totalPackets, totalMessages, totalTrades, filteredTrades, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blockLen); err != nil {
			return totalPackets, totalMessages, totalTrades, filteredTrades, err
		}

		// Block body = blockLen - 12 (4 type + 4 len + 4 trailing len)
		if blockLen < 12 {
			// Invalid block, try to skip
			continue
		}
		bodyLen := int(blockLen) - 12

		switch blockType {
		case sectionHeaderBlock:
			// SHB: skip the body (contains byte order magic, version, etc.)
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return totalPackets, totalMessages, totalTrades, filteredTrades, err
			}

		case interfaceDescBlock:
			// IDB: skip
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return totalPackets, totalMessages, totalTrades, filteredTrades, err
			}

		case enhancedPacketBlock:
			// EPB: interface_id(4) + ts_high(4) + ts_low(4) + captured_len(4) + orig_len(4) = 20 bytes header
			// Then captured_len bytes of packet data (padded to 4 bytes), then options
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return totalPackets, totalMessages, totalTrades, filteredTrades, err
			}

			if len(body) < 20 {
				break
			}
			capturedLen := binary.LittleEndian.Uint32(body[12:16])
			packetData := body[20:]
			if int(capturedLen) < len(packetData) {
				packetData = packetData[:capturedLen]
			}

			totalPackets++

			// The packet data contains the full captured network frame.
			// Strip Ethernet + IP + UDP headers to get the IEX-TP payload.
			//
			// Ethernet (14 bytes): dst(6) + src(6) + ethertype(2)
			// IP (20+ bytes): version/ihl(1) + ... ; actual size = (ihl & 0x0f) * 4
			// UDP (8 bytes): src_port(2) + dst_port(2) + length(2) + checksum(2)
			//
			// After stripping these, the remaining bytes are the IEX-TP segment.
			minHeaders := ethernetHeaderSize + ipHeaderMinSize + udpHeaderSize // 42 bytes
			if len(packetData) < minHeaders+iexTPHeaderSize {
				break
			}

			// Get actual IP header length (first nibble of the byte at offset 14)
			ipVersionIHL := packetData[ethernetHeaderSize]
			ipHeaderLen := int(ipVersionIHL&0x0f) * 4
			if ipHeaderLen < ipHeaderMinSize {
				ipHeaderLen = ipHeaderMinSize
			}

			iexTPOffset := ethernetHeaderSize + ipHeaderLen + udpHeaderSize
			if iexTPOffset >= len(packetData) {
				break
			}

			iexTPData := packetData[iexTPOffset:]
			parseIEXTPSegment(iexTPData, universe, out, &totalMessages, &totalTrades, &filteredTrades)

			// Progress
			now := time.Now()
			if now.Sub(lastReport) > 10*time.Second {
				elapsed := now.Sub(startTime).Seconds()
				fmt.Fprintf(os.Stderr, "[%5.0fs] packets=%d messages=%d trades=%d filtered=%d\n",
					elapsed, totalPackets, totalMessages, totalTrades, filteredTrades)
				lastReport = now
			}

		default:
			// Unknown block type, skip
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return totalPackets, totalMessages, totalTrades, filteredTrades, err
			}
		}

		// Read trailing block length (4 bytes)
		var trailingLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &trailingLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return totalPackets, totalMessages, totalTrades, filteredTrades, nil
			}
			return totalPackets, totalMessages, totalTrades, filteredTrades, err
		}
	}
}

// processClassicPcap reads a classic libpcap stream.
func processClassicPcap(reader io.Reader, universe map[string]bool, out *os.File) (
	totalPackets, totalMessages, totalTrades, filteredTrades uint64, err error) {

	// Skip remaining 20 bytes of global header (we already read the 4-byte magic)
	skip := make([]byte, 20)
	if _, err := io.ReadFull(reader, skip); err != nil {
		return 0, 0, 0, 0, err
	}

	startTime := time.Now()
	lastReport := startTime

	for {
		// Record header: ts_sec(4) + ts_usec(4) + incl_len(4) + orig_len(4) = 16 bytes
		var tsSec, tsUsec, inclLen, origLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &tsSec); err != nil {
			if err == io.EOF {
				break
			}
			return totalPackets, totalMessages, totalTrades, filteredTrades, err
		}
		binary.Read(reader, binary.LittleEndian, &tsUsec)
		binary.Read(reader, binary.LittleEndian, &inclLen)
		binary.Read(reader, binary.LittleEndian, &origLen)
		_ = origLen

		packetData := make([]byte, inclLen)
		if _, err := io.ReadFull(reader, packetData); err != nil {
			break
		}

		totalPackets++

		// Strip Ethernet + IP + UDP headers (same as pcapng)
		minHeaders := ethernetHeaderSize + ipHeaderMinSize + udpHeaderSize
		if len(packetData) < minHeaders+iexTPHeaderSize {
			continue
		}
		ipVersionIHL := packetData[ethernetHeaderSize]
		ipHeaderLen := int(ipVersionIHL&0x0f) * 4
		if ipHeaderLen < ipHeaderMinSize {
			ipHeaderLen = ipHeaderMinSize
		}
		iexTPOffset := ethernetHeaderSize + ipHeaderLen + udpHeaderSize
		if iexTPOffset >= len(packetData) {
			continue
		}
		parseIEXTPSegment(packetData[iexTPOffset:], universe, out, &totalMessages, &totalTrades, &filteredTrades)

		now := time.Now()
		if now.Sub(lastReport) > 10*time.Second {
			elapsed := now.Sub(startTime).Seconds()
			fmt.Fprintf(os.Stderr, "[%5.0fs] packets=%d messages=%d trades=%d filtered=%d\n",
				elapsed, totalPackets, totalMessages, totalTrades, filteredTrades)
			lastReport = now
		}
	}
	return totalPackets, totalMessages, totalTrades, filteredTrades, nil
}

func main() {
	inputFile := flag.String("input", "", "Path to IEX HIST pcap.gz or pcapng.gz file")
	tickersFile := flag.String("tickers", "", "Path to tickers.json (universe filter)")
	outputFile := flag.String("output", "trades.csv", "Output CSV path")
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "Usage: pcap_extract -input FILE.pcap.gz -tickers tickers.json -output trades.csv")
		os.Exit(1)
	}

	// Load ticker universe
	universe := make(map[string]bool)
	if *tickersFile != "" {
		data, err := os.ReadFile(*tickersFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading tickers file: %v\n", err)
			os.Exit(1)
		}
		var tickers []string
		if err := json.Unmarshal(data, &tickers); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing tickers JSON: %v\n", err)
			os.Exit(1)
		}
		for _, t := range tickers {
			universe[t] = true
		}
		fmt.Fprintf(os.Stderr, "Loaded %d tickers in universe\n", len(universe))
	}

	// Open input
	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Try gzip decompression
	var reader io.Reader
	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Seek(0, io.SeekStart)
		reader = f
	} else {
		defer gz.Close()
		reader = gz
	}

	// Open output
	out, err := os.Create(*outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()
	fmt.Fprintln(out, "symbol,timestamp_ns,price,size,trade_id")

	// Read first 4 bytes to determine format
	var magic uint32
	if err := binary.Read(reader, binary.LittleEndian, &magic); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file magic: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()
	var totalPackets, totalMessages, totalTrades, filteredTrades uint64

	switch magic {
	case sectionHeaderBlock:
		// pcapng format (0x0A0D0D0A)
		fmt.Fprintln(os.Stderr, "Detected pcapng format")
		// We already consumed the block type (4 bytes). Read the rest of the SHB.
		var blockLen uint32
		binary.Read(reader, binary.LittleEndian, &blockLen)
		if blockLen >= 12 {
			body := make([]byte, blockLen-12)
			io.ReadFull(reader, body)
			// Read trailing length
			var trailing uint32
			binary.Read(reader, binary.LittleEndian, &trailing)
		}
		totalPackets, totalMessages, totalTrades, filteredTrades, err = processPcapng(reader, universe, out)

	case pcapMagicMicro, pcapMagicNano:
		// Classic pcap format
		fmt.Fprintln(os.Stderr, "Detected classic pcap format")
		totalPackets, totalMessages, totalTrades, filteredTrades, err = processClassicPcap(reader, universe, out)

	default:
		fmt.Fprintf(os.Stderr, "Unknown file magic: 0x%08X\n", magic)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse error: %v\n", err)
	}

	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\nDone in %.1f seconds\n", elapsed.Seconds())
	fmt.Fprintf(os.Stderr, "  Packets:         %d\n", totalPackets)
	fmt.Fprintf(os.Stderr, "  Messages:        %d\n", totalMessages)
	fmt.Fprintf(os.Stderr, "  Trade reports:   %d\n", totalTrades)
	fmt.Fprintf(os.Stderr, "  Filtered trades: %d (in universe)\n", filteredTrades)
	fmt.Fprintf(os.Stderr, "  Output:          %s\n", *outputFile)
}
