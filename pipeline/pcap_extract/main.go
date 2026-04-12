// pcap_extract — Extract trade reports from IEX HIST TOPS pcap.gz files.
//
// This program reads an IEX HIST pcap.gz file (which uses a non-standard
// pcap variant specific to IEX), parses IEX-TP transport segments and
// TOPS trade report messages, filters to a specified ticker universe,
// and outputs a simple CSV of trades.
//
// The output CSV has columns:
//   symbol,timestamp_ns,price,size,trade_id
//
// Usage:
//   pcap_extract -input FILE.pcap.gz -tickers tickers.json -output trades.csv
//
// Binary format references:
//   IEX-TP transport: version(1) + reserved(1) + protocol(2) + channel(4) +
//     session(4) + payload_len(2) + msg_count(2) + stream_offset(8) +
//     first_seq(8) + send_time(8) = 40 bytes
//   TOPS Trade Report (type 'T' = 0x54):
//     msg_type(1) + flags(1) + timestamp(8) + symbol(8) + size(4) +
//     price(8) + trade_id(8) = 38 bytes
//
// The IEX HIST "pcap" format is NOT standard libpcap. It uses:
//   Global header: identical to libpcap (24 bytes) but may have a different
//     magic number (0xa1b2c3d4 or 0xa1b23c4d for nanosecond pcap).
//   Per-packet: 16-byte record header (ts_sec, ts_usec, incl_len, orig_len)
//     followed by raw IEX-TP segment data (NO Ethernet/IP/UDP headers).
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

// IEX-TP segment header (40 bytes, little-endian)
type iexTPHeader struct {
	Version        uint8
	Reserved       uint8
	ProtocolID     uint16
	ChannelID      uint32
	SessionID      uint32
	PayloadLength  uint16
	MessageCount   uint16
	StreamOffset   int64
	FirstSeqNum    int64
	SendTime       int64
}

const (
	iexTPHeaderSize = 40
	topsProtocolID  = 0x8003
	tradeReportType = 0x54 // 'T'
	tradeReportSize = 38
)

// TOPS Trade Report Message (38 bytes)
type tradeReport struct {
	MsgType     uint8
	Flags       uint8
	TimestampNs int64
	Symbol      [8]byte
	Size        uint32
	Price       int64 // fixed-point, divide by 10000
	TradeID     int64
}

// pcap global header (24 bytes)
type pcapGlobalHeader struct {
	MagicNumber  uint32
	VersionMajor uint16
	VersionMinor uint16
	ThisZone     int32
	SigFigs      uint32
	SnapLen      uint32
	Network      uint32
}

// pcap record header (16 bytes)
type pcapRecordHeader struct {
	TsSec   uint32
	TsUsec  uint32
	InclLen uint32
	OrigLen uint32
}

func trimSymbol(sym [8]byte) string {
	n := 8
	for n > 0 && (sym[n-1] == ' ' || sym[n-1] == 0) {
		n--
	}
	return string(sym[:n])
}

func main() {
	inputFile := flag.String("input", "", "Path to IEX HIST pcap.gz file")
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

	// Open input file
	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Wrap in gzip reader if needed
	var reader io.Reader
	gz, err := gzip.NewReader(f)
	if err != nil {
		// Not gzipped, try raw
		f.Seek(0, io.SeekStart)
		reader = f
	} else {
		defer gz.Close()
		reader = gz
	}

	// Read pcap global header (24 bytes)
	var globalHdr pcapGlobalHeader
	if err := binary.Read(reader, binary.LittleEndian, &globalHdr); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading pcap global header: %v\n", err)
		os.Exit(1)
	}

	// Validate magic number
	isNano := false
	switch globalHdr.MagicNumber {
	case 0xa1b2c3d4:
		// Standard microsecond pcap
	case 0xa1b23c4d:
		// Nanosecond pcap
		isNano = true
	default:
		fmt.Fprintf(os.Stderr, "Unknown pcap magic: 0x%08x (trying anyway)\n", globalHdr.MagicNumber)
	}
	_ = isNano // We don't use the pcap timestamp, we use IEX's own nanosecond timestamps

	// Open output file
	out, err := os.Create(*outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()
	fmt.Fprintln(out, "symbol,timestamp_ns,price,size,trade_id")

	// Stats
	var totalPackets, totalMessages, totalTrades, filteredTrades uint64
	startTime := time.Now()
	lastReport := startTime

	// Read packets
	for {
		// Read pcap record header (16 bytes)
		var recHdr pcapRecordHeader
		if err := binary.Read(reader, binary.LittleEndian, &recHdr); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading record header: %v\n", err)
			break
		}

		totalPackets++

		// Read the packet data
		packetData := make([]byte, recHdr.InclLen)
		if _, err := io.ReadFull(reader, packetData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading packet data: %v\n", err)
			break
		}

		// The packet data IS the IEX-TP segment directly (no Ethernet/IP/UDP headers)
		if len(packetData) < iexTPHeaderSize {
			continue
		}

		// Parse IEX-TP header
		var hdr iexTPHeader
		hdr.Version = packetData[0]
		hdr.Reserved = packetData[1]
		hdr.ProtocolID = binary.LittleEndian.Uint16(packetData[2:4])
		hdr.ChannelID = binary.LittleEndian.Uint32(packetData[4:8])
		hdr.SessionID = binary.LittleEndian.Uint32(packetData[8:12])
		hdr.PayloadLength = binary.LittleEndian.Uint16(packetData[12:14])
		hdr.MessageCount = binary.LittleEndian.Uint16(packetData[14:16])

		if hdr.Version != 1 || hdr.ProtocolID != topsProtocolID || hdr.MessageCount == 0 {
			continue
		}

		// Parse messages in the payload
		offset := iexTPHeaderSize
		payloadEnd := iexTPHeaderSize + int(hdr.PayloadLength)
		if payloadEnd > len(packetData) {
			payloadEnd = len(packetData)
		}

		for i := 0; i < int(hdr.MessageCount) && offset < payloadEnd; i++ {
			// Each message: 2-byte length prefix + message body
			if offset+2 > payloadEnd {
				break
			}
			msgLen := int(binary.LittleEndian.Uint16(packetData[offset : offset+2]))
			offset += 2

			if msgLen == 0 || offset+msgLen > payloadEnd {
				break
			}

			totalMessages++

			// Check message type (first byte)
			if msgLen >= tradeReportSize && packetData[offset] == tradeReportType {
				totalTrades++

				// Parse trade report
				var tr tradeReport
				tr.MsgType = packetData[offset]
				tr.Flags = packetData[offset+1]
				tr.TimestampNs = int64(binary.LittleEndian.Uint64(packetData[offset+2 : offset+10]))
				copy(tr.Symbol[:], packetData[offset+10:offset+18])
				tr.Size = binary.LittleEndian.Uint32(packetData[offset+18 : offset+22])
				tr.Price = int64(binary.LittleEndian.Uint64(packetData[offset+22 : offset+30]))
				tr.TradeID = int64(binary.LittleEndian.Uint64(packetData[offset+30 : offset+38]))

				symbol := trimSymbol(tr.Symbol)
				if len(universe) == 0 || universe[symbol] {
					filteredTrades++
					price := float64(tr.Price) / 10000.0
					fmt.Fprintf(out, "%s,%d,%.4f,%d,%d\n",
						symbol, tr.TimestampNs, price, tr.Size, tr.TradeID)
				}
			}

			offset += msgLen
		}

		// Progress reporting every 10 seconds
		now := time.Now()
		if now.Sub(lastReport) > 10*time.Second {
			elapsed := now.Sub(startTime).Seconds()
			fmt.Fprintf(os.Stderr, "[%5.0fs] packets=%d messages=%d trades=%d filtered=%d\n",
				elapsed, totalPackets, totalMessages, totalTrades, filteredTrades)
			lastReport = now
		}
	}

	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\nDone in %.1f seconds\n", elapsed.Seconds())
	fmt.Fprintf(os.Stderr, "  Packets:         %d\n", totalPackets)
	fmt.Fprintf(os.Stderr, "  Messages:        %d\n", totalMessages)
	fmt.Fprintf(os.Stderr, "  Trade reports:   %d\n", totalTrades)
	fmt.Fprintf(os.Stderr, "  Filtered trades: %d (in universe)\n", filteredTrades)
	fmt.Fprintf(os.Stderr, "  Output:          %s\n", *outputFile)
}
