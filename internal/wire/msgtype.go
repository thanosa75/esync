package wire

// Message types, one per row of the §9.2 table.
const (
	MsgSessionParams     MsgType = 0x01
	MsgSessionReady      MsgType = 0x02
	MsgScanProgress      MsgType = 0x10
	MsgScanComplete      MsgType = 0x11
	MsgGroupManifest     MsgType = 0x12
	MsgGroupDecision     MsgType = 0x20
	MsgCredit            MsgType = 0x21
	MsgFileRequest       MsgType = 0x30
	MsgFileHeader        MsgType = 0x31
	MsgFileChunk         MsgType = 0x32
	MsgFileComplete      MsgType = 0x33
	MsgFileError         MsgType = 0x34
	MsgFileCancel        MsgType = 0x35
	MsgSessionSummary    MsgType = 0x40
	MsgSessionSummaryAck MsgType = 0x41
	MsgError             MsgType = 0x50
	MsgPing              MsgType = 0x60
	MsgPong              MsgType = 0x61
	MsgSessionResume     MsgType = 0x70
)

var msgTypeNames = map[MsgType]string{
	MsgSessionParams:     "SESSION_PARAMS",
	MsgSessionReady:      "SESSION_READY",
	MsgScanProgress:      "SCAN_PROGRESS",
	MsgScanComplete:      "SCAN_COMPLETE",
	MsgGroupManifest:     "GROUP_MANIFEST",
	MsgGroupDecision:     "GROUP_DECISION",
	MsgCredit:            "CREDIT",
	MsgFileRequest:       "FILE_REQUEST",
	MsgFileHeader:        "FILE_HEADER",
	MsgFileChunk:         "FILE_CHUNK",
	MsgFileComplete:      "FILE_COMPLETE",
	MsgFileError:         "FILE_ERROR",
	MsgFileCancel:        "FILE_CANCEL",
	MsgSessionSummary:    "SESSION_SUMMARY",
	MsgSessionSummaryAck: "SESSION_SUMMARY_ACK",
	MsgError:             "ERROR",
	MsgPing:              "PING",
	MsgPong:              "PONG",
	MsgSessionResume:     "SESSION_RESUME",
}

func (t MsgType) String() string {
	if n, ok := msgTypeNames[t]; ok {
		return n
	}
	return "UNKNOWN"
}
