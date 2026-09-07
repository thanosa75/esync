package wire

import "esync/internal/fault"

// encodeBody serialises a message body (no frame header). It cannot fail for a
// known type; an unknown type is E5011.
func encodeBody(m Message) ([]byte, error) {
	w := &writer{}
	switch v := m.(type) {
	case *SessionParams:
		w.str(v.RootName)
		w.u8(v.SourceKind)
		w.u8(v.HashAlg)
		w.u64(v.GroupBytes)
		w.u32(v.Flags)
		w.u8(v.SourcePlatform)
		w.u8(v.PathNorm)
		w.str(v.SenderVersion)
		w.u32(v.MaxChannels)
		w.str(v.FilterSignature)
		w.str(v.SysexcludeTag)
	case *SessionReady:
		w.u16(v.Channels)
		w.u16(v.GroupCredit)
		w.u8(v.PipelineDepth)
		w.u32(v.MaxChunk)
		w.u8(v.DestPlatform)
		w.str(v.ReceiverVersion)
		w.u64(v.DestFreeBytes)
	case *ScanProgress:
		w.u64(v.EntriesSeen)
		w.u64(v.BytesSeen)
		w.u32(v.ErrorsSeen)
	case *ScanComplete:
		w.u64(v.TotalFiles)
		w.u64(v.TotalBytes)
		w.u32(v.TotalGroups)
		w.u32(v.SkippedEntries)
		w.blob(v.ManifestDigest)
	case *GroupManifest:
		w.u32(v.GroupID)
		w.u64(v.FirstFileID)
		w.u16(uint16(len(v.Entries)))
		for i := range v.Entries {
			encodeManifestEntry(w, &v.Entries[i])
		}
	case *GroupDecision:
		w.u32(v.GroupID)
		w.u16(uint16(len(v.Needed)))
		for _, idx := range v.Needed {
			w.u16(idx)
		}
		w.u16(v.SkippedCount)
		w.u16(uint16(len(v.Rejected)))
		for _, rj := range v.Rejected {
			w.u16(rj.Index)
			w.u16(rj.ErrorCode)
		}
	case *Credit:
		w.u16(v.AdditionalGroups)
	case *FileRequest:
		w.u64(v.RequestID)
		w.u64(v.FileID)
		w.u64(v.Offset)
		w.u32(v.MaxChunk)
	case *FileHeader:
		w.u64(v.RequestID)
		w.u64(v.FileID)
		w.u64(v.Size)
		w.u32(v.Mode)
		w.i64(v.MtimeSec)
		w.u32(v.MtimeNsec)
		w.u8(uint8(len(v.Digest)))
		w.raw(v.Digest)
		w.u32(v.ChunkSize)
	case *FileChunk:
		w.u64(v.RequestID)
		w.u64(v.Offset)
		w.u32(uint32(len(v.Data)))
		w.raw(v.Data)
	case *FileComplete:
		w.u64(v.RequestID)
		w.u64(v.BytesSent)
		w.u8(uint8(len(v.DigestFull)))
		w.raw(v.DigestFull)
	case *FileError:
		w.u64(v.RequestID)
		w.u16(v.Code)
		w.u8(v.Retryable)
		w.str(v.Message)
	case *FileCancel:
		w.u64(v.RequestID)
		w.u16(v.Reason)
	case *SessionSummary:
		encodeSummary(w, v.FilesTotal, v.FilesTransferred, v.FilesSkipped, v.FilesFailed,
			v.BytesTransferred, v.GroupsTotal, v.CompletionDigest)
	case *SessionSummaryAck:
		encodeSummary(w, v.FilesTotal, v.FilesTransferred, v.FilesSkipped, v.FilesFailed,
			v.BytesTransferred, v.GroupsTotal, v.CompletionDigest)
	case *Error:
		w.u16(v.Code)
		w.u8(v.Fatal)
		w.str(v.Message)
		w.str(v.Detail)
	case *Ping:
		w.u64(v.Token)
	case *Pong:
		w.u64(v.Token)
	case *SessionResume:
		w.u64(v.SessionID)
		w.blob(v.Nonce)
		w.blob(v.Tag)
		w.u64(v.LastSeqSeen)
	default:
		return nil, fault.Newf(fault.E5011, "encode frame", "", nil, "unknown message type %T", m)
	}
	return w.b, nil
}

func encodeManifestEntry(w *writer, e *ManifestEntry) {
	w.u8(e.EntryType)
	w.u16(e.Flags)
	w.blob(e.Path)
	w.u64(e.Size)
	w.u32(e.Mode)
	w.i64(e.MtimeSec)
	w.u32(e.MtimeNsec)
	w.u8(uint8(len(e.Digest)))
	w.raw(e.Digest)
	if e.EntryType == 2 {
		w.blob(e.LinkTarget)
	}
	if e.Flags&ManifestFlagHasHardlinkKey != 0 {
		w.u64(e.HardlinkKey)
	}
}

func encodeSummary(w *writer, ft, ftr, fs, ff, bt uint64, gt uint32, digest []byte) {
	w.u64(ft)
	w.u64(ftr)
	w.u64(fs)
	w.u64(ff)
	w.u64(bt)
	w.u32(gt)
	w.blob(digest)
}

// decodeBody parses a body by type. Unknown type → E5011; malformed → E5003.
func decodeBody(t MsgType, body []byte) (Message, error) {
	r := &reader{b: body}
	var m Message

	switch t {
	case MsgSessionParams:
		x := &SessionParams{}
		x.RootName = r.str(maxStr)
		x.SourceKind = r.u8()
		x.HashAlg = r.u8()
		x.GroupBytes = r.u64()
		x.Flags = r.u32()
		x.SourcePlatform = r.u8()
		x.PathNorm = r.u8()
		x.SenderVersion = r.str(maxStr)
		x.MaxChannels = r.u32()
		x.FilterSignature = r.str(maxStr)
		x.SysexcludeTag = r.str(maxStr)
		m = x
	case MsgSessionReady:
		x := &SessionReady{}
		x.Channels = r.u16()
		x.GroupCredit = r.u16()
		x.PipelineDepth = r.u8()
		x.MaxChunk = r.u32()
		x.DestPlatform = r.u8()
		x.ReceiverVersion = r.str(maxStr)
		x.DestFreeBytes = r.u64()
		m = x
	case MsgScanProgress:
		x := &ScanProgress{}
		x.EntriesSeen = r.u64()
		x.BytesSeen = r.u64()
		x.ErrorsSeen = r.u32()
		m = x
	case MsgScanComplete:
		x := &ScanComplete{}
		x.TotalFiles = r.u64()
		x.TotalBytes = r.u64()
		x.TotalGroups = r.u32()
		x.SkippedEntries = r.u32()
		x.ManifestDigest = r.blob(maxDigest)
		m = x
	case MsgGroupManifest:
		x := &GroupManifest{}
		x.GroupID = r.u32()
		x.FirstFileID = r.u64()
		n := int(r.u16())
		if r.err == nil && n > maxEntryCount {
			r.fail()
		}
		if r.err == nil && n > 0 {
			x.Entries = make([]ManifestEntry, 0, n)
			for k := 0; k < n && r.err == nil; k++ {
				x.Entries = append(x.Entries, decodeManifestEntry(r))
			}
		}
		m = x
	case MsgGroupDecision:
		x := &GroupDecision{}
		x.GroupID = r.u32()
		n := int(r.u16())
		if r.err == nil && n > maxIndexList {
			r.fail()
		}
		if r.err == nil && n > 0 {
			x.Needed = make([]uint16, 0, n)
			for k := 0; k < n && r.err == nil; k++ {
				x.Needed = append(x.Needed, r.u16())
			}
		}
		x.SkippedCount = r.u16()
		rn := int(r.u16())
		if r.err == nil && rn > maxIndexList {
			r.fail()
		}
		if r.err == nil && rn > 0 {
			x.Rejected = make([]RejectedEntry, 0, rn)
			for k := 0; k < rn && r.err == nil; k++ {
				x.Rejected = append(x.Rejected, RejectedEntry{Index: r.u16(), ErrorCode: r.u16()})
			}
		}
		m = x
	case MsgCredit:
		m = &Credit{AdditionalGroups: r.u16()}
	case MsgFileRequest:
		x := &FileRequest{}
		x.RequestID = r.u64()
		x.FileID = r.u64()
		x.Offset = r.u64()
		x.MaxChunk = r.u32()
		m = x
	case MsgFileHeader:
		x := &FileHeader{}
		x.RequestID = r.u64()
		x.FileID = r.u64()
		x.Size = r.u64()
		x.Mode = r.u32()
		x.MtimeSec = r.i64()
		x.MtimeNsec = r.u32()
		dl := int(r.u8())
		x.Digest = r.raw(dl, maxDigest)
		x.ChunkSize = r.u32()
		m = x
	case MsgFileChunk:
		x := &FileChunk{}
		x.RequestID = r.u64()
		x.Offset = r.u64()
		dl := int(r.u32())
		x.Data = r.raw(dl, maxChunkData)
		m = x
	case MsgFileComplete:
		x := &FileComplete{}
		x.RequestID = r.u64()
		x.BytesSent = r.u64()
		dl := int(r.u8())
		x.DigestFull = r.raw(dl, maxDigest)
		m = x
	case MsgFileError:
		x := &FileError{}
		x.RequestID = r.u64()
		x.Code = r.u16()
		x.Retryable = r.u8()
		x.Message = r.str(maxStr)
		m = x
	case MsgFileCancel:
		x := &FileCancel{}
		x.RequestID = r.u64()
		x.Reason = r.u16()
		m = x
	case MsgSessionSummary:
		x := &SessionSummary{}
		x.FilesTotal, x.FilesTransferred, x.FilesSkipped, x.FilesFailed,
			x.BytesTransferred, x.GroupsTotal, x.CompletionDigest = decodeSummary(r)
		m = x
	case MsgSessionSummaryAck:
		x := &SessionSummaryAck{}
		x.FilesTotal, x.FilesTransferred, x.FilesSkipped, x.FilesFailed,
			x.BytesTransferred, x.GroupsTotal, x.CompletionDigest = decodeSummary(r)
		m = x
	case MsgError:
		x := &Error{}
		x.Code = r.u16()
		x.Fatal = r.u8()
		x.Message = r.str(maxStr)
		if r.remaining() > 0 { // opt trailing field (REQ-PROTO-007)
			x.Detail = r.str(maxStr)
		}
		m = x
	case MsgPing:
		m = &Ping{Token: r.u64()}
	case MsgPong:
		m = &Pong{Token: r.u64()}
	case MsgSessionResume:
		x := &SessionResume{}
		x.SessionID = r.u64()
		x.Nonce = r.blob(maxBlob)
		x.Tag = r.blob(maxBlob)
		x.LastSeqSeen = r.u64()
		m = x
	default:
		return nil, fault.Newf(fault.E5011, "decode frame", t.String(), nil, "unknown message type 0x%02x", byte(t))
	}

	if r.err != nil {
		return nil, fault.Newf(fault.E5003, "decode frame", t.String(), nil, "malformed %s body", t)
	}
	return m, nil
}

func decodeManifestEntry(r *reader) ManifestEntry {
	var e ManifestEntry
	e.EntryType = r.u8()
	e.Flags = r.u16()
	e.Path = r.blob(maxPathBytes)
	e.Size = r.u64()
	e.Mode = r.u32()
	e.MtimeSec = r.i64()
	e.MtimeNsec = r.u32()
	dl := int(r.u8())
	e.Digest = r.raw(dl, maxDigest)
	if e.EntryType == 2 {
		e.LinkTarget = r.blob(maxPathBytes)
	}
	if e.Flags&ManifestFlagHasHardlinkKey != 0 {
		e.HardlinkKey = r.u64()
	}
	return e
}

func decodeSummary(r *reader) (ft, ftr, fs, ff, bt uint64, gt uint32, digest []byte) {
	ft = r.u64()
	ftr = r.u64()
	fs = r.u64()
	ff = r.u64()
	bt = r.u64()
	gt = r.u32()
	digest = r.blob(maxDigest)
	return
}
