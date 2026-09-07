package plan

// The default system-file exclusion set, tag "sys-v1" (ARCHITECTURE §10.1.1).
// The FULL union of the macOS, Windows and Linux tables applies on every
// platform. Matching is against the final path component only. macOS and Linux
// patterns are byte-exact and case-sensitive; Windows patterns fold ASCII case
// only. "*" is a literal prefix test, never a glob. "root only" patterns match
// solely at the top level of the source tree.

type sysKind uint8

const (
	sysExact sysKind = iota
	sysPrefix
	sysSuffix
)

type sysRule struct {
	name     string // reported for DEBUG logging + the excluded counter
	pat      string
	kind     sysKind
	dir      bool // a directory match is pruned, not merely dropped
	rootOnly bool
	winFold  bool // ASCII-only case-insensitive comparison
}

var sysRules = []sysRule{
	// macOS
	{"macos:.DS_Store", ".DS_Store", sysExact, false, false, false},
	{"macos:._*", "._", sysPrefix, false, false, false},
	{"macos:.AppleDouble", ".AppleDouble", sysExact, true, false, false},
	{"macos:.AppleDB", ".AppleDB", sysExact, true, false, false},
	{"macos:.AppleDesktop", ".AppleDesktop", sysExact, true, false, false},
	{"macos:__MACOSX", "__MACOSX", sysExact, true, false, false},
	{"macos:.Spotlight-V100", ".Spotlight-V100", sysExact, true, false, false},
	{"macos:.DocumentRevisions-V100", ".DocumentRevisions-V100", sysExact, true, false, false},
	{"macos:.fseventsd", ".fseventsd", sysExact, true, false, false},
	{"macos:.TemporaryItems", ".TemporaryItems", sysExact, true, false, false},
	{"macos:.Trashes", ".Trashes", sysExact, true, false, false},
	{"macos:.Trash", ".Trash", sysExact, true, false, false},
	{"macos:.apdisk", ".apdisk", sysExact, false, false, false},
	{"macos:.VolumeIcon.icns", ".VolumeIcon.icns", sysExact, false, true, false},
	{"macos:.com.apple.timemachine.donotpresent", ".com.apple.timemachine.donotpresent", sysExact, false, false, false},
	{"macos:.PKInstallSandboxManager", ".PKInstallSandboxManager", sysExact, true, false, false},
	{"macos:.PKInstallSandboxManager-SystemSoftware", ".PKInstallSandboxManager-SystemSoftware", sysExact, true, false, false},
	{"macos:.localized", ".localized", sysExact, false, false, false},

	// Windows (ASCII case-insensitive)
	{"windows:Thumbs.db", "Thumbs.db", sysExact, false, false, true},
	{"windows:ehthumbs.db", "ehthumbs.db", sysExact, false, false, true},
	{"windows:ehthumbs_vista.db", "ehthumbs_vista.db", sysExact, false, false, true},
	{"windows:desktop.ini", "desktop.ini", sysExact, false, false, true},
	{"windows:$RECYCLE.BIN", "$RECYCLE.BIN", sysExact, true, true, true},
	{"windows:RECYCLER", "RECYCLER", sysExact, true, true, true},
	{"windows:RECYCLED", "RECYCLED", sysExact, true, true, true},
	{"windows:System Volume Information", "System Volume Information", sysExact, true, true, true},
	{"windows:hiberfil.sys", "hiberfil.sys", sysExact, false, true, true},
	{"windows:pagefile.sys", "pagefile.sys", sysExact, false, true, true},
	{"windows:swapfile.sys", "swapfile.sys", sysExact, false, true, true},
	{"windows:$AttrDef", "$AttrDef", sysExact, false, true, true},
	{"windows:$BadClus", "$BadClus", sysExact, false, true, true},
	{"windows:$Bitmap", "$Bitmap", sysExact, false, true, true},
	{"windows:$Boot", "$Boot", sysExact, false, true, true},
	{"windows:$LogFile", "$LogFile", sysExact, false, true, true},
	{"windows:$MFT", "$MFT", sysExact, false, true, true},
	{"windows:$MFTMirr", "$MFTMirr", sysExact, false, true, true},
	{"windows:$Secure", "$Secure", sysExact, false, true, true},
	{"windows:$UpCase", "$UpCase", sysExact, false, true, true},
	{"windows:$Volume", "$Volume", sysExact, false, true, true},
	{"windows:$Extend", "$Extend", sysExact, true, true, true},

	// Linux / Unix
	{"linux:.Trash-*", ".Trash-", sysPrefix, true, false, false},
	{"linux:lost+found", "lost+found", sysExact, true, true, false},
	{"linux:.directory", ".directory", sysExact, false, false, false},
	{"linux:.nfs*", ".nfs", sysPrefix, false, false, false},
	{"linux:.fuse_hidden*", ".fuse_hidden", sysPrefix, false, false, false},
	{"linux:.gvfs", ".gvfs", sysExact, true, false, false},
}

// MatchSystemFile tests one final path component against the sys-v1 set. isDir
// and atRoot describe the entry. When matched is true, rule is the matched rule's
// name and prune is true only for a directory entry matched by a directory rule
// (the walker must not descend into it).
func MatchSystemFile(finalComp []byte, isDir, atRoot bool) (rule string, prune, matched bool) {
	for _, r := range sysRules {
		if r.rootOnly && !atRoot {
			continue
		}
		if sysNameMatches(r, finalComp) {
			return r.name, r.dir && isDir, true
		}
	}
	return "", false, false
}

func sysNameMatches(r sysRule, name []byte) bool {
	switch r.kind {
	case sysExact:
		return sysEqual(name, r.pat, r.winFold)
	case sysPrefix:
		return len(name) >= len(r.pat) && sysEqual(name[:len(r.pat)], r.pat, r.winFold)
	case sysSuffix:
		return len(name) >= len(r.pat) && sysEqual(name[len(name)-len(r.pat):], r.pat, r.winFold)
	}
	return false
}

func sysEqual(a []byte, b string, fold bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if fold {
			x, y = asciiLower(x), asciiLower(y)
		}
		if x != y {
			return false
		}
	}
	return true
}

func asciiLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}
