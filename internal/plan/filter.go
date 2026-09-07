package plan

import (
	"path"
	"path/filepath"
	"strings"

	"esync/internal/fault"
)

// FilterRule is one --include (Include true) or --exclude (Include false) glob.
type FilterRule struct {
	Include bool
	Pattern string
}

// Options controls Build (ARCHITECTURE §10.1).
type Options struct {
	// KeepSystemFiles disables the whole sys-v1 exclusion set (--keep-system-files).
	KeepSystemFiles bool
	// Filters are the ordered --include/--exclude rules; last match wins.
	Filters []FilterRule
	// GroupBytes is the target group size in bytes (--group-bytes); <= 0 uses the
	// default. It does not enter the manifest digest, so retuning it does not
	// invalidate a resume journal.
	GroupBytes int64
}

// FilterSignature is the canonical string that enters the manifest digest
// (§10.5). It changes whenever the sys set is toggled or the filter list
// changes, so two plans built with different rules have different identities.
func (o Options) FilterSignature() string {
	var b strings.Builder
	b.WriteString("sysexclude=")
	if o.KeepSystemFiles {
		b.WriteString("off")
	} else {
		b.WriteString(sysExcludeTag)
	}
	b.WriteString(";filters=")
	for i, r := range o.Filters {
		if i > 0 {
			b.WriteByte(',')
		}
		if r.Include {
			b.WriteByte('+')
		} else {
			b.WriteByte('-')
		}
		b.WriteString(r.Pattern)
	}
	return b.String()
}

func validateFilters(rules []FilterRule) error {
	for _, r := range rules {
		p := strings.TrimPrefix(strings.TrimSuffix(r.Pattern, "/"), "/")
		if _, err := filepath.Match(p, ""); err == filepath.ErrBadPattern {
			return fault.New(fault.E1007, "compile filter pattern", r.Pattern, err)
		}
	}
	return nil
}

// filterAdmits applies the ordered filter rules to a source-relative '/'-path,
// last match wins. With no matching rule the entry is admitted.
//
// Subset (documented follow-up): a rule matches the path itself or its basename,
// honours a leading "/" anchor and a trailing "/" directory marker; "**" and
// implicit "exclude a directory ⇒ exclude its contents" are not modelled here —
// the walker prunes an excluded directory so its children never reach Build.
func (o Options) filterAdmits(slashPath string, isDir bool) (admit bool, rule string) {
	admit = true
	for _, r := range o.Filters {
		if filterRuleMatches(r.Pattern, slashPath, isDir) {
			admit = r.Include
			rule = r.Pattern
		}
	}
	return admit, rule
}

func filterRuleMatches(pattern, p string, isDir bool) bool {
	if strings.HasSuffix(pattern, "/") {
		if !isDir {
			return false
		}
		pattern = strings.TrimSuffix(pattern, "/")
	}
	if anchored := strings.HasPrefix(pattern, "/"); anchored {
		ok, _ := filepath.Match(strings.TrimPrefix(pattern, "/"), p)
		return ok
	}
	if strings.Contains(pattern, "/") {
		ok, _ := filepath.Match(pattern, p)
		return ok
	}
	ok, _ := filepath.Match(pattern, path.Base(p))
	return ok
}
