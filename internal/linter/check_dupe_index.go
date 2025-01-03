package linter

import (
	"fmt"
	"regexp"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(dupeIndexChecker),
		Name:            "dupe-index",
		Description:     "Flag redundant secondary indexes",
		DefaultSeverity: SeverityWarning,
	})
}

func dupeIndexChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) []Note {
	makeNote := func(indexName, message string) Note {
		re := regexp.MustCompile(fmt.Sprintf("(?i)(key|index)\\s+`?%s(?:`|\\s)", indexName))
		return Note{
			LineOffset: FindFirstLineOffset(re, createStatement),
			Summary:    "Redundant index detected",
			Message:    message,
		}
	}
	var invisibleWord = "INVISIBLE"
	var supportsInvisible = opts.flavor.MinMariaDB(10, 6) || opts.flavor.MinMySQL(8, 0)
	if opts.flavor.IsMariaDB() {
		invisibleWord = "IGNORED"
	}
	makeNoteDupeIndex := func(dupeIndexName, betterIndexName string, equivalent bool) Note {
		var reason string
		if equivalent {
			reason = fmt.Sprintf("Indexes %s and %s of %s are functionally identical.\nOne of them should be dropped.", dupeIndexName, betterIndexName, table.ObjectKey())
		} else if supportsInvisible {
			reason = fmt.Sprintf("Index %s of %s is redundant to larger index %s.\nIn most cases it is safe to drop index %s, but consider making it %s first.", dupeIndexName, table.ObjectKey(), betterIndexName, dupeIndexName, invisibleWord)
		} else {
			reason = fmt.Sprintf("Index %s of %s is redundant to larger index %s.\nIn most cases it is safe to drop index %s.", dupeIndexName, table.ObjectKey(), betterIndexName, dupeIndexName)
		}
		return makeNote(dupeIndexName, reason+" Redundant indexes waste disk space, and harm write performance.")
	}
	results := make([]Note, 0)
	var colsByName map[string]*tengo.Column
	for i, idx := range table.SecondaryIndexes {
		if idx.RedundantTo(table.PrimaryKey) {
			results = append(results, makeNoteDupeIndex(idx.Name, "PRIMARY", false))
			continue // max one note for each idx
		}
		for j, other := range table.SecondaryIndexes {
			if i != j && idx.RedundantTo(other) {
				equivalent := idx.Equivalent(other)
				if !equivalent || i > j { // avoid 2 annotations for an equivalent pair
					results = append(results, makeNoteDupeIndex(idx.Name, other.Name, equivalent))
				}
				break // max one note for each idx
			}
		}
		// MySQL 8.0+ query optimizer ignores SPATIAL indexes on cols lacking an SRID
		if idx.Type == "SPATIAL" && opts.flavor.MinMySQL(8) {
			if colsByName == nil { // populate lazily
				colsByName = table.ColumnsByName()
			}
			for _, part := range idx.Parts { // spatial indexes currently only ever have 1 part, but iterate for robustness
				if col := colsByName[part.ColumnName]; col != nil && !col.HasSpatialReference {
					message := fmt.Sprintf("Spatial index %s of %s includes column %s, which lacks an SRID attribute. The database server's query optimizer will not actually use this index.", idx.Name, table.ObjectKey(), col.Name)
					results = append(results, makeNote(idx.Name, message))
				}
			}
		}
	}
	return results
}
