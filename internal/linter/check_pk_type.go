package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	rule := Rule{
		CheckerFunc:     TableBinaryChecker(pkTypeChecker),
		Name:            "pk-type",
		Description:     "Only allow primary keys to have types listed in --allow-pk-type",
		DefaultSeverity: SeverityIgnore,
	}
	rule.RelatedListOption(
		"allow-pk-type",
		"",
		"List of allowed data types for --lint-pk-type",
		true, // prohibit empty list
	)
	RegisterRule(rule)
}

func pkTypeChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) *Note {
	if table.PrimaryKey == nil {
		// This checker is expected to be used in combination with check-pk
		// so if there is no PK, we don't need to complain twice.
		return nil
	}
	allowedTypes := opts.AllowList("pk-type")
	allowedStr := strings.Join(allowedTypes, ", ")
	cols := table.ColumnsByName()
	for _, part := range table.PrimaryKey.Parts {
		if col, ok := cols[part.ColumnName]; ok {
			if !opts.IsAllowed("pk-type", col.Type.Base) {
				message := fmt.Sprintf(
					"Column %s of %s is using data type %s, which is not configured to be permitted in a primary key. The following data types are listed in option allow-pk-type: %s.",
					col.Name, table.ObjectKey(), col.Type.Base, allowedStr,
				)
				return &Note{
					LineOffset: FindColumnLineOffset(col, createStatement),
					Summary:    "Column data type not permitted for PRIMARY KEY",
					Message:    message,
				}
			}
		}
	}
	return nil
}
