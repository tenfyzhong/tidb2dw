package filter

import (
	"fmt"
	"path"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
)

var systemSchemas = map[string]struct{}{
	"information_schema": {},
	"mysql":              {},
	"performance_schema": {},
	"metrics_schema":     {},
	"inspection_schema":  {},
}

func ResolveTables(tableFilter model.TableFilter, candidates []model.TableName) ([]model.TableName, error) {
	include, exclude, err := normalizeRules(tableFilter)
	if err != nil {
		return nil, err
	}

	resolved := make([]model.TableName, 0, len(candidates))
	for _, candidate := range candidates {
		tableName := candidate.String()
		if !tableFilter.CaseSensitive {
			tableName = strings.ToLower(tableName)
		}
		matched, err := matchAny(include, tableName)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		excluded, err := matchAny(exclude, tableName)
		if err != nil {
			return nil, err
		}
		if excluded {
			continue
		}
		if IsSystemSchema(candidate.Database) {
			return nil, fmt.Errorf("table filter matched system schema %q", candidate.Database)
		}
		resolved = append(resolved, candidate)
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("table filter resolved an empty table set")
	}
	return resolved, nil
}

func ProjectColumns(
	columns []model.Column,
	columnFilter model.ColumnFilter,
	requiredKeys []string,
	caseSensitive bool,
) ([]model.Column, error) {
	columnByName := make(map[string]model.Column, len(columns))
	required := make(map[string]struct{})
	for _, column := range columns {
		key := normalizeName(column.Name, caseSensitive)
		columnByName[key] = column
		if column.IsPK == "true" {
			required[key] = struct{}{}
		}
	}
	for _, column := range requiredKeys {
		key := normalizeName(column, caseSensitive)
		if _, ok := columnByName[key]; !ok {
			return nil, fmt.Errorf("required key column %q does not exist", column)
		}
		required[key] = struct{}{}
	}

	includeSet := make(map[string]struct{})
	excludeSet := make(map[string]struct{})
	for _, column := range columnFilter.Columns {
		key := normalizeName(column, caseSensitive)
		if _, ok := columnByName[key]; !ok {
			return nil, fmt.Errorf("unknown column %q in column filter", column)
		}
		includeSet[key] = struct{}{}
	}
	for _, column := range columnFilter.Exclude {
		key := normalizeName(column, caseSensitive)
		if _, ok := columnByName[key]; !ok {
			return nil, fmt.Errorf("unknown column %q in column filter", column)
		}
		excludeSet[key] = struct{}{}
	}

	projected := make([]model.Column, 0, len(columns))
	for _, column := range columns {
		key := normalizeName(column.Name, caseSensitive)
		selected := true
		switch columnFilter.Mode {
		case "":
			selected = true
		case model.ColumnFilterModeInclude:
			_, selected = includeSet[key]
			if _, ok := required[key]; ok {
				selected = true
			}
		case model.ColumnFilterModeExclude:
			_, selected = excludeSet[key]
			selected = !selected
			if _, ok := required[key]; ok {
				selected = true
			}
		default:
			return nil, fmt.Errorf("unsupported column filter mode %q", columnFilter.Mode)
		}
		if selected {
			projected = append(projected, column)
		}
	}
	if len(projected) == 0 {
		return nil, fmt.Errorf("column filter projected an empty column set")
	}
	return projected, nil
}

func IsSystemSchema(database string) bool {
	_, ok := systemSchemas[strings.ToLower(database)]
	return ok
}

func normalizeRules(tableFilter model.TableFilter) ([]string, []string, error) {
	include := make([]string, 0, len(tableFilter.Include))
	exclude := make([]string, 0, len(tableFilter.Exclude)+len(tableFilter.Include))
	for _, rule := range tableFilter.Include {
		normalized, err := normalizeRule(rule, tableFilter.CaseSensitive)
		if err != nil {
			return nil, nil, err
		}
		if strings.HasPrefix(normalized, "!") {
			exclude = append(exclude, strings.TrimPrefix(normalized, "!"))
			continue
		}
		include = append(include, normalized)
	}
	for _, rule := range tableFilter.Exclude {
		normalized, err := normalizeRule(rule, tableFilter.CaseSensitive)
		if err != nil {
			return nil, nil, err
		}
		exclude = append(exclude, strings.TrimPrefix(normalized, "!"))
	}
	if len(include) == 0 {
		return nil, nil, fmt.Errorf("table filter include rules must not be empty")
	}
	return include, exclude, nil
}

func normalizeRule(rule string, caseSensitive bool) (string, error) {
	rule = strings.TrimSpace(rule)
	if rule == "" {
		return "", fmt.Errorf("table filter rule must not be empty")
	}
	pattern := strings.TrimPrefix(rule, "!")
	if _, err := path.Match(pattern, "test.table"); err != nil {
		return "", fmt.Errorf("invalid table filter rule %q: %w", rule, err)
	}
	if !caseSensitive {
		return strings.ToLower(rule), nil
	}
	return rule, nil
}

func matchAny(patterns []string, tableName string) (bool, error) {
	for _, pattern := range patterns {
		matched, err := path.Match(pattern, tableName)
		if err != nil {
			return false, fmt.Errorf("invalid table filter rule %q: %w", pattern, err)
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func normalizeName(name string, caseSensitive bool) string {
	if caseSensitive {
		return name
	}
	return strings.ToLower(name)
}
