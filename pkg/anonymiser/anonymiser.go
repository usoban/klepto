package anonymiser

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/usoban/klepto/pkg/config"
	"github.com/usoban/klepto/pkg/database"
	"github.com/usoban/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	expr "github.com/antonmedv/expr"
	vm "github.com/antonmedv/expr/vm"
	option "github.com/usoban/klepto/pkg/util"
)

const (
	// literalPrefix defines the constant we use to prefix literals
	literalPrefix = "literal:"
	conditionalPrefix = "cond:"
	email         = "EmailAddress"
	username      = "UserName"
	password      = "Password"
)

type (
	anonymiser struct {
		reader.Reader
		tables config.Tables
		compiledRules map[string]*vm.Program
	}
)

// NewAnonymiser returns a new anonymiser reader.
func NewAnonymiser(source reader.Reader, tables config.Tables) reader.Reader {
	return &anonymiser{source, tables, map[string]*vm.Program{}}
}

// ReadTable decorates reader.ReadTable method for anonymising rows published from the reader.Reader
func (a *anonymiser) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt, matchers config.Matchers) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading anonymiser config")
	table, err := a.tables.FindByName(tableName)
	if err != nil {
		logger.WithError(err).Debug("the table is not configured to be anonymised")
		return a.Reader.ReadTable(tableName, rowChan, opts, matchers)
	}

	if len(table.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.Reader.ReadTable(tableName, rowChan, opts, matchers)
	}

	// Compile conditional anonymisation rules
	for column, fakerType := range table.Anonymise {
		if strings.HasPrefix(fakerType, conditionalPrefix) {
			program, err := expr.Compile(strings.TrimPrefix(fakerType, conditionalPrefix))

			if err != nil {
				logger.WithError(err).Error("Conditional rule compilation failed")
				continue
			}

			ruleKey := RuleKey(tableName, column)
			a.compiledRules[ruleKey] = program
		}
	}

	// Create read/write chanel
	rawChan := make(chan database.Row)

	go func(rowChan chan<- database.Row, rawChan chan database.Row, table *config.Table) {
		for {
			row, more := <-rawChan
			if !more {
				close(rowChan)
				return
			}

			for column, fakerType := range table.Anonymise {
				if strings.HasPrefix(fakerType, literalPrefix) {
					row[column] = strings.TrimPrefix(fakerType, literalPrefix)
					continue
				}

				if strings.HasPrefix(fakerType, conditionalPrefix) {

					env := map[string]interface{}{
						"row": row,
						"column": row[column],
						"Value": func(row database.Row, columnName string) string {
							str := row[columnName].([]uint8)

							return string(str)
						},
						"Anon": func(fakerType string) *option.Option {
							return option.Some(Anonymise(fakerType))
						},
						"Skip": func() *option.Option {
							return option.None()
						},
					}
					
					ruleKey := RuleKey(table.Name, column)
					output, err := expr.Run(a.compiledRules[ruleKey], env)
					if err != nil {
						logger.WithError(err).Error("Eval rule runtime error")
						continue
					}

					opt := output.(*option.Option)
					if option.IsSome(opt) {
						row[column] = option.Value(opt)
					}

					continue
				}

				row[column] = Anonymise(fakerType)
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := a.Reader.ReadTable(tableName, rawChan, opts, matchers); err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}

// Anonymise generates a fake value 
func Anonymise(fakerType string) string {
	var value string

	for name, faker := range Functions {
		if fakerType != name {
			continue
		}

		switch name {
		case email, username:
			b := make([]byte, 2)
			rand.Read(b)
			value = fmt.Sprintf(
				"%s.%s",
				faker.Call([]reflect.Value{})[0].String(),
				hex.EncodeToString(b),
			)
		default:
			value = faker.Call([]reflect.Value{})[0].String()
		}
	}

	return value
}

// RuleKey generates a key for storing VM program of specific table's column.
func RuleKey(tableName string, columnName string) string {
	return tableName + "." + columnName
}