package commands

import (
	"fmt"
	"io"
	"strings"
)

func DescribeCommand(cmd string, out io.Writer) error {
	desc, err := Database.Describe(cmd)
	if err != nil {
		return err
	}
	desc = append([]string{fmt.Sprintf("Table: %s", cmd)}, desc...)
	_, err = fmt.Fprintln(out, strings.Join(desc, "\n"))
	return err
}

func TablesCommand(cmd string, out io.Writer) error {
	_, err := fmt.Fprintln(out, strings.Join(Database.TableNames(), "\n"))
	return err
}
