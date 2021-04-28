// Package query provides query building and parsing functionalities used by both MicroDB client
// and external users.
package query

import (
	"errors"
	"fmt"

	"github.com/cube2222/octosql/parser/sqlparser"
)

// Parser provides all functionalities for parsing a SQL query.

func parseQuery(query string) (*QueryStmt, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query with sqlparser: %w", err)
	}

	qs, err := parseStmt(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statement: %w", err)
	}
	qs.originQuery = query

	return qs, nil
}

func parseStmt(stmt sqlparser.Statement) (*QueryStmt, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		q := &QueryStmt{
			queryType:       QueryTypeSelect,
			destinationType: DestinationTypeLocal,
		}
		if err := parseSelect(s, q); err != nil {
			return nil, fmt.Errorf("failed to parse select statement: %w", err)
		}
		return q, nil

	case *sqlparser.Insert:
		q := &QueryStmt{
			queryType:       QueryTypeInsert,
			destinationType: DestinationTypeOrigin,
		}
		if err := parseInsert(s, q); err != nil {
			return nil, fmt.Errorf("failed to parse insert statement: %w", err)
		}
		return q, nil

	case *sqlparser.Update:
		q := &QueryStmt{
			queryType:       QueryTypeUpdate,
			destinationType: DestinationTypeOrigin,
		}
		if err := parseUpdate(s, q); err != nil {
			return nil, fmt.Errorf("failed to parse udpate statement: %w", err)
		}
		return q, nil
	}

	return nil, errors.New("unsupported query statement type")
}

func parseSelect(stmt *sqlparser.Select, qs *QueryStmt) error {
	if len(stmt.From) != 1 {
		return errors.New("currently only one expression in from supported")
	}

	// Get required tables from query
	if err := parseTableExpression(stmt.From[0], qs, false); err != nil {
		return fmt.Errorf("failed to parse from query: %w", err)
	}

	return nil
}

func parseInsert(stmt *sqlparser.Insert, qs *QueryStmt) error {
	// Get required tables from select query
	if s, ok := stmt.Rows.(*sqlparser.Select); ok {
		if err := parseSelect(s, qs); err != nil {
			return fmt.Errorf("failed to parse query: %w", err)
		}
	}

	qs.destinationTable = stmt.Table.Name.String()
	qs.requiredTables = append(qs.requiredTables, qs.destinationTable)

	return nil
}

func parseUpdate(stmt *sqlparser.Update, qs *QueryStmt) error {
	for _, expr := range stmt.TableExprs {
		if err := parseTableExpression(expr, qs, false); err != nil {
			return fmt.Errorf("failed to parse table expression in query: %w", err)
		}
	}

	return nil
}

func parseTableExpression(expr sqlparser.TableExpr, qs *QueryStmt, mustBeAliased bool) error {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return parseAliasedTableExpression(expr, qs, mustBeAliased)
	case *sqlparser.JoinTableExpr:
		return parseJoinTableExpression(expr, qs)
	case *sqlparser.ParenTableExpr:
		return parseTableExpression(expr.Exprs[0], qs, mustBeAliased)
	case *sqlparser.TableValuedFunction:
		return parseTableValuedFunction(expr, qs)
	}
	return errors.New("failed to parse table expression")
}

func parseAliasedTableExpression(expr *sqlparser.AliasedTableExpr, qs *QueryStmt, mustBeAliased bool) error {
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		if expr.As.IsEmpty() && mustBeAliased {
			return errors.New("table must have unique alias")
		}
		qs.requiredTables = append(qs.requiredTables, subExpr.Name.String())
		return nil

	case *sqlparser.Subquery:
		if err := parseSelect(subExpr.Select.(*sqlparser.Select), qs); err != nil {
			return fmt.Errorf("couldn't parse subquery: %w", err)
		}
		return nil
	}
	return errors.New("invalid aliased table expression")
}

func parseJoinTableExpression(expr *sqlparser.JoinTableExpr, qs *QueryStmt) error {
	if err := parseTableExpression(expr.LeftExpr, qs, false); err != nil {
		return fmt.Errorf("couldn't parse join left table expression: %w", err)
	}

	if err := parseTableExpression(expr.RightExpr, qs, false); err != nil {
		return fmt.Errorf("couldn't parse join right table expression: %w", err)
	}

	return nil
}

func parseTableValuedFunction(expr *sqlparser.TableValuedFunction, qs *QueryStmt) error {
	for i := range expr.Args {
		if err := parseTableValuedFunctionArgument(expr.Args[i].Value, qs); err != nil {
			return fmt.Errorf("couldn't parse table valued function argument: %w", err)
		}
	}

	return nil
}

func parseTableValuedFunctionArgument(expr sqlparser.TableValuedFunctionArgumentValue, qs *QueryStmt) error {
	switch expr := expr.(type) {
	case *sqlparser.ExprTableValuedFunctionArgumentValue:
		if err := parseExpression(expr.Expr, qs); err != nil {
			return fmt.Errorf("couldn't parse table valued function argument expression: %w", err)
		}
		return nil

	case *sqlparser.TableDescriptorTableValuedFunctionArgumentValue:
		if err := parseTableExpression(expr.Table, qs, false); err != nil {
			return fmt.Errorf("couldn't parse table valued function argument table expression: %w", err)
		}
		return nil

	case *sqlparser.FieldDescriptorTableValuedFunctionArgumentValue:
		return nil
	}

	return errors.New("invalid table valued function argument")
}

//nolint // Allow parse method to exceed suggested method size.
func parseExpression(expr sqlparser.Expr, qs *QueryStmt) error {
	switch expr := expr.(type) {
	case *sqlparser.UnaryExpr:
		if err := parseExpression(expr.Expr, qs); err != nil {
			return fmt.Errorf("couldn't parse left child expression: %w", err)
		}
		return nil

	case *sqlparser.BinaryExpr:
		if err := parseExpression(expr.Left, qs); err != nil {
			return fmt.Errorf("couldn't parse left child expression: %w", err)
		}

		if err := parseExpression(expr.Right, qs); err != nil {
			return fmt.Errorf("couldn't parse right child expression: %w", err)
		}
		return nil

	case *sqlparser.FuncExpr:
		for i := range expr.Exprs {
			arg := expr.Exprs[i]

			switch arg := arg.(type) {
			case *sqlparser.AliasedExpr:
				if err := parseExpression(arg.Expr, qs); err != nil {
					return fmt.Errorf("couldn't parse an aliased expression argument: %w", err)
				}
			default:
				return nil
			}
		}

	case *sqlparser.ColName:
		return nil

	case *sqlparser.Subquery:
		selectExpr, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return fmt.Errorf("expected select statement in subquery")
		}
		if err := parseSelect(selectExpr, qs); err != nil {
			return fmt.Errorf("couldn't parse select expression: %w", err)
		}
		return nil

	case *sqlparser.SQLVal:
		return nil

	case *sqlparser.NullVal:
		return nil

	case sqlparser.BoolVal:
		return nil

	case sqlparser.ValTuple:
		if len(expr) == 1 {
			return parseExpression(expr[0], qs)
		}

		for i := range expr {
			if err := parseExpression(expr[i], qs); err != nil {
				return fmt.Errorf("couldn't parse tuple subexpression: %w", err)
			}
		}
		return nil

	case *sqlparser.IntervalExpr:
		if err := parseExpression(expr.Expr, qs); err != nil {
			return fmt.Errorf("couldn't parse expression in interval: %w", err)
		}
		return nil

	case *sqlparser.AndExpr:
		return nil

	case *sqlparser.OrExpr:
		return nil

	case *sqlparser.NotExpr:
		return nil

	case *sqlparser.ComparisonExpr:
		return nil

	case *sqlparser.ParenExpr:
		if err := parseExpression(expr.Expr, qs); err != nil {
			return fmt.Errorf("couldn't parse parenthesized expression: %w", err)
		}
		return nil
	}

	return errors.New("unsupported expression")
}
