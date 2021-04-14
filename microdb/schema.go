package microdb

import "errors"

var schemaStore = make(map[string]*Schema)

type Schema struct {
	doType           DataOriginType
	originTableQuery string
	localTableQuery  string
	insertQuery      string
}

func LocalTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.localTableQuery, nil
}

func OriginTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.originTableQuery, nil
}

func InsertQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.insertQuery, nil
}
