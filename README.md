# CSQL

## Features
- [x] Create a table
  - [x] Create with column names and types
    - [x] Default values
    - [x] Constraints
      - [ ] NOT NULL
      - [x] UNIQUE
      - [ ] PRIMARY KEY
      - [x] AUTOINCREMENT
  - [x] Create with AS clause
    - [x] Subquery
    - [x] Table name
  - [ ] IF NOT EXISTS clause
- [ ] Drop a table
- [ ] Alter a table
- [x] Select data from a table
  - [x] Column specification
    - [x] "*" for all columns
    - [x] Column names
    - [x] Column aliases
    - [ ] Column expressions
  - [x] Table specification
    - [x] Table name
    - [x] Subquery
  - [x] WHERE clause
    - [x] Comparison operators
    - [x] Logical operators
    - [x] Arithmetic operators
    - [x] Parentheses
    - [x] IS NULL operator
    - [x] IS NOT NULL operator
    - [ ] IN operator
    - [ ] BETWEEN operator
    - [ ] LIKE operator
  - [ ] ORDER BY clause
  - [ ] GROUP BY clause
  - [ ] LIMIT clause
  - [ ] OFFSET clause
- [x] Insert data into a table
  - [x] Insert into a table
    - [x] Explicit column specification
    - [x] Value specification
      - [x] Literal values
      - [x] Subquery
  - [ ] Insert into a table from another table
- [ ] Update data in a table
- [x] Delete data from a table
  - [x] Delete from a table
  - [x] WHERE clause
- [ ] Create an index
- [ ] Drop an index
- [x] Export data to a file
  - [x] Export to a CSV file
  - [ ] Export to a BINARY file
- [x] Import data from a file
  - [x] Import from a CSV file
  - [ ] Import from a BINARY file
