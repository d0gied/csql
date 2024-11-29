#include <iostream>
#include <string>

#include "csql.h"

int main() {
  csql::Database db;

  db.execute(
      "create table users ({key, autoincrement} id :int32, {unique} login: string[32] = "
      "\"hello abc\", password_hash: bytes[8] = 0x0011223344556677, is_admin: bool)");

  db.execute(
      "insert (login=\"admin\", password_hash=0x0011223344556677, is_admin=true) to "
      "users");
  db.execute(
      "insert (login=\"user1\", password_hash=0x0011223344556677) to "
      "users");
  db.execute(
      "insert (login=\"user2\", password_hash=0x2233445566778899) to "
      "users");

  db.exportTableToCSV("users", "users.csv");

  return 0;
}