#include <iostream>
#include <string>

#include "csql.h"

int main() {
  csql::Database db;

  db.execute(
      "create table users ({key, autoincrement} id :int32, {unique} login: string[32] = "
      "\"hello abc\", password_hash: bytes[8] = 0x0011223344556677, is_admin: bool = false)");

  db.execute(
      "insert (login=\"admin\", password_hash=0x0011223344556677, is_admin=true) to "
      "users");
  for (int i = 0; i < 10; ++i) {
    db.execute("insert (login=\"user" + std::to_string(i) +
               "\", password_hash=0x0011223344556677, "
               "is_admin=false) to users");
  }
  db.execute("DELETE FROM users WHERE login = \"user2\"");
  auto iterator = db.execute("SELECT login AS username FROM users WHERE |login| % 2 = 1");
  // while (iterator->hasValue()) {
  //   auto row = *(*iterator);
  //   ++(*iterator);
  //   std::cout << "Login: " << row->get<std::string>("username") << std::endl;
  // }

  db.execute(
      "CREATE TABLE test AS (SELECT login as username, password_hash FROM users WHERE is_admin = "
      "false)");

  db.exportTableToCSV("users", "users.csv");
  db.exportTableToCSV("test", "test.csv");

  return 0;
}